# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

**fwp-route-aggregator** — the route aggregation microservice for FunWithFlights (FWP), a cloud-native flight booking platform. It aggregates flight route data from multiple external providers, deduplicates, caches in Redis, and exposes `GET /routes`.

Stack: Java 25 · Spring Boot 4.0.6 · Gradle · PostgreSQL · Redis · ECS Fargate (AWS `ap-south-2`)

## Commands

```bash
# Run (Docker Compose for postgres + redis auto-starts via spring-boot-docker-compose)
./gradlew bootRun

# Build JAR
./gradlew build

# Run all tests
./gradlew test

# Run a single test class
./gradlew test --tests "org.fwp.route.aggregator.SomeTest"

# Run a single test method
./gradlew test --tests "org.fwp.route.aggregator.SomeTest.methodName"

# Build OCI image (for ECR push)
./gradlew bootBuildImage

# GraalVM native compile
./gradlew nativeCompile

# Verify compilation only (fast feedback)
./gradlew compileJava
```

Local dev: `spring-boot-docker-compose` is a `developmentOnly` dependency — it starts `compose.yaml` (postgres:16 + redis:7) automatically when you run `bootRun`. No manual `docker compose up` needed. Requires Docker running.

Flush Redis cache manually (useful during development):
```bash
docker exec $(docker ps --filter "ancestor=redis:7" -q) redis-cli FLUSHALL
```

Force-refresh routes without restarting:
```bash
curl -X DELETE http://localhost:8080/routes/cache
```

## Architecture

### Package layout

```
org.fwp.route.aggregator/
├── config/          — ProviderProperties, RestClientConfig, CacheConfig
├── model/           — Route (record, implements Serializable)
├── provider/        — RouteProvider interface + HttpRouteProvider
├── service/         — RouteAggregatorService
└── web/             — RoutesController
```

### Key design decisions

**Provider abstraction** — `RouteProvider` is an interface; each external provider is a separate Spring bean wired in `RestClientConfig`. Adding a new provider = add a new `@Bean` returning `HttpRouteProvider`. The service auto-discovers all `RouteProvider` beans via `List<RouteProvider>` injection.

**Virtual thread parallel fetch** — `RouteAggregatorService` fires `CompletableFuture.supplyAsync` for every provider concurrently using a named virtual thread executor (`provider-fetch-N`). Provider calls are IO-bound — virtual threads park cheaply during HTTP waits rather than blocking carrier threads. `StructuredTaskScope` (JEP 505) is the cleaner API for this pattern but remains preview in Java 25.

**Deduplication** — routes are keyed on `(airline, sourceAirport, destinationAirport, stops)`. First-seen wins (`LinkedHashMap.putIfAbsent`). Preserves insertion order for deterministic output.

**Caching** — `@Cacheable(value = "routes", unless = "#result == null || #result.isEmpty()")` on `getRoutes()` backed by Redis (JDK serialization — `Route` implements `Serializable`). Empty results are never cached. TTL is configurable via `routes.cache.ttl`. Cache eviction via `DELETE /routes/cache` or `RouteAggregatorService.evictRoutes()`.

**Resilience** — each `HttpRouteProvider` wraps its HTTP call in `Retry.decorateSupplier(retry, ...)`. On `RestClientException` Resilience4j retries up to `maxAttempts` times with `waitDuration` back-off, then falls back to an empty list so the aggregator degrades gracefully without failing the request.

### Configuration reference

All tuneable values are externalised (ECS task definition injects env vars):

| Property | Env var | Default |
|---|---|---|
| `routes.provider.one-base-url` | `PROVIDER1_BASE_URL` | Lambda URL /flights1 |
| `routes.provider.two-base-url` | `PROVIDER2_BASE_URL` | Lambda URL /flights2 |
| `routes.provider.connect-timeout` | — | `PT3S` |
| `routes.provider.read-timeout` | — | `PT15S` |
| `routes.provider.retry.max-attempts` | — | `3` |
| `routes.provider.retry.wait-duration` | — | `PT0.5S` |
| `routes.cache.ttl` | `ROUTES_CACHE_TTL` | `PT5M` |
| `spring.data.redis.host` | `REDIS_HOST` | `localhost` |
| `spring.datasource.url` | `DB_HOST` / `DB_PORT` / `DB_NAME` | local defaults |
| `spring.datasource.password` | `DB_PASSWORD` (Secrets Manager in prod) | — |

### Caching behaviour — important gotcha

`@Cacheable` caches the result in Redis using JDK serialization. The cache key is `routes::SimpleKey []`. If the cache holds a stale empty result (e.g. from a startup before providers were wired), call `DELETE /routes/cache` or flush Redis. The `unless` guard prevents future empty results from being cached.

### Retry behaviour

`ProviderProperties.RetryProperties` holds retry config. `RestClientConfig.buildRetry()` constructs a `Retry` instance per provider using `Retry.of(name, config)`. The retry instance is passed to `HttpRouteProvider` at construction — no AOP, no `RetryRegistry` required. The `resilience4j-retry:2.3.0` core module is the only dependency needed.

### API endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/routes` | Aggregated + deduplicated routes (cached) |
| `DELETE` | `/routes/cache` | Evict the routes cache, force re-fetch on next GET |
| `GET` | `/actuator/health` | ALB health check target |
| `GET` | `/actuator/metrics` | Micrometer metrics |
| `GET` | `/actuator/beans` | Registered Spring beans (dev/debug) |
| `GET` | `/actuator/env` | Resolved config properties (dev/debug) |

### Cloud deployment

The ECS task definition (in `architecture/infrastructure.md`) runs 2 Fargate tasks behind an ALB. CloudFront → WAF → ALB → ECS. Health check: `GET /actuator/health`. JVM flags: `-Xmx96m -XX:+UseSerialGC`. Image pushed to ECR `fun-with-flights/airlines-aggregator` via `scripts/ecr-push.sh`.

### Architecture documents

- `architecture/req.md` — full FunWithFlights requirements
- `architecture/solution-architecture.md` — C4 diagrams, ADRs, tech stack decisions
- `architecture/infrastructure.md` — Terraform reference for the demo AWS environment (`ap-south-2`)