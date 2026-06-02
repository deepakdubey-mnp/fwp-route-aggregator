# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

**fwp-route-aggregator** — the route aggregation microservice for FunWithFlights (FWP), a cloud-native flight booking platform. It aggregates flight route data from multiple external providers, deduplicates, publishes to Kafka, and serves cached routes from Redis via `GET /routes`.

Stack: Java 25 · Spring Boot 4.0.6 · Gradle · PostgreSQL · Redis · Kafka (MSK Serverless in prod) · ECS Fargate (AWS `ap-south-2`)

## Commands

```bash
# Run (Docker Compose for postgres + redis auto-starts via spring-boot-docker-compose)
# The local profile is activated automatically by the bootRun task
./gradlew bootRun

# Build JAR
./gradlew build

# Run all tests
./gradlew test

# Run a single test class
./gradlew test --tests "org.fwp.route.aggregator.SomeTest"

# Run a single test method
./gradlew test --tests "org.fwp.route.aggregator.SomeTest.methodName"

# Build OCI image (for ECR push, pinned to linux/amd64)
./gradlew bootBuildImage

# GraalVM native compile
./gradlew nativeCompile

# Verify compilation only (fast feedback)
./gradlew compileJava
```

Local dev: `spring-boot-docker-compose` is a `developmentOnly` dependency — it starts `compose.yaml` (postgres:16 + redis:7) automatically when you run `bootRun`. No manual `docker compose up` needed. Requires Docker running. **Note:** `compose.yaml` does not include Kafka; to test Kafka locally you need a separate Kafka instance on `localhost:9092`.

Spring profiles:
- `local` — auto-activated by `bootRun` (Gradle task sets `spring.profiles.active=local`); points to localhost Kafka/Redis/Postgres, `kafka.enabled=true`
- `prod` — activated by ECS task definition; uses MSK IAM auth (`SASL_SSL`/`AWS_MSK_IAM`), `kafka.enabled=true`

## Architecture

### Package layout

```
org.fwp.route.aggregator/
├── config/          — ProviderProperties, RestClientConfig, CacheConfig (ObjectMapper bean)
├── kafka/           — RouteEventProducer, RouteKafkaTemplateConfig
├── model/           — Route (record)
├── provider/        — RouteProvider interface + HttpRouteProvider
├── service/         — RouteAggregatorService
└── web/             — RoutesController
```

### Data flow

```
POST /routes/publish
  → RouteAggregatorService.publishToKafka()
    → fetch from all providers concurrently (virtual threads)
    → deduplicate by (airline, source, destination, stops)
    → RouteEventProducer.publishRoutes()  →  Kafka topic "routes"
                                              (key: "routes:<8-hex>")

Kafka consumer (external) writes each route to Redis as:
  key = "routes:<8-hex>"  value = JSON

GET /routes
  → RouteAggregatorService.getRoutes()
    → SCAN Redis for "routes:*" keys
    → deserialize JSON → List<Route>
```

### Key design decisions

**Provider abstraction** — `RouteProvider` is an interface; each external provider is a separate Spring bean wired in `RestClientConfig`. Adding a new provider = add a new `@Bean` returning `HttpRouteProvider`. The service auto-discovers all `RouteProvider` beans via `List<RouteProvider>` injection.

**Virtual thread parallel fetch** — `RouteAggregatorService.publishToKafka()` fires `CompletableFuture.supplyAsync` for every provider concurrently using a named virtual thread executor (`provider-fetch-N`). Provider calls are IO-bound — virtual threads park cheaply during HTTP waits.

**Deduplication** — routes are keyed on `(airline, sourceAirport, destinationAirport, stops)` via the private `RouteKey` record. First-seen wins (`LinkedHashMap.putIfAbsent`). Preserves insertion order for deterministic output.

**Kafka publish** — `RouteEventProducer` is `@ConditionalOnProperty(name = "kafka.enabled", havingValue = "true")`. It serializes each `Route` to JSON and sends it to the `routes` topic with a stable partition key derived from `String.hashCode()` of the composite identity fields (format: `routes:<8-char unsigned hex>`). Failures per-route are logged and skipped; they don't abort the batch. `RouteEventProducer` is setter-injected into `RouteAggregatorService` (`@Autowired(required = false)`) so the service starts even when Kafka is disabled.

**Redis read via SCAN** — `getRoutes()` uses `RedisCallback` + `SCAN` (batch size 200) to avoid blocking Redis with `KEYS`. It reads all values under `routes:*`, deserializes JSON to `Route`, and silently skips malformed entries.

**Resilience** — each `HttpRouteProvider` wraps its HTTP call in `Retry.decorateSupplier(retry, ...)`. On `RestClientException` Resilience4j retries up to `maxAttempts` times with `waitDuration` back-off, then falls back to an empty list so the aggregator degrades gracefully without failing the request.

**ObjectMapper** — `CacheConfig` registers a lenient `ObjectMapper` (`FAIL_ON_UNKNOWN_PROPERTIES=false`) as a `@ConditionalOnMissingBean` fallback, used for both provider HTTP response deserialization and Redis JSON serialization in the service layer.

### API endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/routes` | Routes read from Redis (SCAN `routes:*`) |
| `POST` | `/routes/publish` | Fetch from providers → deduplicate → publish to Kafka; returns `{published, topic}` |
| `GET` | `/actuator/health` | ALB health check target |
| `GET` | `/actuator/metrics` | Micrometer metrics |
| `GET` | `/actuator/info` | Application info |

### Configuration reference

All tuneable values are externalised (ECS task definition injects env vars):

| Property | Env var | Default |
|---|---|---|
| `routes.redis-key` | `ROUTES_REDIS_KEY` | `routes` |
| `routes.provider.one-base-url` | `PROVIDER1_BASE_URL` | Lambda URL /flights1 |
| `routes.provider.two-base-url` | `PROVIDER2_BASE_URL` | Lambda URL /flights2 |
| `routes.provider.connect-timeout` | — | `PT3S` |
| `routes.provider.read-timeout` | — | `PT15S` |
| `routes.provider.retry.max-attempts` | — | `3` |
| `routes.provider.retry.wait-duration` | — | `PT0.5S` |
| `kafka.enabled` | — | `false` (base); `true` in `local` and `prod` profiles |
| `spring.kafka.bootstrap-servers` | `KAFKA_BOOTSTRAP` | `localhost:9092` (local profile) |
| `spring.data.redis.host` | `REDIS_HOST` | `localhost` |
| `spring.data.redis.port` | `REDIS_PORT` | `6379` |
| `spring.datasource.url` | `DB_HOST` / `DB_PORT` / `DB_NAME` | local defaults |
| `spring.datasource.password` | `DB_PASSWORD` (Secrets Manager in prod) | — |

### Cloud deployment

The ECS task definition (in `architecture/infrastructure.md`) runs 2 Fargate tasks behind an ALB. CloudFront → WAF → ALB → ECS. Health check: `GET /actuator/health`. JVM flags: `-Xmx96m -XX:+UseSerialGC`. Image pushed to ECR `fun-with-flights/airlines-aggregator` via `scripts/ecr-push.sh`. Kafka uses MSK Serverless with IAM auth (no credentials — IAM role assumed by the ECS task).

### Architecture documents

- `architecture/req.md` — full FunWithFlights requirements
- `architecture/solution-architecture.md` — C4 diagrams, ADRs, tech stack decisions
- `architecture/infrastructure.md` — Terraform reference for the demo AWS environment (`ap-south-2`)
