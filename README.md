# fwp-route-aggregator

Flight route aggregation microservice for **FunWithFlights (FWP)** — a cloud-native flight booking platform.

The service aggregates flight route data from two external providers in parallel, deduplicates results, exposes them via a REST API backed by Redis, and publishes routes to an AWS MSK Kafka topic on demand.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Java 25 |
| Framework | Spring Boot 4.0.6 |
| Build | Gradle |
| Cache / Store | Redis 7 (ElastiCache on AWS) |
| Messaging | Apache Kafka via AWS MSK Serverless (Kafka 3.6) |
| Deployment | AWS ECS Fargate (`ap-south-2` — Hyderabad) |
| Auth | AWS IAM (IAM roles for MSK) |
| Resilience | Resilience4j Retry |

---

## Architecture

```
External Provider 1 (Lambda) ──┐
                                ├─→ RouteAggregatorService ─→ GET /routes  (reads Redis)
External Provider 2 (Lambda) ──┘         │
                                          └─→ POST /routes/publish ─→ Kafka topic "routes"
                                                    ↑
                                          fetches providers in parallel
                                          deduplicates by (airline, source, dest, stops)
                                          publishes with stable hash key

Kafka topic "routes"
        ↓
[Streaming App — separate service]
        ├─→ Enriches data
        ├─→ Persists to PostgreSQL
        └─→ Writes back to Redis
```

**This service is Kafka-producer-only.** It does not read from or write to PostgreSQL. A dedicated streaming app (separate repository) consumes the `routes` topic, enriches route data, and persists it to PostgreSQL and Redis.

### Package Layout

```
org.fwp.route.aggregator/
├── config/         — CacheConfig, ProviderProperties, RestClientConfig
├── kafka/          — RouteEventProducer, RouteKafkaTemplateConfig, RoutePublisher, NoOpRoutePublisher
├── model/          — Route (record)
├── provider/       — RouteProvider interface, HttpRouteProvider
├── service/        — RouteAggregatorService
└── web/            — RoutesController
```

### Key Design Decisions

**Adapter pattern for publishing**
`RoutePublisher` is the output port for publishing routes. `RouteEventProducer` is the Kafka adapter (active when `kafka.enabled=true`). `NoOpRoutePublisher` is the null-object adapter (active when `kafka.enabled=false`). The service depends only on the interface — transport is swappable without touching service logic.

**Dual publish paths**
- `POST /routes/publish` (pull): fetches from both external providers in parallel, deduplicates, publishes.
- `POST /routes/webhook` (push): accepts a `List<Route>` body, deduplicates, publishes. Useful for external systems pushing routes directly.

**Virtual thread parallel fetch**
Provider HTTP calls are dispatched via `CompletableFuture.supplyAsync` on a named virtual thread executor (`provider-fetch-N`). IO-bound waits park cheaply without blocking carrier threads.

**Deduplication**
Routes are keyed on `(airline, sourceAirport, destinationAirport, stops)`. First-seen wins using a `LinkedHashMap.putIfAbsent`. Preserves insertion order for deterministic output.

**Kafka key derivation**
Each route is published with a stable, partition-consistent key:
```
key = "routes:" + hex(hashCode("airline|source|dest|stops"))
```
`String.hashCode()` is guaranteed deterministic by the Java spec. Same route always lands in the same Kafka partition, enabling ordered consumption.

**Resilience**
Each `HttpRouteProvider` wraps its HTTP call in `Retry.decorateSupplier`. On `RestClientException`, Resilience4j retries up to `maxAttempts` times with `waitDuration` back-off, then falls back to an empty list so the aggregator degrades gracefully.

---

## API Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/routes` | All routes from Redis (pattern `routes:*`) |
| `POST` | `/routes/publish` | Fetch from providers → deduplicate → publish to Kafka |
| `POST` | `/routes/webhook` | Accept pushed routes → deduplicate → publish to Kafka |
| `GET` | `/actuator/health` | ALB health check target |
| `GET` | `/actuator/metrics` | Micrometer metrics |

### `POST /routes/publish` Response

```json
{
  "published": 42,
  "destination": "kafka:routes"
}
```

### `POST /routes/webhook` Request / Response

```bash
curl -X POST http://localhost:8080/routes/webhook \
  -H "Content-Type: application/json" \
  -d '[{"airline":"AA","sourceAirport":"JFK","destinationAirport":"LAX","stops":0,"equipment":"738"}]'
```

```json
{
  "published": 1,
  "destination": "kafka:routes"
}
```

Both endpoints return `202 Accepted`. Kafka sends are async — the response returns before MSK acknowledges.

---

## Route Model

```json
{
  "airline": "AA",
  "sourceAirport": "JFK",
  "destinationAirport": "LAX",
  "codeShare": "Y",
  "stops": 0,
  "equipment": "738"
}
```

---

## Running Locally

### Prerequisites

- Java 25
- Docker (for Redis via Docker Compose)
- Local Kafka on port `9092` (e.g. `docker run -p 9092:9092 apache/kafka`)

### Start

```bash
./gradlew bootRun
```

The `local` Spring profile activates automatically via `bootRun`. It configures:
- Kafka at `localhost:9092` with `PLAINTEXT` (no SASL)
- Redis at `localhost:6379`
- `kafka.enabled=true`

`spring-boot-docker-compose` auto-starts `compose.yaml` (Redis) when Docker is running. No manual `docker compose up` needed.

### Useful Dev Commands

```bash
# Build JAR
./gradlew build

# Run all tests
./gradlew test

# Run a single test class
./gradlew test --tests "org.fwp.route.aggregator.SomeTest"

# Compile check only (fast feedback)
./gradlew compileJava

# Fetch all routes from Redis
curl http://localhost:8080/routes

# Trigger provider fetch → Kafka publish
curl -X POST http://localhost:8080/routes/publish

# Push routes via webhook
curl -X POST http://localhost:8080/routes/webhook \
  -H "Content-Type: application/json" \
  -d '[{"airline":"AA","sourceAirport":"JFK","destinationAirport":"LAX","stops":0}]'

# Flush Redis manually
docker exec $(docker ps --filter "ancestor=redis:7" -q) redis-cli FLUSHALL
```

---

## Configuration Reference

All tuneable values are externalised. ECS task definition injects env vars for production.

| Property | Env Var | Local Default | Description |
|---|---|---|---|
| `spring.kafka.bootstrap-servers` | `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka broker(s) |
| `kafka.enabled` | — | `true` (local + prod) | Enables Kafka producer |
| `routes.redis-key` | `ROUTES_REDIS_KEY` | `routes` | Redis key prefix scanned for routes |
| `routes.provider.one-base-url` | `PROVIDER1_BASE_URL` | Lambda URL `/flights1` | External provider 1 |
| `routes.provider.two-base-url` | `PROVIDER2_BASE_URL` | Lambda URL `/flights2` | External provider 2 |
| `routes.provider.connect-timeout` | — | `PT3S` | HTTP connect timeout |
| `routes.provider.read-timeout` | — | `PT15S` | HTTP read timeout |
| `routes.provider.retry.max-attempts` | — | `3` | Retry attempts per provider |
| `routes.provider.retry.wait-duration` | — | `PT0.5S` | Wait between retries |
| `spring.data.redis.host` | `REDIS_HOST` | `localhost` | Redis host |
| `spring.data.redis.port` | `REDIS_PORT` | `6379` | Redis port |

---

## Spring Profiles

| Profile | Activated by | Use case |
|---|---|---|
| `local` | `./gradlew bootRun` (automatic) | Local dev — PLAINTEXT Kafka, localhost services |
| `prod` | `SPRING_PROFILES_ACTIVE=prod` (ECS task def) | AWS — MSK SASL/IAM, ElastiCache |

---

## Kafka Integration

### MSK Serverless Auth (prod)

MSK Serverless only accepts IAM authentication. No credentials are stored — the ECS task role grants the required Kafka permissions.

```
security.protocol = SASL_SSL
sasl.mechanism    = AWS_MSK_IAM
sasl.jaas.config  = software.amazon.msk.auth.iam.IAMLoginModule required;
```

### Required IAM Permissions (ECS task role)

| Action | Resource |
|---|---|
| `kafka-cluster:Connect`, `kafka-cluster:WriteDataIdempotently` | cluster ARN |
| `kafka-cluster:WriteData`, `kafka-cluster:*Topic*` | `topic/cluster-name/cluster-id/*` |
| `kafka-cluster:AlterGroup`, `kafka-cluster:DescribeGroup` | `group/cluster-name/cluster-id/*` |

Note: topic and group resource ARNs use `topic/` and `group/` prefixes respectively — not `cluster/`.

### Topics

| Topic | Producer | Consumer |
|---|---|---|
| `routes` | This service | Streaming App (separate) |

### Kafka Message Format

```
Key:   routes:<8-char-hex>   (e.g. routes:3a2f8b1c)
Value: {"airline":"AA","sourceAirport":"JFK","destinationAirport":"LAX","stops":0,"equipment":"738"}
```

---

## Deployment

### Local → ECS (manual)

```bash
chmod +x scripts/ecr-push.sh
./scripts/ecr-push.sh                # build → push → deploy
./scripts/ecr-push.sh --skip-deploy  # build → push only
```

The script:
1. Derives the image tag from `build.gradle` version + short git SHA
2. Builds a JAR via `./gradlew bootJar`
3. Builds a `linux/amd64` Docker image via `docker buildx build --push`
4. Pushes versioned + `latest` tags to ECR (`fun-with-flights/airlines-aggregator`)
5. Registers a new ECS task definition revision
6. Triggers a rolling update on `airlines-aggregator-svc`
7. Waits for service stability

### Cloud Infrastructure (AWS `ap-south-2`)

```
CloudFront → WAF → ALB (public subnets)
                      ↓
              ECS Fargate (2 tasks, private subnets)
              cpu: 256  memory: 512MB
              JVM flags: -Xmx96m -XX:+UseSerialGC
                      ↓
              ┌────────────────┐
          ElastiCache Redis   MSK Serverless
          (cache.t3.micro)    (Kafka 3.6)
```

Health check: `GET /actuator/health` — used by ALB target group.

---

## Project Structure

```
fwp-route-aggregator/
├── .github/
│   └── task-definition-base.json   — base ECS task def for first deploy
├── architecture/
│   ├── infrastructure.md           — Terraform reference for AWS environment
│   ├── req.md                      — Full FunWithFlights requirements
│   └── solution-architecture.md    — C4 diagrams, ADRs, tech stack decisions
├── scripts/
│   └── ecr-push.sh                 — local build → ECR push → ECS deploy
├── src/
│   └── main/
│       ├── java/org/fwp/route/aggregator/
│       └── resources/
│           ├── application.yaml          — shared base config
│           ├── application-local.yaml    — local dev profile
│           └── application-prod.yaml     — ECS production profile
├── compose.yaml                    — Redis 7 for local dev
└── build.gradle
```
