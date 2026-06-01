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
| Database | PostgreSQL 16 (RDS on AWS) |
| Cache / Store | Redis 7 (ElastiCache on AWS) |
| Messaging | Apache Kafka via AWS MSK Serverless (Kafka 3.6) |
| Deployment | AWS ECS Fargate (`ap-south-2` — Hyderabad) |
| Auth | AWS IAM (OIDC for CI/CD, IAM roles for MSK) |
| Resilience | Resilience4j Retry |

---

## Architecture

```
External Provider 1 (Lambda) ──┐
                                ├─→ RouteAggregatorService ─→ GET /routes  (reads Redis)
External Provider 2 (Lambda) ──┘         │
                                          └─→ POST /routes/publish ─→ Kafka topic "routes"
                                                    ↑
                                          reads providers in parallel
                                          deduplicates by (airline, source, dest, stops)
                                          publishes with stable hash key
```

### Package Layout

```
org.fwp.route.aggregator/
├── config/         — CacheConfig, KafkaConfig, ProviderProperties, RestClientConfig
├── kafka/          — RouteEventProducer, RouteKafkaTemplateConfig
├── model/          — Route (record)
├── provider/       — RouteProvider interface, HttpRouteProvider
├── service/        — RouteAggregatorService
└── web/            — RoutesController
```

### Key Design Decisions

**Dual data paths**
- `GET /routes` reads all routes from Redis by scanning keys matching `routes:*`. Routes are written by a separate publisher service with keys in the format `routes:<datetime>`.
- `POST /routes/publish` fetches from both external providers in parallel (virtual threads), deduplicates, and publishes each route to Kafka.

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
| `GET` | `/actuator/health` | ALB health check target |
| `GET` | `/actuator/metrics` | Micrometer metrics |

### `POST /routes/publish` Response

```json
{
  "published": 42,
  "topic": "routes"
}
```

Returns `202 Accepted`. Kafka sends are async — the response returns before MSK acknowledges.

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
- Docker (for Postgres + Redis via Docker Compose)
- Local Kafka on port `9092` (e.g. via `docker run -p 9092:9092 apache/kafka`)

### Start

```bash
./gradlew bootRun
```

The `local` Spring profile activates automatically via `bootRun`. It configures:
- Kafka at `localhost:9092` with `PLAINTEXT` (no SASL)
- Redis at `localhost:6379`
- Postgres at `localhost:5432`
- `kafka.enabled=true`

`spring-boot-docker-compose` auto-starts `compose.yaml` (Postgres + Redis) when Docker is running. No manual `docker compose up` needed.

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

# Build OCI image for ECR
./gradlew bootBuildImage

# Fetch all routes from Redis
curl http://localhost:8080/routes

# Trigger provider fetch → Kafka publish
curl -X POST http://localhost:8080/routes/publish

# Flush Redis manually
docker exec $(docker ps --filter "ancestor=redis:7" -q) redis-cli FLUSHALL
```

---

## Configuration Reference

All tuneable values are externalised. ECS task definition injects env vars for production.

| Property | Env Var | Local Default | Description |
|---|---|---|---|
| `spring.kafka.bootstrap-servers` | `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka broker(s) |
| `kafka.enabled` | `KAFKA_ENABLED` | `true` (local) | Enables Kafka producer + streams |
| `routes.redis-key` | `ROUTES_REDIS_KEY` | `routes` | Redis key prefix scanned for routes |
| `routes.provider.one-base-url` | `PROVIDER1_BASE_URL` | Lambda URL `/flights1` | External provider 1 |
| `routes.provider.two-base-url` | `PROVIDER2_BASE_URL` | Lambda URL `/flights2` | External provider 2 |
| `routes.provider.connect-timeout` | — | `PT3S` | HTTP connect timeout |
| `routes.provider.read-timeout` | — | `PT15S` | HTTP read timeout |
| `routes.provider.retry.max-attempts` | — | `3` | Retry attempts per provider |
| `routes.provider.retry.wait-duration` | — | `PT0.5S` | Wait between retries |
| `spring.data.redis.host` | `REDIS_HOST` | `localhost` | Redis host |
| `spring.datasource.url` | `DB_HOST` / `DB_PORT` / `DB_NAME` | `localhost:5432/fwf_demo_db` | Postgres JDBC URL |
| `spring.datasource.password` | `DB_PASSWORD` | `changeme` | DB password (Secrets Manager in prod) |

---

## Spring Profiles

| Profile | Activated by | Use case |
|---|---|---|
| `local` | `./gradlew bootRun` (automatic) | Local dev — PLAINTEXT Kafka, localhost services |
| `prod` | `SPRING_PROFILES_ACTIVE=prod` (ECS task def) | AWS — MSK SASL/IAM, RDS, ElastiCache |

---

## Kafka Integration

### MSK Serverless Auth (prod)

MSK Serverless only accepts IAM authentication. No credentials are stored — the ECS task role grants the required Kafka permissions.

```
security.protocol = SASL_SSL
sasl.mechanism    = AWS_MSK_IAM
sasl.jaas.config  = software.amazon.msk.auth.iam.IAMLoginModule required;
```

### Topics

| Topic | Description |
|---|---|
| `routes` | Deduplicated route events published by this service |
| `routes-ag` | Reserved for Kafka Streams aggregation (future) |

### Kafka Message Format

```
Key:   routes:<8-char-hex>   (e.g. routes:3a2f8b1c)
Value: {"airline":"AA","sourceAirport":"JFK","destinationAirport":"LAX","stops":0,"equipment":"738"}
```

### Connecting to MSK Locally (SSM Tunnel)

MSK Serverless is in a private subnet. Use SSM port-forwarding through an ECS task to reach it from your machine:

```bash
# 1. Get bootstrap endpoint
aws kafka get-bootstrap-brokers --cluster-arn <arn> --region ap-south-2 \
  --query "BootstrapBrokerStringSaslIam" --output text

# 2. Open tunnel (requires session-manager-plugin)
aws ssm start-session --region ap-south-2 \
  --target "ecs:fwf-demo_<task-id>_<runtime-id>" \
  --document-name AWS-StartPortForwardingSessionToRemoteHost \
  --parameters '{"host":["<bootstrap-endpoint>"],"portNumber":["9098"],"localPortNumber":["9098"]}'

# 3. Connect IDE/consumer to localhost:9098 with SASL_SSL + AWS_MSK_IAM
```

---

## CI/CD

GitHub Actions workflow (`.github/workflows/deploy.yml`) triggers on push to `master`:

1. Build OCI image via `./gradlew bootBuildImage` (pinned to `linux/amd64`)
2. Push versioned + `latest` tags to ECR (`fun-with-flights/airlines-aggregator`)
3. Fetch current ECS task definition (falls back to `.github/task-definition-base.json` on first deploy)
4. Render updated task definition with new image
5. Deploy rolling update to ECS cluster `fwf-demo-cluster`, service `airlines-aggregator-svc`

### Required GitHub Secrets

| Secret | Description |
|---|---|
| `AWS_DEPLOY_ROLE_ARN` | IAM role for OIDC-based deploy (no stored credentials) |
| `ECS_EXECUTION_ROLE_ARN` | ECS execution role ARN |
| `ECS_TASK_ROLE_ARN` | ECS task role ARN |
| `REDIS_HOST` | ElastiCache endpoint |
| `DB_HOST` | RDS endpoint |
| `MSK_BOOTSTRAP` | MSK bootstrap server URL |
| `DB_PASSWORD_SECRET_ARN` | Secrets Manager ARN for DB password |

---

## Cloud Deployment (AWS `ap-south-2`)

```
CloudFront → WAF → ALB (public subnets)
                      ↓
              ECS Fargate (2 tasks, private subnets)
              cpu: 256  memory: 512MB
              JVM flags: -Xmx96m -XX:+UseSerialGC
                      ↓
         ┌────────────┼────────────┐
      RDS Postgres  ElastiCache  MSK Serverless
      (db.t3.micro)  Redis        (Kafka 3.6)
```

Health check: `GET /actuator/health` — used by ALB target group.

ECR push script: `scripts/ecr-push.sh`

---

## Project Structure

```
fwp-route-aggregator/
├── .github/
│   ├── task-definition-base.json   — base ECS task def for first deploy
│   └── workflows/deploy.yml        — CI/CD pipeline
├── architecture/
│   ├── infrastructure.md           — Terraform reference for AWS environment
│   ├── req.md                      — Full FunWithFlights requirements
│   └── solution-architecture.md    — C4 diagrams, ADRs, tech stack decisions
├── scripts/
│   └── ecr-push.sh
├── src/
│   └── main/
│       ├── java/org/fwp/route/aggregator/
│       └── resources/
│           ├── application.yaml          — shared base config
│           ├── application-local.yaml    — local dev profile
│           └── application-prod.yaml     — ECS production profile
├── compose.yaml                    — Postgres 16 + Redis 7 for local dev
└── build.gradle
```
