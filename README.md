# Midas Core

## What it does

**Midas Core** is a Spring Boot service for a simple **ledger**:

1. **Users** — Stored in **H2** via JPA (`UserRecord`: generated id, name, **balance**). The application layer saves rows through `DatabaseConduit` → `UserRepository`.

2. **Transfers** — A **transaction** is a transfer instruction: **sender user id**, **recipient user id**, and **amount** (`Transaction`, JSON-friendly). In the integration setup, line-oriented test fixtures are parsed into `Transaction` instances and published to a Kafka topic (config key `general.kafka-topic`, default name **`trader-updates`**). A **`@KafkaListener`** consumes those messages (in **`src/test`**, the handler currently logs each `Transaction`).

3. **Balance reads** — **`Balance`** is the DTO for a user’s balance amount. The test helper **`BalanceQuerier`** performs `GET http://localhost:33400/balance?userId={id}` and deserializes the response as `Balance`. That implies the running app is expected to serve that contract when those tests are used with **`WebEnvironment.DEFINED_PORT`** and matching `server.port`.

**Scope in this tree:** `src/main` holds persistence, shared DTOs, and Kafka **configuration**. The **`@KafkaListener`**, **`KafkaTemplate`** producer, and **`RestTemplate`** balance client live under **`src/test`** as the integration harness—not as shipped production controllers/listeners in `src/main`.

---

## Stack

| Layer | Technology |
|--------|------------|
| Language | Java 17 |
| Application framework | Spring Boot 3.2.5 |
| Web | Spring Web |
| Data | Spring Data JPA, H2 2.2.224 (runtime) |
| Messaging | Spring Kafka 3.1.4 |
| Testing | JUnit 5, `spring-boot-starter-test`, `spring-kafka-test`, Testcontainers Kafka 1.19.1 |
| Build | Maven (wrapper included) |

---

## Layout

| Location | Role |
|----------|------|
| `src/main/java/.../entity`, `repository`, `component` | `UserRecord`, `UserRepository`, `DatabaseConduit` |
| `src/main/java/.../foundation` | `Transaction`, `Balance` |
| `src/main/java/.../MidasCoreApplication.java` | Bootstrap |
| `src/test/java/...` | `KafkaProducer`, `KafkaTransactionListener`, `UserPopulator`, `FileLoader`, `BalanceQuerier`, integration tests (`TaskOneTests`–`TaskFiveTests`) |
| `src/test/resources/test_data/` | User and transaction line files for tests |
| `application.yml` | Kafka serializers/deserializers, consumer group `midas-group`, trusted JSON package `com.jpmc.midascore.foundation`, default `Transaction` type for deserialization |

Producer: `StringSerializer` + `JsonSerializer` for values. Consumer: `StringDeserializer` + `JsonDeserializer` with the settings above.

---

## Build and run

```bash
./mvnw clean verify
./mvnw spring-boot:run
```

Windows: `mvnw.cmd` instead of `./mvnw`.

Single test class:

```bash
./mvnw -Dtest=TaskOneTests test
```

Some Kafka-backed test classes use **`@EmbeddedKafka`** and **infinite loops** after setup; they are meant for interactive debugging, not unattended full-suite runs.
