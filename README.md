# Midas Core

## What it does

**Midas Core** is a Spring Boot **ledger** demo backed by **H2** and **Kafka**.

1. **Users** — JPA entity **`UserRecord`** (id, name, balance). Rows are saved via **`DatabaseConduit`** / **`UserRepository`** (tests also use **`UserPopulator`** from fixtures).

2. **Inbound transfers (Kafka)** — Messages deserialize to **`foundation.Transaction`** (sender id, recipient id, amount) on topic **`trader-updates`** (`general.kafka-topic` in config).

3. **Consumer (test scope)** — **`KafkaTransactionListener`** in **`src/test`** is a **`@KafkaListener`** wired to **`UserRepository`** and **`TransactionRecordRepository`**. For each message it:
   - resolves sender and recipient by id;
   - **discards** the message (no DB writes) if either user is missing or the sender’s balance is below the amount;
   - otherwise **debits/credits** balances, **`save`s** both users, and **`save`s** a **`TransactionRecord`** (`@ManyToOne` to sender and recipient) inside **`@Transactional`**.

4. **Persistence model** — **`entity.TransactionRecord`** maps to table **`ledger_transaction`**. The Kafka DTO **`Transaction`** is not an entity; persisted history uses **`TransactionRecord`** only.

5. **Balance over HTTP (later tasks)** — **`Balance`** is the JSON shape for balance reads; **`BalanceQuerier`** calls `GET http://localhost:33400/balance?userId={id}` when **`TaskFiveTests`** runs with a defined port.

**Split:** `src/main` holds entities, repositories, DTOs, and Kafka **configuration**. The **Kafka consumer implementation** and **`KafkaTemplate`** producer live under **`src/test`** with the task tests.

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
| `src/main/java/.../entity` | `UserRecord`, `TransactionRecord` |
| `src/main/java/.../repository` | `UserRepository`, `TransactionRecordRepository` |
| `src/main/java/.../component` | `DatabaseConduit` |
| `src/main/java/.../foundation` | `Transaction`, `Balance` |
| `src/main/resources/application.yml` | Kafka serializers, consumer group `midas-group`, JSON trusted package / default type for `Transaction` |
| `src/test/java/...` | `KafkaProducer`, `KafkaTransactionListener`, `UserPopulator`, `FileLoader`, `BalanceQuerier`, `TaskOneTests`–`TaskFiveTests` |
| `src/test/resources/test_data/` | User and transaction line files |

Producer: `StringSerializer` + `JsonSerializer`. Consumer: `StringDeserializer` + `JsonDeserializer` for `Transaction` values.

---

## Build and run

```bash
./mvnw clean verify
./mvnw spring-boot:run
```

Windows: `mvnw.cmd` instead of `./mvnw`.

Single test class (example):

```bash
./mvnw -Dtest=TaskOneTests test
./mvnw -Dtest=TaskThreeTests test
```

**Task tests:** Several use **`@EmbeddedKafka`**. **`TaskTwoTests`–`TaskFourTests`** enter an **infinite loop** after setup so you can **debug** (e.g. breakpoints in **`KafkaTransactionListener.listen`**, inspect **`UserRepository`** / **`TransactionRecordRepository`**). Stop the run from the IDE when finished. **`mvn test`** without filtering is not a good fit for CI on this project.

---

## Debugging Task Three

After **`UserPopulator`** runs, user ids in transaction files match persisted **`UserRecord`** ids (insert order). Use the debugger on **`listen`** to step through validation, balance updates, and **`TransactionRecord`** persistence; console lines summarize **received**, **discarded**, or **recorded** rows.
