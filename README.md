# Banking Transfer POC — SAGA Choreography

Proof of concept for asynchronous bank transfers using **SAGA Choreography** over Apache Kafka, notifications through AWS SQS (LocalStack), and a web UI with a real-time animated sequence diagram.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Architecture](#2-architecture)
3. [SAGA Flow in Detail](#3-saga-flow-in-detail)
4. [Components](#4-components)
5. [Infrastructure](#5-infrastructure)
6. [Running the Project](#6-running-the-project)
7. [Web UI](#7-web-ui)
8. [Testing](#8-testing)
9. [Monitoring](#9-monitoring)
10. [Design Decisions](#10-design-decisions)

---

## 1. Overview

The system implements a bank transfer as a **Choreographed SAGA**: the sequence of operations is driven by Kafka events, with no central coordinator. Each step is independent, compensable and traceable via `sagaId`.

```
Client → API (immediate 202) → Kafka → SAGA Consumer → SQS notification
```

The client receives `202 Accepted` instantly. Processing happens in the background and can be followed in real time through the web UI.

A second, parallel SAGA implements a **Payroll Loan** product (origination + payroll deductions), reusing the same building blocks.

---

## 2. Architecture

### Sequence Diagram

```
👤 Client     ⚡ REST API     📨 Kafka        ⚙️ Consumer      🏦 AccountService  📬 SQS
    │               │               │                │                  │                │
    │─POST/transf──▶│               │                │                  │                │
    │               │──publish(evt)─▶│                │                  │                │
    │◀──202 Accepted│               │                │                  │                │
    │               │               │──consume(evt)──▶│                  │                │
    │               │               │                │─validateBalance()▶│                │
    │               │               │                │◀──OK──────────────│                │
    │               │               │                │──debit()──────────▶│                │
    │               │               │                │◀──newBalance───────│                │
    │               │               │                │──credit()─────────▶│                │
    │               │               │                │◀──newBalance───────│                │
    │               │               │                │──publish()─────────────────────────▶│
    │               │               │                │◀──messageId────────────────────────│
```

### Main Components

| Component | Responsibility |
|-----------|----------------|
| `TransferController` | Receives `POST /transfers`, validates input, returns `202` |
| `RequestTransferService` | Generates `protocolId` and `sagaId`, publishes the event to Kafka |
| `TransferProducer` | `@KafkaClient` — sends `TransferRequestedEvent` |
| `TransferKafkaListener` + `ExecuteTransferSagaService` | `@KafkaListener` — executes the 4 SAGA steps |
| `AccountService` | Sole owner of account state (thread-safe in-memory map) |
| `SqsNotificationPublisher` | Publishes JSON notifications to the SQS queue |
| `SagaTrackerAdapter` | Singleton recording every SAGA's events for the UI |
| `PayrollLoanController` | Receives `POST /loans/payroll`, validates input, returns `202` |
| `PayrollLoanService` | Prices the installment (annuity formula), stores contracts, schedules monthly deductions |
| `PayrollLoanConsumer` | `@KafkaListener` — executes the 4 steps of the origination SAGA |

The transfer domain follows a **hexagonal (ports & adapters)** layout under `transfer/`: pure domain + use cases behind inbound/outbound ports, with web, Kafka and SQS confined to adapters.

### Concurrency Principles

- `AccountService` keeps balances in a `ConcurrentHashMap<String, BigDecimal>`
- One `ReentrantReadWriteLock` per account guarantees atomic operations
- Debit and credit are reversible checkpoints (automatic compensation)
- Kafka consumer: `enable.auto.commit=false` — manual commit after processing

---

## 3. SAGA Flow in Detail

### 3.1 Happy Path (successful transfer)

```
POST /transfers
  { "sourceAccount": "ACC-001", "targetAccount": "ACC-002", "amount": 500.00 }

┌── REST API ──────────────────────────────────────────────────────────────┐
│  1. @Valid validation (source ≠ target, amount > 0)                      │
│  2. Generates protocolId = "TRF-XXXXXXXX"                                │
│  3. Generates sagaId     = "SAGA-XXXXXXXX"                               │
│  4. Publishes TransferRequestedEvent to Kafka                            │
│  5. Registers in the SagaTracker: status = PROCESSING                    │
│  6. Returns 202 Accepted                                                 │
└──────────────────────────────────────────────────────────────────────────┘

┌── Kafka Consumer (background) ───────────────────────────────────────────┐
│                                                                          │
│  STEP 1 — Validate balance (read-only, idempotent)                       │
│    AccountService.validateBalance(source, amount)                        │
│    → reads the balance under a ReadLock                                  │
│    → throws InsufficientBalanceException if balance < amount             │
│    → event: STEP1_VALIDATION                                             │
│                                                                          │
│  STEP 2 — Debit source (compensable checkpoint)                          │
│    AccountService.debit(source, amount)                                  │
│    → acquires WriteLock, subtracts, releases lock                        │
│    → returns newBalance                                                  │
│    → event: STEP2_DEBIT                                                  │
│                                                                          │
│  STEP 3 — Credit target                                                  │
│    AccountService.credit(target, amount)                                 │
│    → acquires WriteLock, adds, releases lock                             │
│    → returns newBalance                                                  │
│    → event: STEP3_CREDIT                                                 │
│                                                                          │
│  STEP 4 — SQS notification (non-critical)                                │
│    SqsNotificationPublisher.publish(notification)                        │
│    → sends JSON with sagaId, amount, sender, recipient                   │
│    → event: STEP4_SQS                                                    │
│    → a failure here does NOT undo the transfer (already completed)       │
│                                                                          │
│  → SagaTracker.complete(protocolId): status = COMPLETED                  │
│  → Manual Kafka commit (offset advances)                                 │
└──────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Cancellation on Insufficient Balance

```
STEP 1 fails → InsufficientBalanceException
  → SagaTracker.cancel(): status = CANCELED
  → Manual Kafka commit (event discarded, no reprocessing)
  → No financial operation was executed
```

### 3.3 Compensation (Step 3 failure)

```
STEP 2 completed (debit OK)
STEP 3 fails (error crediting the target)
  → AccountService.creditCompensation(source, amount)   ← reverses the debit
  → SagaTracker.cancel(): "Credit failed — debit reverted"
  → No money is lost
```

### 3.4 Critical Failure (compensation also fails)

```
STEP 2 OK → STEP 3 fails → COMPENSATION fails
  → SagaTracker.fail(): status = FAILED
  → [SAGA-CRITICAL] log entry for monitoring alerts
  → Requires manual intervention (record preserved)
```

### 3.5 SQS Failure (non-critical)

```
STEPS 1-3 completed → STEP 4 throws an SQS exception
  → Event: STEP4_SQS_FAILED
  → SagaTracker.complete(): status = COMPLETED (the transfer went through)
  → In production: the message goes to a DLQ for automatic retry
```

### 3.6 Payroll Loan SAGA

Same choreography pattern, with one structural difference: there is no
customer-account debit. "Step 2" books the contract as a bank liability (not an
account in `AccountService`), and "Step 3" credits the disbursement using
**literally the same `AccountService.credit`** used by transfers.

```
POST /loans/payroll
  { "customerAccount": "ACC-001", "enrollmentId": "ENR-001",
    "monthlyIncome": 3000.00, "requestedAmount": 5000.00, "termMonths": 36 }

┌── REST API ──────────────────────────────────────────────────────────────┐
│  1. Prices the installment with the annuity formula (fixed 1.54%/month)  │
│  2. Generates contractId = "PLN-XXXXXXXX", protocolId = "LOAN-XXXXXXXX"  │
│  3. Registers the contract as AWAITING_DISBURSEMENT                      │
│  4. Publishes PayrollLoanRequestedEvent to Kafka                         │
│  5. Returns 202 Accepted                                                 │
└──────────────────────────────────────────────────────────────────────────┘

┌── PayrollLoanConsumer (background) ──────────────────────────────────────┐
│                                                                          │
│  STEP 1 — Payroll margin (read-only, idempotent)                         │
│    the installment must not exceed 35% of the informed monthly income    │
│    → failure: PayrollMarginExceededException, contract CANCELED          │
│                                                                          │
│  STEP 2 — Activate contract (compensable checkpoint)                     │
│    status: AWAITING_DISBURSEMENT → ACTIVE                                │
│                                                                          │
│  STEP 3 — Credit customer (reuses AccountService.credit)                 │
│    → failure: contract is CANCELED (compensation — nothing disbursed)    │
│                                                                          │
│  STEP 4 — SQS notification (non-critical)                                │
│    "Payroll loan originated! A monthly installment of $X..."             │
└──────────────────────────────────────────────────────────────────────────┘
```

**Installment collection does not go through this consumer.** It is simulated
by a scheduler (`PayrollLoanService.processMonthlyDeductions`, every
`payroll-loan.deduction.interval` — 30s by default, compressed purely for the
demo) that never calls `AccountService.debit`: the deduction happens on the
benefit/payroll, before the money reaches the checking account. The
outstanding balance follows annuity amortization (the period's interest accrues
on the balance before the installment is subtracted), so the contract is paid
off exactly after `termMonths` deductions. A cycle can also be triggered
manually via `POST /loans/payroll/{id}/simulate-deduction`.

### IDs and Traceability

| Field | Prefix | Visible to | Purpose |
|-------|--------|------------|---------|
| `protocolId` | `TRF-` / `LOAN-` | Client | Receipt of the transfer / origination |
| `sagaId` | `SAGA-` | Internal | Idempotency across steps |
| `contractId` | `PLN-` | Client | Identifies the payroll loan contract |

---

## 4. Components

### 4.1 API and DTOs

**`TransferRequest`** — validated input:
```json
{
  "sourceAccount": "ACC-001",   // required, must differ from target
  "targetAccount": "ACC-002",   // required
  "amount":         500.00      // required, minimum 0.01
}
```

**`TransferResponse`** — `202 Accepted` payload:
```json
{
  "protocolId": "TRF-79D7E2F3",
  "status":     "PROCESSING",
  "message":    "Transfer received and being processed asynchronously",
  "timestamp":  "2026-06-19T15:53:09.440Z"
}
```

**Available endpoints:**

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/transfers` | Starts a transfer |
| `GET`  | `/transfers/health` | Health check |
| `GET`  | `/transfers/accounts` | Real-time balances |
| `GET`  | `/transfers/history` | SAGA history (last 50) |
| `POST` | `/loans/payroll` | Originates a payroll loan |
| `GET`  | `/loans/payroll` | Lists loan contracts |
| `GET`  | `/loans/payroll/{id}` | Detail + the contract's SAGA history |
| `POST` | `/loans/payroll/{id}/simulate-deduction` | Manually triggers one payroll deduction cycle (demo) |

### 4.2 Kafka

- **Topics:** `transfers.requested` and `loans.payroll.requested` (3 partitions, replication factor 1)
- **Producer:** `acks=all`, `enable.idempotence=true`, `retries=3`
- **Consumer:** `enable.auto.commit=false`, `auto.offset.reset=earliest`
- **Serde:** handled automatically by micronaut-kafka 5.x via `SerdeRegistry` (no explicit serializer/deserializer config)

### 4.3 AccountService

In-memory state — initial balances:

| ID | Holder | Initial balance |
|----|--------|-----------------|
| ACC-001 | Alice Johnson | $5,000.00 |
| ACC-002 | Bob Smith | $1,000.00 |
| ACC-003 | Carol Davis | $250.00 |

> **Note:** the state is volatile — restarting the container resets the balances. In production this would be replaced by a database with per-account ACID transactions.

### 4.4 SagaTracker

Singleton keeping the last 50 transfers in memory (`ConcurrentHashMap` + `ConcurrentLinkedDeque`). Feeds the `/accounts` and `/history` endpoints consumed by the web UI.

---

## 5. Infrastructure

### Docker Topology

| Container | External port | Internal port | Purpose |
|-----------|---------------|---------------|---------|
| `poc-zookeeper` | 2181 | 2181 | Kafka coordinator |
| `poc-kafka` | 9092 | 29092 | Kafka broker |
| `poc-kafdrop` | 9000 | 9000 | Kafka UI |
| `poc-localstack` | 4566 | 4566 | AWS SQS emulator |
| `poc-app` | 8080 | 8080 | Micronaut application |

### Network

Every container shares the `poc-banking-network` network. The application reaches Kafka at `kafka:29092` and LocalStack at `localstack:4566` (Docker's internal DNS resolution).

### SQS Queue

- **Name:** `notifications` (Standard Queue — not FIFO), with a `notifications-dlq` dead-letter queue
- **Created by:** `init-localstack.sh`, automatically at LocalStack startup
- **Local URL:** `http://localhost:4566/000000000000/notifications`

---

## 6. Running the Project

### Prerequisites

- Docker Desktop 4.x or newer
- Java 21+ (only to run the tests locally)
- Maven 3.9+ (only to run the tests locally)

### Start the infrastructure

```bash
# Clone the repository
git clone <repository-url>
cd banking-saga-poc

# Start all containers
docker compose up -d

# Follow the application logs
docker logs -f poc-app
```

### Check that everything is up

```bash
# Application health check (~30-60s after docker compose up)
curl http://localhost:8080/transfers/health
# Expected: {"status":"UP","kafka":"CONNECTED","sqs":"CONNECTED (LocalStack)"}

# Check every container
docker compose ps
```

### Run the demonstration script

```bash
# Runs a sequence of scenarios automatically
chmod +x test-flow.sh
./test-flow.sh
```

The script runs successful transfers, an insufficient-balance case, concurrent transfers and both payroll loan scenarios, printing formatted results to the terminal.

### Stop everything

```bash
docker compose down          # stops and removes containers (keeps volumes)
docker compose down -v       # stops, removes containers and volumes (full reset)
```

---

## 7. Web UI

Open **http://localhost:8080/ui/index.html** after `docker compose up`. There is an equivalent dashboard for the payroll loan at **http://localhost:8080/ui/payroll-loan.html** (both pages cross-link at the top) — same animated sequence-diagram pattern, adapted to the flow Step 1 (payroll margin) → Step 2 (activate contract) → Step 3 (disbursement, reusing `AccountService.credit`) → Step 4 (SQS), plus an amortization panel with an installment progress bar and a button to simulate the next payroll deduction without waiting for the scheduler. A consolidated system dashboard lives at **http://localhost:8080/ui/dashboard.html**.

### Main panel

```
┌─────────────────┬──────────────────────────────────────────────────────┐
│  💳 Accounts    │  SAGA Sequence Diagram (animated in real time)       │
│  ─────────────  │                                                      │
│  ACC-001 Alice  │  👤 Client → ⚡ API → 📨 Kafka → ⚙️ Consumer → ...   │
│  ACC-002 Bob    │                                                      │
│  ACC-003 Carol  │  [arrows animate as each step completes]             │
│                 │                                                      │
│  ➕ New Transfer│  ─────────────────────────────────────────────────   │
│  ─────────────  │  📋 Transfer History                                 │
│  Source: [   ]  │  ┌──────────────────────────────────────────────┐    │
│  Target: [   ]  │  │ TRF-79D7E2F3  Alice→Bob  $500  ✓ COMPLETED  │    │
│  Amount: [   ]  │  │ TRF-AB123456  Carol→A    $999  ✗ CANCELED   │    │
│  [Transfer →]   │  └──────────────────────────────────────────────┘    │
│                 │                                                      │
│  ⚡ Scenarios   │                                                      │
│  [Alice→Bob]    │                                                      │
│  [Carol→Alice]  │                                                      │
│  [Bob→Carol]    │                                                      │
└─────────────────┴──────────────────────────────────────────────────────┘
```

### Using the diagram

1. Click any transfer in the **history** (right column, bottom)
2. The sequence diagram **replays the full animation** of the SAGA flow
3. For in-flight transfers (`PROCESSING`): arrows appear automatically every 2 seconds
4. Click the quick scenarios to fire new transfers and watch the flow in real time

### Arrow legend

| Color | Type | Meaning |
|-------|------|---------|
| Solid blue | Synchronous call | `POST /transfers`, `debit()`, `credit()` |
| Dashed yellow | Asynchronous call | `publish(event)`, `consume(event)`, SQS `publish()` |
| Dashed green | Successful return | `202 Accepted`, `OK — sufficient balance`, `newBalance` |
| Solid red | Error / exception | `InsufficientBalanceException`, `SQS Error` |

---

## 8. Testing

### 8.1 Unit Tests

```bash
# Runs the test suite (Kafka is provided by Micronaut Test Resources/Testcontainers)
mvn test
```

Unit test coverage:

| Test class | What it covers |
|------------|----------------|
| `AccountServiceTest` | Debit, credit, compensation, insufficient balance, unknown account, thread-safety |
| `TransferControllerTest` | HTTP 202, response body, input validation, health endpoint |
| `ExecuteTransferSagaServiceTest` | SAGA choreography with mocked ports: happy path, cancellation, compensation |
| `RequestTransferServiceTest` | Request use case: ID generation, event publication, self-transfer rejection |
| `PayrollLoanServiceTest` | Annuity formula, payroll margin, contract life cycle (activate/cancel), deduction and payoff |
| `PayrollLoanControllerTest` | HTTP 202/400/404/409 with a mocked `PayrollLoanService` |

### 8.2 Tests with JaCoCo Coverage

```bash
# Runs tests + generates the coverage report
# Fails if line coverage < 80%
mvn verify

# Open the report (macOS/Linux)
open target/site/jacoco/index.html

# Open the report (Windows)
start target/site/jacoco/index.html
```

The report shows coverage per class, method and line. The 80% gate is configured in `pom.xml` via the JaCoCo plugin.

### 8.3 Automated Script

```bash
./test-flow.sh
```

The script runs, in order:
1. **Health check** — waits for the application to be up
2. **Successful transfer** — Alice (ACC-001) → Bob (ACC-002), $500
3. **SQS check** — reads the notification from the queue
4. **Insufficient balance** — Carol (ACC-003) → Alice (ACC-001), $1,000
5. **Concurrent transfers** — three parallel requests
6. **Payroll loan** — origination + margin-exceeded scenario

### 8.4 Manual Tests with cURL

#### Scenario 1 — Successful transfer

```bash
# Step 1: check balances before
curl -s http://localhost:8080/transfers/accounts | jq .

# Step 2: make the transfer
curl -s -X POST http://localhost:8080/transfers \
  -H "Content-Type: application/json" \
  -d '{"sourceAccount": "ACC-001", "targetAccount": "ACC-002", "amount": 500.00}'

# Step 3: wait for processing (~1s) and check the balances
sleep 2 && curl -s http://localhost:8080/transfers/accounts | jq .

# Step 4: check the SAGA history
curl -s http://localhost:8080/transfers/history | jq '.[0].status, [.[0].events[].step]'
# Expected: "COMPLETED" with all 5 steps
```

**Expected balances:**
- ACC-001 (Alice): $5,000.00 → $4,500.00
- ACC-002 (Bob): $1,000.00 → $1,500.00

#### Scenario 2 — Insufficient balance (SAGA cancels at Step 1)

```bash
# Carol only has $250.00
curl -s -X POST http://localhost:8080/transfers \
  -H "Content-Type: application/json" \
  -d '{"sourceAccount": "ACC-003", "targetAccount": "ACC-001", "amount": 9999.00}'

sleep 2 && curl -s http://localhost:8080/transfers/history | jq '.[0]'
# status: "CANCELED"
# No balance was changed
```

#### Scenario 3 — Input validation (returns 400)

```bash
# Unknown account
curl -v -X POST http://localhost:8080/transfers \
  -H "Content-Type: application/json" \
  -d '{"sourceAccount": "ACC-999", "targetAccount": "ACC-002", "amount": 100}'

# Source equals target
curl -v -X POST http://localhost:8080/transfers \
  -H "Content-Type: application/json" \
  -d '{"sourceAccount": "ACC-001", "targetAccount": "ACC-001", "amount": 100}'

# Negative amount
curl -v -X POST http://localhost:8080/transfers \
  -H "Content-Type: application/json" \
  -d '{"sourceAccount": "ACC-001", "targetAccount": "ACC-002", "amount": -50}'
```

#### Scenario 4 — Multiple concurrent transfers

```bash
# Fires 5 transfers in parallel (concurrency test)
for i in 1 2 3 4 5; do
  curl -s -X POST http://localhost:8080/transfers \
    -H "Content-Type: application/json" \
    -d '{"sourceAccount": "ACC-001", "targetAccount": "ACC-002", "amount": 10}' &
done
wait

sleep 3
curl -s http://localhost:8080/transfers/accounts | jq .
# ACC-001 must have exactly $50 less, regardless of processing order
```

#### Scenario 5 — Payroll loan (origination + deduction)

```bash
# Step 1: originate (income $3,000, amount $5,000, 36 months)
curl -s -X POST http://localhost:8080/loans/payroll \
  -H "Content-Type: application/json" \
  -d '{"customerAccount": "ACC-001", "enrollmentId": "ENR-001", "monthlyIncome": 3000.00, "requestedAmount": 5000.00, "termMonths": 36}'
# Keep the contractId from the response (e.g. PLN-XXXXXXXX)

# Step 2: wait for the SAGA (Step1 margin → Step2 activate → Step3 disburse → Step4 SQS)
sleep 2 && curl -s http://localhost:8080/loans/payroll/PLN-XXXXXXXX | jq .
# status: "ACTIVE", history with STEP1_MARGIN_VALIDATED, STEP2_CONTRACT_ACTIVATED, STEP3_DISBURSEMENT, STEP4_SQS

# Step 3: confirm the disbursement credited the account (reuses AccountService.credit)
curl -s http://localhost:8080/transfers/accounts | jq .

# Step 4: manually simulate one payroll deduction cycle (skipping the 30s scheduler)
curl -s -X POST http://localhost:8080/loans/payroll/PLN-XXXXXXXX/simulate-deduction | jq .
# installmentsPaid increments, outstandingBalance drops per annuity amortization — the account balance does NOT change
```

**Payroll margin exceeded (SAGA cancels at Step 1):**

```bash
# An income of $500 cannot afford the installment of a $10,000 loan over 24 months
curl -s -X POST http://localhost:8080/loans/payroll \
  -H "Content-Type: application/json" \
  -d '{"customerAccount": "ACC-002", "enrollmentId": "ENR-002", "monthlyIncome": 500.00, "requestedAmount": 10000.00, "termMonths": 24}'

sleep 2 && curl -s http://localhost:8080/loans/payroll/PLN-YYYYYYYY | jq '.status'
# "CANCELED" — nothing was credited
```

### 8.5 Inspecting SQS Notifications

```bash
# Read messages from the queue (LocalStack)
docker exec poc-localstack awslocal sqs receive-message \
  --queue-url http://localhost:4566/000000000000/notifications \
  --max-number-of-messages 10 \
  --attribute-names All

# Check the number of messages on the queue
docker exec poc-localstack awslocal sqs get-queue-attributes \
  --queue-url http://localhost:4566/000000000000/notifications \
  --attribute-names ApproximateNumberOfMessages
```

**SQS message structure:**
```json
{
  "sagaId":     "SAGA-C0E237F1",
  "transferId": "TRF-79D7E2F3",
  "type":       "TRANSFER_NOTIFICATION",
  "recipient":  "ACC-002",
  "sender":     "ACC-001",
  "amount":     300,
  "message":    "You received $300 from account ACC-001",
  "timestamp":  "2026-06-19T15:53:09Z"
}
```

### 8.6 Through the Web UI (no command line)

1. Open **http://localhost:8080/ui/index.html**
2. Use the form or the **Quick Scenarios** to fire transfers
3. Watch the animated sequence diagram in the right column
4. Click past transfers in the history to replay the flow

---

## 9. Monitoring

### Application logs

```bash
# Follow logs in real time
docker logs -f poc-app

# Filter by SAGA
docker logs poc-app 2>&1 | grep "SAGA"

# Filter errors
docker logs poc-app 2>&1 | grep -E "ERROR|CANCELED|CRITICAL"
```

Log pattern per step:

```
=== SAGA STARTED | sagaId=SAGA-xxx trf=TRF-xxx ===
[SAGA-STEP-2] ✓ Debit completed | account=ACC-001 balance=4500.00
[SAGA-STEP-3] ✓ Credit completed | account=ACC-002 balance=1500.00
[SAGA-STEP-4] ✓ SQS notification sent | sagaId=SAGA-xxx
=== SAGA COMPLETED ✓ | sagaId=SAGA-xxx source=ACC-001 target=ACC-002 amount=500 ===
```

### Kafdrop — Kafka UI

Open **http://localhost:9000**

- **Topics:** inspect the `transfers.requested` topic, partitions and offsets
- **Messages:** inspect each published event's JSON payload
- **Consumer Groups:** check the `transfer-processor` consumer lag

To see raw messages:
1. Click `transfers.requested`
2. Click **View Messages**
3. Pick the partition and offset

### Diagnostic endpoints

```bash
# Overall status
curl -s http://localhost:8080/transfers/health | jq .

# Current balances of every account
curl -s http://localhost:8080/transfers/accounts | jq .

# Last 50 transfers with the SAGA event history
curl -s http://localhost:8080/transfers/history | jq '.[0]'
```

---

## 10. Design Decisions

### Why SAGA Choreography (and not Orchestration)?

| Aspect | Choreography (this project) | Orchestration |
|--------|-----------------------------|---------------|
| Coordination | Each service reacts to events | A central orchestrator |
| Single point of failure | None | The orchestrator |
| Coupling | Low (only via Kafka) | High (dependency on the orchestrator) |
| Flow visibility | Distributed (requires tracing) | Centralized |
| Kafka fit | Natural | Requires adaptation |

### Why eventual consistency?

2PC (Two-Phase Commit) does not scale in microservices — it locks resources indefinitely. Compensation (automatic reversal) is the equivalent banking pattern: the debit is reverted if the credit fails, exactly as in real clearing-house systems.

### Why `enable.auto.commit=false`?

With auto-commit, a message could be marked as processed before Step 4 finished. With manual commit, the offset only advances after every step — guaranteeing reprocessing on a crash (at-least-once delivery).

### Why is SQS non-critical?

Steps 1-3 are financial: they affect balances. Step 4 is a notification: the money has already moved successfully. A notification failure must not undo the transfer — it goes to a DLQ for a later retry, as in real banking systems.

### Why in-memory state?

This is a **POC** focused on the SAGA pattern and the Kafka/SQS integration. In production, `AccountService` would be replaced by:
- A relational database (PostgreSQL) with per-account `SELECT ... FOR UPDATE`
- Or a NoSQL store with per-document optimistic locking
- The current `ReentrantReadWriteLock` locks simulate exactly that semantics

---

## Project Structure

```
src/
├── main/
│   ├── java/com/bank/poc/
│   │   ├── BankingApplication.java              # Micronaut entrypoint
│   │   ├── transfer/                            # Hexagonal transfer module
│   │   │   ├── domain/
│   │   │   │   ├── Transfer.java                # Aggregate root
│   │   │   │   ├── Notification.java
│   │   │   │   ├── event/                       # TransferRequestedEvent, DebitCompletedEvent
│   │   │   │   ├── exception/                   # InvalidTransferException
│   │   │   │   └── port/                        # in: use cases · out: gateways
│   │   │   ├── application/
│   │   │   │   ├── RequestTransferService.java  # Request use case
│   │   │   │   └── ExecuteTransferSagaService.java # SAGA choreography
│   │   │   └── adapter/
│   │   │       ├── in/web/                      # TransferController + DTOs
│   │   │       ├── in/messaging/                # TransferKafkaListener
│   │   │       └── out/                         # Kafka, SQS, AccountGateway, SagaTracker
│   │   ├── controller/
│   │   │   └── PayrollLoanController.java       # Payroll loan REST endpoints
│   │   ├── service/
│   │   │   ├── AccountService.java              # Account state + operations
│   │   │   └── PayrollLoanService.java          # Installment pricing, contracts, scheduled deductions
│   │   ├── kafka/
│   │   │   ├── PayrollLoanProducer.java         # @KafkaClient producer
│   │   │   └── PayrollLoanConsumer.java         # @KafkaListener + origination SAGA
│   │   ├── sqs/
│   │   │   └── SqsNotificationPublisher.java    # AWS SDK v2 → LocalStack
│   │   ├── event/
│   │   │   └── PayrollLoanRequestedEvent.java
│   │   ├── model/
│   │   │   ├── SagaState.java
│   │   │   └── ContractStatus.java
│   │   ├── dto/
│   │   │   ├── PayrollLoanRequest.java
│   │   │   └── PayrollLoanResponse.java
│   │   └── exception/
│   │       ├── InsufficientBalanceException.java
│   │       ├── AccountNotFoundException.java
│   │       ├── PayrollMarginExceededException.java
│   │       └── ContractNotFoundException.java
│   └── resources/
│       ├── application.yml                      # Kafka, SQS, static resources
│       ├── logback.xml                          # Console appender
│       └── public/
│           ├── index.html                       # Transfer dashboard (animated SAGA diagram)
│           ├── payroll-loan.html                # Payroll loan dashboard
│           ├── dashboard.html                   # Consolidated system dashboard
│           └── chatbot.js / chatbot.css         # Offline contextual assistant widget
└── test/
    ├── java/com/bank/poc/
    │   ├── service/                             # AccountServiceTest, PayrollLoanServiceTest
    │   ├── controller/                          # PayrollLoanControllerTest
    │   └── transfer/                            # TransferControllerTest + use case tests
    └── resources/
        └── application-test.yml                 # Disables the scheduled deduction during tests
```

---

## Quick Links

| Resource | URL |
|----------|-----|
| Web UI (SAGA Dashboard) | http://localhost:8080/ui/index.html |
| Payroll Loan Dashboard | http://localhost:8080/ui/payroll-loan.html |
| System Dashboard | http://localhost:8080/ui/dashboard.html |
| Health Check | http://localhost:8080/transfers/health |
| Account Balances | http://localhost:8080/transfers/accounts |
| SAGA History | http://localhost:8080/transfers/history |
| Kafdrop (Kafka UI) | http://localhost:9000 |
| LocalStack SQS | http://localhost:4566 |

---

## Tech Stack

| Technology | Version | Role |
|------------|---------|------|
| Java | 21 | Runtime |
| Micronaut | 4.4.3 | Framework (Netty HTTP server) |
| Apache Kafka | 7.6.1 (Confluent) | Message broker |
| micronaut-kafka | 5.x | Kafka integration |
| AWS SDK v2 | latest | SQS client |
| LocalStack | 3.4 | Local AWS emulator |
| JUnit 5 | latest | Unit tests |
| Mockito | latest | Test mocks |
| JaCoCo | latest | Coverage (≥80% gate) |
| Docker Compose | — | Local orchestration |
