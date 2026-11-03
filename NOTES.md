# Engineering Notes — Campaign Events Service

## Part 1 — Problem Interpretation, Assumptions & Design Decisions

### 1. Problem Interpretation
Relay acts as a tracking and aggregation service between marketing clients and third-party delivery providers (SendGrid, Twilio, etc.). Providers deliver the messages and send back asynchronous webhook events: `sent`, `delivered`, `opened`, `clicked`.

The core technical realities are:
- **At-least-once delivery:** Providers retry on any non-2xx response or timeout. Duplicate events are guaranteed.
- **Out-of-order delivery:** Events arrive over diverse network pathways; an `opened` or `clicked` event may arrive before `delivered`.
- **Dirty batch data:** Providers send mixed batches containing valid events, invalid timestamps, missing fields, and unrecognized event types.
- **Concurrency:** Multiple webhook workers push events concurrently for the same campaign, requiring race-free state updates.
- **Dashboard integrity:** Marketers require live, trustworthy counts and conversion rates without duplicate inflation.

---

### 2. Assumptions Made

1. **Event Identity & Deduplication:**
   - The primary deduplication key is `event_id`.
   - If an `event_id` is received that has already been ingested, it is identified as a duplicate and ignored for counter increments, but acknowledged in the API response to satisfy provider retries.
   - If an event arrives with an existing `event_id` but conflicting payload data (e.g., different event type), the **first-seen event wins**; subsequent conflicting payloads are rejected or treated as duplicates to preserve immutability.

2. **Validation Rules:**
   - **Required fields:** `event_id` (non-empty string), `campaign_id` (non-empty string), `contact_id` (non-empty string), `type` (one of `sent`, `delivered`, `opened`, `clicked`), and `timestamp` (valid RFC3339 / ISO 8601).
   - **Type Normalization:** Event types are checked case-insensitively (e.g., `"OPENED"` is normalized to `"opened"`). Unknown event types like `"spam_report"` are rejected as unsupported by the current schema.
   - **Timestamps:** Timestamps must be valid RFC3339 UTC. Malformed timestamps (e.g., `"not-a-time"`, `"10-08-2026 09:00"`) are rejected.

3. **Batch Ingestion Semantics (Partial Success):**
   - Batches are decoded item-by-item (`[]json.RawMessage`). A malformed or invalid event at index 28 does **not** fail the remaining 199 valid events.
   - The endpoint returns HTTP `200 OK` (or `207 Multi-Status`) with a structured summary (`received`, `accepted`, `duplicates`, `rejected`, `errors`) so providers do not trigger infinite retry loops for bad individual records.

4. **Storage & Concurrency:**
   - An in-memory store protected by a `sync.RWMutex` provides zero-dependency, high-throughput thread safety for the prototype.
   - Unique recipients (`unique_opens`, `unique_clicks`) are tracked using per-campaign sets (`map[string]struct{}`) of `contact_id`s.

5. **Campaign Stats Definitions:**
   - `sent`, `delivered`, `opened`, `clicked`: Raw count of distinct, valid events.
   - `unique_opens`: Number of distinct contacts who opened at least once in that campaign.
   - `unique_clicks`: Number of distinct contacts who clicked at least once in that campaign.
   - Rates:
     - `delivery_rate = delivered / sent` (if sent > 0)
     - `open_rate = unique_opens / delivered` (if delivered > 0)
     - `click_to_open_rate = unique_clicks / unique_opens` (if unique_opens > 0)

---

### 3. Ambiguities in the Brief

1. **Definition of "How many opened":** Does the dashboard need total interaction volume (raw open events) or recipient reach (unique contacts opened)? Marketers need both: raw opens indicate engagement frequency, while unique opens determine true audience reach.
2. **Batch Failure Status Code:** Standard HTTP doesn't define a single behavior for partial batch failures. If we return `400 Bad Request` because 1 event in 200 is bad, the provider retries the whole batch, amplifying traffic. Partial success reporting is necessary.
3. **Implicit Campaign Lifecycle:** Can an event create a campaign on the fly, or must campaigns be registered beforehand? (We assume dynamic creation on first event).
4. **Handling Out-of-Order Delivered vs. Sent:** If `delivered` arrives before `sent`, should counters reflect `delivered > sent` temporarily? (Yes, real-time counters should reflect reality as events arrive).

---

### 4. Questions I Would Ask the PM

1. *"Should the stats API return total raw opens/clicks, unique recipient opens/clicks, or both?"*
2. *"What should happen when a provider sends an unknown event type like `bounced` or `spam_report`? Should we store them for future features or reject them?"*
3. *"Does the dashboard need date-filtered metrics (e.g., today, last 7 days) or strictly lifetime metrics per campaign?"*
4. *"Do we need to store raw event history and metadata for audit trails, or are aggregated counters sufficient for MVP?"*

---

### 5. Prioritization Plan

1. **Ingestion Resilience:** Stream/RawMessage JSON parsing to ensure a single malformed event does not fail an entire batch.
2. **Idempotency & Deduplication:** Global `event_id` tracking to prevent double-counting.
3. **Concurrency Safety:** Read/Write mutex protection over in-memory campaign state.
4. **Stats API Design:** Clean, intuitive JSON response providing raw counts, unique reach, and conversion rates.
5. **Seed Data Verification:** Testing against `seed/events.json` to verify zero crashes and proper rejection of bad data.



---

## Part 4 — Scale Memo: Scaling to 100 Million Events / Day

### 1. What Breaks First in the Current Implementation?
1. **Memory Exhaustion (RAM Crash):** The service stores all event IDs (`seenEvents`) and contact sets in RAM. 100M events/day with 36-character UUIDs and metadata requires 10–25 GB of RAM daily. Within days, the process will trigger an Out-Of-Memory (OOM) kill.
2. **Global Mutex Contention:** A single `sync.RWMutex` serializing writes across all campaigns will experience severe lock contention at 5,000–10,000 requests/sec.
3. **Webhook Timeouts & Retry Avalanches:** Synchronously unmarshaling, validating, and recording large batches will push HTTP response latency past provider timeout thresholds (often 2–5 seconds). Providers will retry timed-out batches, exponentially amplifying incoming traffic.

---

### 2. Architectural Evolution & Order of Changes

```
[Providers] 
     │
     ▼ (HTTPS POST /events)
[Stateless Ingestion API]  ──(Push raw bytes)──► [Kafka / Kinesis Partitioned Queue]
     │ (Return 202 in <10ms)                           │
                                                       ▼
                                            [Stream Consumer Workers]
                                           (Partitioned by campaign_id)
                                                       │
                           ┌───────────────────────────┴───────────────────────────┐
                           ▼                                                       ▼
            [Redis Sliding Dedup (SET NX)]                              [ClickHouse / DynamoDB]
                 (7-day TTL on event_id)                                 (Immutable Raw Events +
                                                                         Pre-aggregated Counters)
```

1. **Phase 1: Asynchronous Ingestion Buffer (Introduce Message Queue):**
   - Decouple webhook ingestion from processing using Apache Kafka or AWS Kinesis.
   - The HTTP API performs minimal JSON syntax validation, writes the raw batch to a Kafka topic, and immediately returns HTTP `202 Accepted` in <10ms.
2. **Phase 2: Partitioning by `campaign_id`:**
   - Partition Kafka topics by `campaign_id`. This guarantees events for any single campaign are processed sequentially by a dedicated worker, eliminating cross-thread lock contention without needing global mutexes.
3. **Phase 3: Persistent Distributed Storage:**
   - Store immutable raw events in a columnar analytical database like **ClickHouse** or **BigQuery** (designed for scanning billions of rows per second).
   - Store live aggregated counters and contact sets in **Redis** (HyperLogLog for approximate unique reach at scale, or Redis Sets for exact reach).
4. **Phase 4: Distributed Deduplication:**
   - Use Redis with a sliding TTL (e.g. 7 days): `SET event:{event_id} 1 EX 604800 NX`. If the key exists, drop the event as a duplicate.

---

### 3. API Contract and Storage Changes
- **API Status Code:** `POST /events` changes from `200 OK` (with inline error breakdown) to `202 Accepted` with a `batch_id`. Detailed per-event error notifications can be streamed to a dead-letter queue (DLQ) or webhook error log.
- **Storage:** Transition from volatile in-memory maps to an immutable append-only event log (ClickHouse) paired with a fast key-value cache (Redis) for real-time dashboard reads.

---

### 4. Queue Trade-offs & Where Duplicates Sneak In
- **New Problems:**
  - *At-least-once delivery:* Queues do not guarantee exactly-once delivery; network partitions or consumer restarts cause messages to be redelivered.
  - *Consumer lag & backpressure management:* A surge in provider events can create lag, delaying dashboard updates.
  - *Dead-letter queues (DLQ):* Poison-pill messages (badly malformed payloads) must be shunted to a DLQ so they do not block worker consumer offsets.
- **Where duplicates sneak in:** If a worker processes an event, increments a database counter, but crashes *before* acknowledging its Kafka offset, the broker reassigns the partition and a second worker processes the exact same event. Worker-level idempotency via database atomic upserts (`INSERT ... ON CONFLICT DO NOTHING`) is mandatory.

---

### 5. Keeping Counters Trustworthy
- **Two-Tier Architecture (Speed Layer + Batch Truth):**
  - *Speed Layer (Redis):* Workers increment live counters in Redis for real-time dashboard responsiveness.
  - *Batch Truth Layer (ClickHouse):* A scheduled hourly/nightly reconciliation job executes `SELECT count(DISTINCT contact_id) FROM events WHERE campaign_id = ?` against the append-only event log and syncs Redis if any counter drift occurred.

---

### 6. What to Monitor
- **Ingestion p95/p99 Latency:** Alert if HTTP response time exceeds 200ms.
- **Queue Consumer Lag:** Measure the delta between produced and consumed offsets.
- **Provider Retry & Duplicate Rate:** A sudden spike in duplicates indicates upstream provider timeout issues.
- **Dead-Letter Queue (DLQ) Rate:** Alert on any sudden rise in unparseable events.

---

### 7. What Deliberately NOT to Solve Yet
- **Multi-Region Active-Active Consistency:** Avoid complex cross-continent distributed consensus until global latency dictates it.
- **Real-Time Arbitrary Slice-and-Dice Analytics:** Avoid dynamic OLAP filtering (e.g., "users in France who opened on iOS between 2 PM and 4 PM") on the live dashboard until dedicated analytical data warehouses are funded.

---

## Part 5 — Forensic Investigation: The Angry Marketer

### 1. The Scenario
- **10:00 AM:** Sent: 1,000,000 | Delivered: 970,000 | Opened: 250,000 | Clicked: 20,000
- **10:30 AM:** Sent: 1,000,000 | Delivered: 975,000 | Opened: 248,000 | Clicked: 20,000
- **Change:** Delivered rose by **+5,000**; Opened fell by **-2,000**. Marketer claims: *"Your dashboard is broken."*

---

### 2. Is Delivered Rising (+5,000) Suspicious?
**No, it is completely normal and expected.**
Delivery providers (especially for SMS and email) receive delivery receipts asynchronously from carrier networks and mail exchange servers. Cellular towers and receiving MX servers frequently queue messages during peak times; delivery confirmations commonly arrive 15–45 minutes after dispatch.

---

### 3. Plausible Non-Bug Explanations for Opened Dropping (-2,000)

1. **Automated Bot & Security Scanner Filtering (Most Likely):**
   - Modern email systems (e.g., Apple Mail Privacy Protection, Microsoft Defender, Google Workspace) automatically pre-fetch tracking pixels to scan for phishing links upon inbox arrival.
   - Initial webhook events report these pre-fetches as `opened`.
   - Between 10:00 and 10:30, the provider's or Relay's bot-detection heuristics identified automated user agents, IP blocks, or pre-fetch signatures and purged the artificial opens to protect the marketer's actual deliverability metrics.
2. **Rolling Time Window Filter on the Dashboard:**
   - The marketer’s dashboard view may have been set to a rolling window (e.g., "Last 24 Hours").
   - Events that occurred 24.5 hours ago aged out of the active window between 10:00 and 10:30, causing the aggregate metric to decrease.
3. **GDPR / Privacy "Right to be Forgotten" Erasure:**
   - A privacy erasure or suppression list request was processed, removing 2,000 contacts and their historical event interactions from the campaign's statistics.
4. **Deduplication / Compaction Reconciliation:**
   - High concurrent ingestion at 10:00 might have temporarily allowed duplicated contact opens into a dirty read cache; a background reconciliation job at 10:15 recomputed the true distinct contact count (`unique_opens`), normalizing the metric down to 248,000.

---

### 4. Forensic Investigation Workflow

1. **Inspect Query Parameters:** Check if the marketer's dashboard had active filters (date range, timezone, device, rolling window).
2. **Audit Background System Jobs:** Review logs between 10:00 and 10:30 for bot suppression jobs, privacy erasure batches, or counter reconciliation workers.
3. **Query the Immutable Event Log:**
   Execute a ground-truth distinct count query directly on the raw events table:
   ```sql
   SELECT 
       count(DISTINCT contact_id) AS true_unique_opens,
       count(*) AS total_raw_opens
   FROM events 
   WHERE campaign_id = 'cmp_xxx' AND type = 'opened';
   ```
4. **Inspect Provider Webhook Logs:** Check if the delivery provider issued event retraction or cancellation webhooks (e.g., SendGrid/Mailgun bot suppression updates).

---

## What I Completed / What I Intentionally Skipped

### What I Completed:
1. **Resilient Webhook Ingestion (`POST /events`):** Implemented item-by-item decoding using `[]json.RawMessage` to ensure malformed JSON records or bad timestamps never abort valid events in a batch.
2. **Idempotency & Deduplication:** Global `event_id` tracking preventing duplicate counts across provider retries.
3. **Contact-Level Reach & Rates (`GET /stats`):** Calculated raw counts, unique recipient opens/clicks, delivery rate, open rate, and click-to-open rate without race conditions.
4. **Paginated Event Log (`GET /events`):** Implemented the optional paginated historical activity endpoint with limit and offset query parameters.
5. **Survives Dirty Provider Traffic:** Tested against all 235 lines of dirty data in `seed/events.json` with 100% survival.
6. **Debugging Exercise:** Identified, explained, and fixed all 4 bugs in `debugging/main.go`, achieving a byte-for-byte exact match against `expected_output.txt`.

### What I Intentionally Skipped:
1. **External Database (SQLite / Postgres):** Chose an in-memory datastore with `sync.RWMutex` to keep the submission zero-dependency, zero-CGO, fast, and simple to run on any machine.
2. **Authentication / Authorization:** Left endpoints unauthenticated as instructed by the assignment guidelines to focus strictly on event processing logic.
3. **Complex Frontend:** Bypassed UI build to prioritize backend resilience, concurrency safety, and comprehensive unit test coverage.

---

## If I Had Another Day

1. **Persistent SQLite / WAL Storage:** Transition from in-memory maps to SQLite with Write-Ahead Logging (WAL) enabled, providing disk persistence and crash recovery while retaining single-binary simplicity.
2. **Dead-Letter Queue & Ingestion Telemetry:** Build a structured logging and dead-letter pipeline for rejected events so engineering can monitor provider schema drift in real time.
3. **Time-Bucket Filtering:** Add query parameter support to `GET /stats` (e.g., `?from=2026-08-01&to=2026-08-07`) allowing marketers to view engagement trends over custom date intervals.
4. **Benchmark Suite:** Write Go benchmark tests (`go test -bench .`) to profile memory allocation overhead under sustained high-throughput loads.
