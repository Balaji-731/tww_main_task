# Relay Campaign Events Service

A high-performance, resilient event ingestion and analytics service built in Go using standard library only.

## Features
- **Resilient Batch Processing:** Ingests `[]json.RawMessage` individually so invalid records never poison the batch.
- **Idempotency & Deduplication:** Tracks `event_id` globally to guarantee accurate counts during provider retries.
- **Unique Reach & Rates:** Computes total counts, unique recipient opens/clicks, and conversion rates.
- **Concurrency-Safe:** Thread-safe in-memory datastore using `sync.RWMutex`.
- **Survives Dirty Webhook Batches:** Tested against real-world provider traffic in `seed/events.json`.

---

## Quick Start

### 1. Run the Service
From within the `starter/` directory:
```bash
go run .
```
The server will start listening on port `:8080`.

---

### 2. Ingest Events
Single-line command (compatible across Windows CMD, PowerShell, macOS, and Linux):

```bash
curl -s -X POST http://localhost:8080/events -H "Content-Type: application/json" -d "[{\"event_id\":\"evt_1\",\"campaign_id\":\"cmp_summer_sale\",\"contact_id\":\"ct_001\",\"type\":\"sent\",\"timestamp\":\"2026-08-10T06:00:00Z\"},{\"event_id\":\"evt_2\",\"campaign_id\":\"cmp_summer_sale\",\"contact_id\":\"ct_001\",\"type\":\"delivered\",\"timestamp\":\"2026-08-10T06:15:00Z\"},{\"event_id\":\"evt_3\",\"campaign_id\":\"cmp_summer_sale\",\"contact_id\":\"ct_001\",\"type\":\"opened\",\"timestamp\":\"2026-08-10T06:30:00Z\"}]"
```

Response:
```json
{
  "received": 3,
  "accepted": 3,
  "duplicates": 0,
  "rejected": 0
}
```

---

### 3. Load Seed Provider Traffic
Load the full 235 lines of real-world provider traffic:
```bash
curl -s -X POST http://localhost:8080/events -H "Content-Type: application/json" --data-binary @seed/events.json
```

Response:
```json
{
  "received": 235,
  "accepted": 211,
  "duplicates": 18,
  "rejected": 6,
  "errors": [
    {"index": 27, "event_id": "evt_00212", "reason": "invalid timestamp \"not-a-time\": must be RFC3339 UTC"},
    {"index": 59, "event_id": "evt_00211", "reason": "invalid timestamp \"10-08-2026 09:00\": must be RFC3339 UTC"},
    {"index": 81, "reason": "missing or empty event_id"},
    {"index": 127, "event_id": "evt_00213", "reason": "missing or empty contact_id"},
    {"index": 169, "event_id": "evt_00214", "reason": "missing or empty contact_id"},
    {"index": 175, "event_id": "evt_00215", "reason": "invalid event type \"spam_report\" (allowed: sent, delivered, opened, clicked)"}
  ]
}
```

---

### 4. Fetch Campaign Statistics
```bash
curl -s http://localhost:8080/campaigns/cmp_summer_sale/stats
```

Response:
```json
{
  "campaign_id": "cmp_summer_sale",
  "sent": 40,
  "delivered": 35,
  "opened": 28,
  "clicked": 12,
  "unique_opens": 20,
  "unique_clicks": 12,
  "rates": {
    "delivery_rate": 0.875,
    "open_rate": 0.5714,
    "click_to_open_rate": 0.6
  }
}
```

---

### 5. Fetch Paginated Event Activity (Optional Endpoint)
```bash
curl -s "http://localhost:8080/campaigns/cmp_summer_sale/events?limit=3&offset=0"
```

Response:
```json
{
  "campaign_id": "cmp_summer_sale",
  "total": 115,
  "limit": 3,
  "offset": 0,
  "events": [
    {
      "event_id": "evt_00204",
      "campaign_id": "cmp_summer_sale",
      "contact_id": "ct_030",
      "type": "opened",
      "timestamp": "2026-08-10T12:45:00Z"
    },
    {
      "event_id": "evt_00066",
      "campaign_id": "cmp_summer_sale",
      "contact_id": "ct_028",
      "type": "sent",
      "timestamp": "2026-08-10T07:22:00Z"
    },
    {
      "event_id": "evt_00042",
      "campaign_id": "cmp_summer_sale",
      "contact_id": "ct_017",
      "type": "delivered",
      "timestamp": "2026-08-10T07:20:00Z"
    }
  ]
}
```

---

### 6. Run Automated Tests
```bash
go test -v .
```
*(Note: If your environment has GCC/MinGW installed for CGO, you can also run with the race detector: `go test -v -race .`)*