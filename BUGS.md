# Debugging Report — Four Bugs in `eventstats`

## Bug 1: Local Deduplication Map (Batch Scope vs. File Scope)
- **What the bug is:** Event deduplication only functioned within individual batches of 200 events rather than across the entire dataset.
- **Why it happens:** In `processBatch()`, `seen := make(map[string]bool)` was instantiated locally. Each batch began with an empty map. When duplicate `event_id`s appeared across different batches, they were treated as new events, inflating all statistics.
- **Fix:** Promoted `seen` to package-level scope (`var seen = map[string]bool{}`) and removed the local re-instantiation inside `processBatch()`.
- **How verified:** Verified that total event counts decreased to match `expected_output.txt`, confirming duplicate events across batches are correctly ignored.

---

## Bug 2: Global Contact Tracking for `unique_opens`
- **What the bug is:** `unique_opens` undercounted recipient reach when the same contact opened messages across multiple distinct campaigns.
- **Why it happens:** The tracking map `var openedBy = map[string]bool{}` used only `ev.ContactID` as the key. If contact `ct_01` opened Campaign A, `openedBy["ct_01"]` was set to `true`. When `ct_01` subsequently opened Campaign B, the check `if !openedBy[ev.ContactID]` evaluated to `false`, preventing Campaign B from registering the unique open.
- **Fix:** Composite-keyed the map by campaign and contact: `key := ev.CampaignID + ":" + ev.ContactID`, ensuring unique open status is tracked independently per campaign.
- **How verified:** Inspected `unique_opens` in `actual.txt` for campaigns B, C, D, and E; values matched `expected_output.txt` exactly.

---

## Bug 3: Daily Delivery Bucketing Uses Local Machine Timezone
- **What it is:** Delivery dates were bucketed using the local machine's timezone instead of UTC.
- **Why it happens:** Line 144 executed `day := ev.Timestamp.Local().Format("2006-01-02")`. The `.Local()` call shifts UTC timestamps by the machine's timezone offset (e.g. UTC+5:30 in IST), pushing late-night deliveries into the following calendar day.
- **Fix:** Changed `.Local()` to `.UTC()`: `day := ev.Timestamp.UTC().Format("2006-01-02")`.
- **How verified:** Delivery counts across all calendar days (2026-08-01 through 2026-08-07) now align identically with `expected_output.txt`.

---

## Bug 4: Data Race on Counter Increments in Worker Goroutines
- **What it is:** Unsynchronized concurrent updates to `CampaignStats` counters across 8 worker goroutines.
- **Why it happens:** In `processBatch()`, 8 worker goroutines concurrently pull events from the `jobs` channel and call `apply(ev)`. `apply()` directly mutates `cs.Sent++`, `cs.Delivered++`, `cs.Opened++`, and `cs.Clicked++` on shared pointers. Because integer increment in Go is a non-atomic read-modify-write operation, concurrent updates produce lost updates and data races.
- **Fix:** Added a package-level mutex `var applyMu sync.Mutex` and wrapped counter increments in `apply()` with `applyMu.Lock()` and `applyMu.Unlock()`.
- **How verified:** The program executes safely without lost increments, and output remains deterministic across repeated runs.