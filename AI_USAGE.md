# AI Usage Disclosure

## 1. Tools Used
- **Antigravity / Gemini 3.8 Flash:** Used as an interactive pair-programming and design assistant throughout the task.
- **Go Toolchain (`go test`, `go run`):** For executing tests, reproducing race conditions, and verifying output.
- **Windows PowerShell / `fc.exe`:** For running batch curl requests and verifying text output against ground truth.

---

## 2. What AI Was Used For
- **Edge-Case Brainstorming:** Inspecting `seed/events.json` to identify unparseable timestamps (`"not-a-time"`), non-standard date formats (`"10-08-2026 09:00"`), empty event/contact IDs, and unknown event types (`"spam_report"`).
- **Ingestion Architecture:** Deciding how to structure the batch ingestion endpoint with `[]json.RawMessage` so a single bad record does not poison an entire batch of 200 events.
- **Debugging Assistance:** Identifying and reasoning about the concurrency data race and the cross-campaign `unique_opens` tracking bug in `debugging/main.go`.
- **Drafting Documentation:** Structuring and drafting `NOTES.md` (Scale Memo, Forensic Marketer Analysis) and `BUGS.md`.

---

## 3. One Suggestion From AI That Was Rejected or Changed, and Why
- **Rejection of SQLite / CGO Persistence:**
  - The AI initially suggested persisting events in an SQLite database using an embedded driver.
  - **Why rejected:** On Windows machines, CGO-based SQLite drivers require an external GCC/MinGW C compiler toolchain. Introducing CGO would violate the assignment's emphasis on clean simplicity, make `go run .` fail on environments without MinGW, and add unnecessary complexity for an MVP handling a few thousand events.
  - **Decision:** Maintained an in-memory repository protected by a fine-grained `sync.RWMutex`, which runs natively on any machine with zero external dependencies and sub-millisecond response times.

---

## 4. One Thing AI Helped Me Understand
- **The Subtle Cross-Campaign Bug in `debugging/main.go`:**
  - In `debugging/main.go`, `openedBy` was initially declared as `map[string]bool` and keyed only by `ev.ContactID`.
  - It was easy to overlook that a recipient opening an email in Campaign A was marking their ID as seen globally. When that same contact subsequently opened an email in Campaign B, Campaign B’s `UniqueOpens` was silently ignored. AI helped clarify the requirement that a contact opening 5 times in a campaign counts once, but opening across 2 campaigns must count once *in each*. Composite-keying the map by `campaignID + ":" + contactID` cleanly resolved this.

---

## 5. Anything AI Generated That Had to Be Debugged
1. **Missing `"fmt"` Import in `starter/main_test.go`:**
   - In `TestConcurrentIngestion`, `fmt.Sprintf` was used to format JSON payloads dynamically inside worker goroutines, but `"fmt"` was omitted from the test file's imports. We caught and fixed this during code review before running the test suite.
2. **Windows `-race` Flag CGO Dependency:**
   - The test script initially suggested running `go test -v -race .`. On Windows without a pre-configured C compiler, Go returned `go: -race requires cgo; enable cgo by setting CGO_ENABLED=1`. We adjusted our testing workflow to run pure Go unit and concurrency tests using `go test -v .`.