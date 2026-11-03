package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
)

func TestIngestAndDeduplication(t *testing.T) {
	server := NewServer()

	// Ingest first event
	payload := `[
		{"event_id":"evt_1","campaign_id":"cmp_test","contact_id":"ct_1","type":"sent","timestamp":"2026-08-10T06:00:00Z"},
		{"event_id":"evt_2","campaign_id":"cmp_test","contact_id":"ct_1","type":"delivered","timestamp":"2026-08-10T06:05:00Z"},
		{"event_id":"evt_3","campaign_id":"cmp_test","contact_id":"ct_1","type":"opened","timestamp":"2026-08-10T06:10:00Z"}
	]`

	req := httptest.NewRequest("POST", "/events", bytes.NewBufferString(payload))
	w := httptest.NewRecorder()
	server.handlePostEvents(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var batchResp BatchResponse
	if err := json.Unmarshal(w.Body.Bytes(), &batchResp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if batchResp.Accepted != 3 || batchResp.Duplicates != 0 {
		t.Fatalf("expected 3 accepted, 0 duplicates; got %+v", batchResp)
	}

	// Resend identical batch (simulating provider retry)
	reqRetry := httptest.NewRequest("POST", "/events", bytes.NewBufferString(payload))
	wRetry := httptest.NewRecorder()
	server.handlePostEvents(wRetry, reqRetry)

	if err := json.Unmarshal(wRetry.Body.Bytes(), &batchResp); err != nil {
		t.Fatalf("failed to decode retry response: %v", err)
	}
	if batchResp.Accepted != 0 || batchResp.Duplicates != 3 {
		t.Fatalf("expected 0 accepted, 3 duplicates; got %+v", batchResp)
	}

	// Verify stats did not double count
	statsReq := httptest.NewRequest("GET", "/campaigns/cmp_test/stats", nil)
	statsReq.SetPathValue("campaignID", "cmp_test")
	wStats := httptest.NewRecorder()
	server.handleGetStats(wStats, statsReq)

	var stats CampaignStats
	if err := json.Unmarshal(wStats.Body.Bytes(), &stats); err != nil {
		t.Fatalf("failed to decode stats: %v", err)
	}
	if stats.Sent != 1 || stats.Delivered != 1 || stats.Opened != 1 || stats.UniqueOpens != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestPartialBatchFailure(t *testing.T) {
	server := NewServer()

	// 1 valid, 1 bad timestamp, 1 empty event_id, 1 unknown type
	payload := `[
		{"event_id":"evt_good","campaign_id":"cmp_partial","contact_id":"ct_1","type":"sent","timestamp":"2026-08-10T06:00:00Z"},
		{"event_id":"evt_bad_time","campaign_id":"cmp_partial","contact_id":"ct_2","type":"sent","timestamp":"not-a-time"},
		{"event_id":"","campaign_id":"cmp_partial","contact_id":"ct_3","type":"sent","timestamp":"2026-08-10T06:00:00Z"},
		{"event_id":"evt_bad_type","campaign_id":"cmp_partial","contact_id":"ct_4","type":"spam_report","timestamp":"2026-08-10T06:00:00Z"}
	]`

	req := httptest.NewRequest("POST", "/events", bytes.NewBufferString(payload))
	w := httptest.NewRecorder()
	server.handlePostEvents(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var batchResp BatchResponse
	if err := json.Unmarshal(w.Body.Bytes(), &batchResp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if batchResp.Accepted != 1 || batchResp.Rejected != 3 {
		t.Fatalf("expected 1 accepted and 3 rejected; got %+v", batchResp)
	}
}

func TestUniqueRecipientTracking(t *testing.T) {
	server := NewServer()

	// Same contact opening multiple times
	payload := `[
		{"event_id":"evt_open_1","campaign_id":"cmp_unique","contact_id":"ct_1","type":"opened","timestamp":"2026-08-10T06:00:00Z"},
		{"event_id":"evt_open_2","campaign_id":"cmp_unique","contact_id":"ct_1","type":"opened","timestamp":"2026-08-10T07:00:00Z"},
		{"event_id":"evt_open_3","campaign_id":"cmp_unique","contact_id":"ct_2","type":"opened","timestamp":"2026-08-10T08:00:00Z"}
	]`

	req := httptest.NewRequest("POST", "/events", bytes.NewBufferString(payload))
	w := httptest.NewRecorder()
	server.handlePostEvents(w, req)

	statsReq := httptest.NewRequest("GET", "/campaigns/cmp_unique/stats", nil)
	statsReq.SetPathValue("campaignID", "cmp_unique")
	wStats := httptest.NewRecorder()
	server.handleGetStats(wStats, statsReq)

	var stats CampaignStats
	if err := json.Unmarshal(wStats.Body.Bytes(), &stats); err != nil {
		t.Fatalf("failed to decode stats: %v", err)
	}

	if stats.Opened != 3 {
		t.Fatalf("expected 3 total opens, got %d", stats.Opened)
	}
	if stats.UniqueOpens != 2 {
		t.Fatalf("expected 2 unique opens (ct_1 and ct_2), got %d", stats.UniqueOpens)
	}
}

func TestConcurrentIngestion(t *testing.T) {
	server := NewServer()
	var wg sync.WaitGroup

	numGoroutines := 10
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			payload := fmt.Sprintf(`[
				{"event_id":"evt_concurrent_%d","campaign_id":"cmp_concurrent","contact_id":"ct_%d","type":"opened","timestamp":"2026-08-10T06:00:00Z"}
			]`, id, id)

			req := httptest.NewRequest("POST", "/events", bytes.NewBufferString(payload))
			w := httptest.NewRecorder()
			server.handlePostEvents(w, req)
		}(i)
	}

	wg.Wait()

	statsReq := httptest.NewRequest("GET", "/campaigns/cmp_concurrent/stats", nil)
	statsReq.SetPathValue("campaignID", "cmp_concurrent")
	wStats := httptest.NewRecorder()
	server.handleGetStats(wStats, statsReq)

	var stats CampaignStats
	if err := json.Unmarshal(wStats.Body.Bytes(), &stats); err != nil {
		t.Fatalf("failed to decode stats: %v", err)
	}

	if stats.Opened != numGoroutines || stats.UniqueOpens != numGoroutines {
		t.Fatalf("expected %d opened, got %d", numGoroutines, stats.Opened)
	}
}

func TestSeedDataSurvival(t *testing.T) {
	data, err := os.ReadFile("seed/events.json")
	if err != nil {
		t.Skipf("seed file not found, skipping: %v", err)
	}

	server := NewServer()
	req := httptest.NewRequest("POST", "/events", bytes.NewReader(data))
	w := httptest.NewRecorder()
	server.handlePostEvents(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var batchResp BatchResponse
	if err := json.Unmarshal(w.Body.Bytes(), &batchResp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	t.Logf("Seed data summary: received=%d, accepted=%d, duplicates=%d, rejected=%d",
		batchResp.Received, batchResp.Accepted, batchResp.Duplicates, batchResp.Rejected)

	if batchResp.Received == 0 {
		t.Fatalf("expected non-zero received events")
	}
	if batchResp.Accepted == 0 {
		t.Fatalf("expected some events to be accepted")
	}
}