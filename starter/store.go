package main

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"
)

// CampaignData holds the raw counters and contact sets for a single campaign.
type CampaignData struct {
	Sent                  int
	Delivered             int
	Opened                int
	Clicked               int
	uniqueOpenedContacts  map[string]struct{}
	uniqueClickedContacts map[string]struct{}
	events                []Event // stored for the optional paginated GET /events endpoint
}

func newCampaignData() *CampaignData {
	return &CampaignData{
		uniqueOpenedContacts:  make(map[string]struct{}),
		uniqueClickedContacts: make(map[string]struct{}),
		events:                make([]Event, 0),
	}
}

// Store is a thread-safe, in-memory repository for events and campaign analytics.
type Store struct {
	mu         sync.RWMutex
	seenEvents map[string]bool          // deduplication registry: event_id -> true
	campaigns  map[string]*CampaignData // campaign_id -> CampaignData
}

// NewStore initializes a new in-memory store.
func NewStore() *Store {
	return &Store{
		seenEvents: make(map[string]bool),
		campaigns:  make(map[string]*CampaignData),
	}
}

// IngestBatch processes an array of raw JSON elements resiliently.
// If an individual event is malformed or invalid, only that event is rejected;
// the remainder of the batch continues processing.
func (s *Store) IngestBatch(rawItems []json.RawMessage) BatchResponse {
	resp := BatchResponse{
		Received: len(rawItems),
		Errors:   make([]ItemError, 0),
	}

	for i, rawItem := range rawItems {
		var raw RawEvent
		if err := json.Unmarshal(rawItem, &raw); err != nil {
			resp.Rejected++
			resp.Errors = append(resp.Errors, ItemError{
				Index:  i,
				Reason: fmt.Sprintf("malformed JSON object: %v", err),
			})
			continue
		}

		event, err := s.validateAndNormalize(raw)
		if err != nil {
			resp.Rejected++
			resp.Errors = append(resp.Errors, ItemError{
				Index:   i,
				EventID: raw.EventID,
				Reason:  err.Error(),
			})
			continue
		}

		// Thread-safe ingestion and deduplication
		status := s.recordEvent(event)
		switch status {
		case "accepted":
			resp.Accepted++
		case "duplicate":
			resp.Duplicates++
		}
	}

	return resp
}

// validateAndNormalize enforces schema rules:
// - event_id, campaign_id, contact_id must be non-empty strings
// - type must be one of: sent, delivered, opened, clicked (case-insensitive)
// - timestamp must be a valid RFC3339 UTC time
func (s *Store) validateAndNormalize(raw RawEvent) (Event, error) {
	eventID := strings.TrimSpace(raw.EventID)
	if eventID == "" {
		return Event{}, fmt.Errorf("missing or empty event_id")
	}

	campaignID := strings.TrimSpace(raw.CampaignID)
	if campaignID == "" {
		return Event{}, fmt.Errorf("missing or empty campaign_id")
	}

	contactID := strings.TrimSpace(raw.ContactID)
	if contactID == "" {
		return Event{}, fmt.Errorf("missing or empty contact_id")
	}

	// Normalize event type to lowercase
	eventType := strings.ToLower(strings.TrimSpace(raw.Type))
	switch eventType {
	case "sent", "delivered", "opened", "clicked":
		// valid type
	default:
		return Event{}, fmt.Errorf("invalid event type %q (allowed: sent, delivered, opened, clicked)", raw.Type)
	}

	// Validate timestamp (RFC3339 / ISO 8601)
	parsedTime, err := time.Parse(time.RFC3339, raw.Timestamp)
	if err != nil {
		return Event{}, fmt.Errorf("invalid timestamp %q: must be RFC3339 UTC (e.g. 2026-08-10T09:15:04Z)", raw.Timestamp)
	}

	return Event{
		EventID:    eventID,
		CampaignID: campaignID,
		ContactID:  contactID,
		Type:       eventType,
		Timestamp:  parsedTime.UTC(),
		Metadata:   raw.Metadata,
	}, nil
}

// recordEvent safely applies an event to the store while checking for duplicates.
func (s *Store) recordEvent(ev Event) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Deduplication check: has this event_id been processed before?
	if s.seenEvents[ev.EventID] {
		return "duplicate"
	}
	s.seenEvents[ev.EventID] = true

	// Ensure campaign record exists
	camp, ok := s.campaigns[ev.CampaignID]
	if !ok {
		camp = newCampaignData()
		s.campaigns[ev.CampaignID] = camp
	}

	// Append to event history
	camp.events = append(camp.events, ev)

	// Update raw counts and unique recipient sets
	switch ev.Type {
	case "sent":
		camp.Sent++
	case "delivered":
		camp.Delivered++
	case "opened":
		camp.Opened++
		camp.uniqueOpenedContacts[ev.ContactID] = struct{}{}
	case "clicked":
		camp.Clicked++
		camp.uniqueClickedContacts[ev.ContactID] = struct{}{}
	}

	return "accepted"
}

// GetStats returns aggregated campaign stats and conversion rates.
func (s *Store) GetStats(campaignID string) (*CampaignStats, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	camp, ok := s.campaigns[campaignID]
	if !ok {
		return nil, false
	}

	uniqueOpens := len(camp.uniqueOpenedContacts)
	uniqueClicks := len(camp.uniqueClickedContacts)

	// Calculate conversion rates safely without divide-by-zero panics
	var deliveryRate, openRate, clickToOpenRate float64

	if camp.Sent > 0 {
		deliveryRate = roundFloat(float64(camp.Delivered) / float64(camp.Sent), 4)
	}
	if camp.Delivered > 0 {
		openRate = roundFloat(float64(uniqueOpens) / float64(camp.Delivered), 4)
	}
	if uniqueOpens > 0 {
		clickToOpenRate = roundFloat(float64(uniqueClicks) / float64(uniqueOpens), 4)
	}

	return &CampaignStats{
		CampaignID:   campaignID,
		Sent:         camp.Sent,
		Delivered:    camp.Delivered,
		Opened:       camp.Opened,
		Clicked:      camp.Clicked,
		UniqueOpens:  uniqueOpens,
		UniqueClicks: uniqueClicks,
		Rates: CampaignRates{
			DeliveryRate:    deliveryRate,
			OpenRate:        openRate,
			ClickToOpenRate: clickToOpenRate,
		},
	}, true
}

// GetEvents returns a paginated list of events for the campaign (Optional bonus endpoint).
func (s *Store) GetEvents(campaignID string, limit, offset int) (*CampaignEventsResponse, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	camp, ok := s.campaigns[campaignID]
	if !ok {
		return nil, false
	}

	total := len(camp.events)
	if offset > total {
		offset = total
	}
	end := offset + limit
	if end > total {
		end = total
	}

	paged := camp.events[offset:end]
	eventsCopy := make([]Event, len(paged))
	copy(eventsCopy, paged)

	return &CampaignEventsResponse{
		CampaignID: campaignID,
		Total:      total,
		Limit:      limit,
		Offset:     offset,
		Events:     eventsCopy,
	}, true
}

func roundFloat(val float64, precision int) float64 {
	ratio := math.Pow(10, float64(precision))
	return math.Round(val*ratio) / ratio
}