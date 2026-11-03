package main

import "time"

// RawEvent represents an incoming event before validation.
// Fields are strings or raw values so malformed data doesn't fail the initial parse.
type RawEvent struct {
	EventID    string            `json:"event_id"`
	CampaignID string            `json:"campaign_id"`
	ContactID  string            `json:"contact_id"`
	Type       string            `json:"type"`
	Timestamp  string            `json:"timestamp"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

// Event is a validated, normalized webhook event.
type Event struct {
	EventID    string            `json:"event_id"`
	CampaignID string            `json:"campaign_id"`
	ContactID  string            `json:"contact_id"`
	Type       string            `json:"type"` // sent | delivered | opened | clicked
	Timestamp  time.Time         `json:"timestamp"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

// ItemError captures why an individual event in a batch was rejected.
type ItemError struct {
	Index   int    `json:"index"`
	EventID string `json:"event_id,omitempty"`
	Reason  string `json:"reason"`
}

// BatchResponse is returned by POST /events.
// Provides visibility into accepted, duplicated, and rejected records.
type BatchResponse struct {
	Received   int         `json:"received"`
	Accepted   int         `json:"accepted"`
	Duplicates int         `json:"duplicates"`
	Rejected   int         `json:"rejected"`
	Errors     []ItemError `json:"errors,omitempty"`
}

// CampaignRates contains calculated marketing conversion rates.
type CampaignRates struct {
	DeliveryRate     float64 `json:"delivery_rate"`       // delivered / sent
	OpenRate         float64 `json:"open_rate"`           // unique_opens / delivered
	ClickToOpenRate  float64 `json:"click_to_open_rate"`  // unique_clicks / unique_opens
}

// CampaignStats is the response for GET /campaigns/{campaign_id}/stats.
type CampaignStats struct {
	CampaignID   string        `json:"campaign_id"`
	Sent         int           `json:"sent"`
	Delivered    int           `json:"delivered"`
	Opened       int           `json:"opened"`
	Clicked      int           `json:"clicked"`
	UniqueOpens  int           `json:"unique_opens"`
	UniqueClicks int           `json:"unique_clicks"`
	Rates        CampaignRates `json:"rates"`
}

// CampaignEventsResponse represents a paginated list of campaign events.
type CampaignEventsResponse struct {
	CampaignID string  `json:"campaign_id"`
	Total      int     `json:"total"`
	Limit      int     `json:"limit"`
	Offset     int     `json:"offset"`
	Events     []Event `json:"events"`
}