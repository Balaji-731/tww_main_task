package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
)

// Server holds the dependencies for the HTTP handlers.
type Server struct {
	store *Store
}

func NewServer() *Server {
	return &Server{
		store: NewStore(),
	}
}

func main() {
	server := NewServer()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /events", server.handlePostEvents)
	mux.HandleFunc("GET /campaigns/{campaignID}/stats", server.handleGetStats)
	mux.HandleFunc("GET /campaigns/{campaignID}/events", server.handleGetEvents)

	addr := ":8080"
	fmt.Printf("Relay Campaign Events service listening on %s\n", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}

// handlePostEvents ingests a JSON array of events from webhook providers.
func (s *Server) handlePostEvents(w http.ResponseWriter, r *http.Request) {
	// Guard against oversized payloads (10 MB limit)
	r.Body = http.MaxBytesReader(w, r.Body, 10<<20)

	var rawItems []json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&rawItems); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": fmt.Sprintf("invalid request payload: expected a JSON array of events: %v", err),
		})
		return
	}

	// Ingest items individually so bad items do not poison the whole batch
	batchResp := s.store.IngestBatch(rawItems)

	writeJSON(w, http.StatusOK, batchResp)
}

// handleGetStats returns aggregated stats and conversion rates for one campaign.
func (s *Server) handleGetStats(w http.ResponseWriter, r *http.Request) {
	campaignID := r.PathValue("campaignID")
	if campaignID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "campaignID is required"})
		return
	}

	stats, found := s.store.GetStats(campaignID)
	if !found {
		writeJSON(w, http.StatusNotFound, map[string]string{
			"error": fmt.Sprintf("campaign %q not found", campaignID),
		})
		return
	}

	writeJSON(w, http.StatusOK, stats)
}

// handleGetEvents returns a paginated list of events for one campaign (Optional bonus endpoint).
func (s *Server) handleGetEvents(w http.ResponseWriter, r *http.Request) {
	campaignID := r.PathValue("campaignID")
	if campaignID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "campaignID is required"})
		return
	}

	limit := 50
	offset := 0

	query := r.URL.Query()
	if l := query.Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			if parsed > 100 {
				parsed = 100
			}
			limit = parsed
		}
	}
	if o := query.Get("offset"); o != "" {
		if parsed, err := strconv.Atoi(o); err == nil && parsed >= 0 {
			offset = parsed
		}
	}

	eventsResp, found := s.store.GetEvents(campaignID, limit, offset)
	if !found {
		writeJSON(w, http.StatusNotFound, map[string]string{
			"error": fmt.Sprintf("campaign %q not found", campaignID),
		})
		return
	}

	writeJSON(w, http.StatusOK, eventsResp)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("writeJSON error: %v", err)
	}
}