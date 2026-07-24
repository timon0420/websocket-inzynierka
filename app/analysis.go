package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

type analysisService interface {
	Activate(code, token string, expiresAt time.Time) error
	Deactivate(code string) error
	Ready() bool
}

type httpAnalysisService struct {
	url    string
	token  string
	client *http.Client
}

func newHTTPAnalysisService(url, token string) analysisService {
	if url == "" {
		if hostport := strings.TrimSpace(os.Getenv("ANALYSIS_SERVICE_HOSTPORT")); hostport != "" {
			url = "http://" + hostport
		}
	}
	return &httpAnalysisService{
		url: strings.TrimRight(url, "/"), token: token,
		client: &http.Client{Timeout: 10 * time.Second},
	}
}

func (s *httpAnalysisService) Ready() bool { return s.url != "" && s.token != "" }

func (s *httpAnalysisService) Activate(code, token string, expiresAt time.Time) error {
	payload, _ := json.Marshal(map[string]string{
		"code": code, "token": token, "expiresAt": expiresAt.UTC().Format(time.RFC3339),
	})
	return s.request(http.MethodPost, "/internal/sessions", payload)
}

func (s *httpAnalysisService) Deactivate(code string) error {
	return s.request(http.MethodDelete, "/internal/sessions/"+code, nil)
}

func (s *httpAnalysisService) request(method, path string, body []byte) error {
	if !s.Ready() {
		return errors.New("analysis service is not configured")
	}
	request, err := http.NewRequest(method, s.url+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("X-Internal-Token", s.token)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := s.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(response.Body, 1024))
		return fmt.Errorf("analysis service returned %d: %s", response.StatusCode, data)
	}
	return nil
}
