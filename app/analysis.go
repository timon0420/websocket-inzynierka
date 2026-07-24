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
	Analyze(sessionCode string, jpeg []byte) (AnalysisData, error)
	Ready() bool
}

func (s *httpAnalysisService) Ready() bool {
	return s.url != "" && s.token != ""
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
		client: &http.Client{Timeout: 5 * time.Second},
	}
}

func (s *httpAnalysisService) Analyze(sessionCode string, jpeg []byte) (AnalysisData, error) {
	if s.url == "" {
		return AnalysisData{}, errors.New("ANALYSIS_SERVICE_URL is not configured")
	}
	request, err := http.NewRequest(http.MethodPost, s.url+"/analyze", bytes.NewReader(jpeg))
	if err != nil {
		return AnalysisData{}, err
	}
	request.Header.Set("Content-Type", "image/jpeg")
	request.Header.Set("X-Session-Code", sessionCode)
	request.Header.Set("X-Internal-Token", s.token)
	response, err := s.client.Do(request)
	if err != nil {
		return AnalysisData{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 1024))
		return AnalysisData{}, fmt.Errorf("analysis service returned %d: %s", response.StatusCode, body)
	}
	var result AnalysisData
	if err := json.NewDecoder(io.LimitReader(response.Body, 64*1024)).Decode(&result); err != nil {
		return AnalysisData{}, err
	}
	if result.Detected && !validAngles(result.Angles) {
		return AnalysisData{}, errors.New("analysis service returned invalid angles")
	}
	return result, nil
}
