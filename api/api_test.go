package api_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/TFMV/parity/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewServer ensures that creating a new server does not return a nil instance
func TestNewServer(t *testing.T) {
	opts := api.ServerOptions{
		Port:    "3000",
		Prefork: false,
	}
	s := api.NewServer(opts)
	require.NotNil(t, s, "Expected a non-nil server instance")
}

// TestHealthEndpoint checks if the /health endpoint returns "OK"
func TestHealthEndpoint(t *testing.T) {
	opts := api.ServerOptions{
		Port:    "3000",
		Prefork: false,
	}
	s := api.NewServer(opts) // Use default port "3000" if empty
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	resp, err := s.GetApp().Test(req)
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Read the response body correctly
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, "OK", string(body))
}

// versionResponse is used for JSON unmarshalling in the /version endpoint test
type versionResponse struct {
	Service string `json:"service"`
	Version string `json:"version"`
	Build   string `json:"build"`
	Time    string `json:"time"`
}

// TestVersionEndpoint checks if the /version endpoint returns the correct JSON structure
func TestVersionEndpoint(t *testing.T) {
	opts := api.ServerOptions{
		Port:    "3000",
		Prefork: false,
	}
	s := api.NewServer(opts)
	req := httptest.NewRequest(http.MethodGet, "/version", nil)
	resp, err := s.GetApp().Test(req)
	require.NoError(t, err, "Unexpected error when making request to /version")

	assert.Equal(t, http.StatusOK, resp.StatusCode, "Expected status code 200 for /version endpoint")

	// Read and parse the response body
	defer resp.Body.Close()
	var v versionResponse
	err = json.NewDecoder(resp.Body).Decode(&v)
	require.NoError(t, err, "Failed to decode JSON response")

	// Basic checks — adjust as needed to match your version package data
	assert.Equal(t, "Parity API", v.Service, "Expected the service name to be 'Parity API'")
	assert.NotEmpty(t, v.Version, "Expected a non-empty version")
	assert.NotEmpty(t, v.Build, "Expected a non-empty build date")
	assert.NotEmpty(t, v.Time, "Expected a non-empty timestamp")
}

// TestShutdown verifies that calling Shutdown on the server does not return an error
func TestShutdown(t *testing.T) {
	opts := api.ServerOptions{
		Port:    "3000",
		Prefork: false,
	}
	s := api.NewServer(opts)
	err := s.Shutdown(context.Background())
	assert.NoError(t, err, "Expected no error calling Shutdown on server")
}
