package splunkhec

import (
	"net/http"
	"testing"
)

func TestAuth_NoHeader_ReturnsMissing(t *testing.T) {
	req, _ := http.NewRequest(http.MethodPost, "/", nil)
	if err := (AuthConfig{Enabled: true}).Authorize(req); err != ErrMissingToken {
		t.Fatalf("err = %v, want ErrMissingToken", err)
	}
}

func TestAuth_NonSplunkScheme_ReturnsMissing(t *testing.T) {
	req, _ := http.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("Authorization", "Bearer token")
	if err := (AuthConfig{Enabled: true}).Authorize(req); err != ErrMissingToken {
		t.Fatalf("err = %v, want ErrMissingToken", err)
	}
}

func TestAuth_AnyToken_Accepts(t *testing.T) {
	req, _ := http.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("Authorization", "Splunk token")
	if err := (AuthConfig{Enabled: true}).Authorize(req); err != nil {
		t.Fatalf("Authorize: %v", err)
	}
}
