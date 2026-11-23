package splunkhec

import (
	"net/http"
	"testing"
)

func acceptAll(string) error { return nil }

func TestAuth_NoCheck_AcceptsMissingHeader(t *testing.T) {
	req, _ := http.NewRequest(http.MethodPost, "/", nil)
	if err := (AuthConfig{}).Authorize(req); err != nil {
		t.Fatalf("Authorize: %v", err)
	}
}

func TestAuth_NoHeader_ReturnsMissing(t *testing.T) {
	req, _ := http.NewRequest(http.MethodPost, "/", nil)
	if err := (AuthConfig{Check: acceptAll}).Authorize(req); err != ErrMissingToken {
		t.Fatalf("err = %v, want ErrMissingToken", err)
	}
}

func TestAuth_NonSplunkScheme_ReturnsMissing(t *testing.T) {
	req, _ := http.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("Authorization", "Bearer token")
	if err := (AuthConfig{Check: acceptAll}).Authorize(req); err != ErrMissingToken {
		t.Fatalf("err = %v, want ErrMissingToken", err)
	}
}

func TestAuth_CheckSeesTheToken(t *testing.T) {
	req, _ := http.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("Authorization", "Splunk  token ")
	var got string
	check := func(token string) error {
		got = token
		return ErrInvalidToken
	}
	if err := (AuthConfig{Check: check}).Authorize(req); err != ErrInvalidToken {
		t.Fatalf("err = %v, want ErrInvalidToken", err)
	}
	if got != "token" {
		t.Fatalf("token = %q, want %q", got, "token")
	}
}
