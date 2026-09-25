package ingest_test

import (
	"strings"
	"testing"

	"webhook-relay/internal/ingest"
)

func TestValidateEventID(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want bool
	}{
		{"uuid lower", "8f1e2b3c-4d5e-4f60-8192-a3b4c5d6e7f8", true},
		{"uuid upper", "8F1E2B3C-4D5E-4F60-8192-A3B4C5D6E7F8", true},
		{"slug short", "abc", true},
		{"slug one char", "a", true},
		{"slug with dash", "order-123", true},
		{"slug numeric", "123", true},
		{"slug double dash", "a--b", true}, // internal "--" is legal per the slug pattern
		{"max length", strings.Repeat("a", 128), true},
		{"empty", "", false},
		{"too long", strings.Repeat("a", 129), false},
		{"leading dash", "-leading", false},
		{"trailing dash (allowed by slug pattern)", "trailing-", true},
		{"uppercase slug", "UPPER", false},
		{"mixed-case slug", "Upper", false},
		{"space", "has space", false},
		{"bad character", "8f1e2b3c4d5e!", false},
		{"hex without dashes is a valid slug", "8f1e2b3c4d5e", true},
		{"dotted id", "payment.paid", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ingest.ValidateEventID(tc.in)
			if got := err == nil; got != tc.want {
				t.Errorf("ValidateEventID(%q) ok=%v, want %v (err=%v)", tc.in, got, tc.want, err)
			}
		})
	}
}

func TestValidateEventType(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want bool
	}{
		{"simple", "payment.paid", true},
		{"two segments", "a.b", true},
		{"three segments", "a.b.c", true},
		{"digit segment", "order.created.2", true},
		{"empty", "", false},
		{"single segment", "a", false},
		{"leading dot", ".a", false},
		{"trailing dot", "a.", false},
		{"empty middle segment", "a..b", false},
		{"uppercase", "A.b", false},
		{"underscore", "a_b.c", false},
		{"space", "a b.c", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ingest.ValidateEventType(tc.in)
			if got := err == nil; got != tc.want {
				t.Errorf("ValidateEventType(%q) ok=%v, want %v (err=%v)", tc.in, got, tc.want, err)
			}
		})
	}
}

func TestValidateURL(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want bool
	}{
		{"https", "https://x.example/hooks", true},
		{"http with port", "http://127.0.0.1:8080/hook", true},
		{"host only", "https://x.example", true},
		{"empty", "", false},
		{"ftp scheme", "ftp://x.example/hook", false},
		{"javascript scheme", "javascript:alert(1)", false},
		{"garbage", "not a url", false},
		{"no host", "https://", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ingest.ValidateURL(tc.in)
			if got := err == nil; got != tc.want {
				t.Errorf("ValidateURL(%q) ok=%v, want %v (err=%v)", tc.in, got, tc.want, err)
			}
		})
	}
}
