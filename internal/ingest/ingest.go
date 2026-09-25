// Package ingest validates producer events and subscription requests against
// the rules in PROJECT_BRIEF.md §8.4.
package ingest

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
)

var (
	// slugRe: lowercase slug, 1–128 chars.
	slugRe = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,127}$`)
	// uuidRe: canonical UUID, case-insensitive.
	uuidRe = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	// typeRe: dotted lowercase segment, ≥ 2 segments (e.g. "payment.paid").
	typeRe = regexp.MustCompile(`^[a-z0-9]+(\.[a-z0-9]+)+$`)
)

// ValidateEventID enforces: required, ≤ 128 chars, valid UUID or slug.
func ValidateEventID(s string) error {
	if s == "" {
		return errors.New("event id is required")
	}
	if len(s) > 128 {
		return fmt.Errorf("event id too long (%d > 128)", len(s))
	}
	if uuidRe.MatchString(s) || slugRe.MatchString(s) {
		return nil
	}
	return fmt.Errorf("event id %q must be a UUID or slug [a-z0-9-]", s)
}

// ValidateEventType enforces ^[a-z0-9]+(\.[a-z0-9]+)+$.
func ValidateEventType(s string) error {
	if s == "" {
		return errors.New("event type is required")
	}
	if !typeRe.MatchString(s) {
		return fmt.Errorf("event type %q must match ^[a-z0-9]+(\\.[a-z0-9]+)+$", s)
	}
	return nil
}

// ValidateURL enforces a parseable http(s) URL with a host.
func ValidateURL(s string) error {
	if s == "" {
		return errors.New("url is required")
	}
	u, err := url.Parse(s)
	if err != nil {
		return fmt.Errorf("url: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("url scheme %q must be http or https", u.Scheme)
	}
	if u.Host == "" {
		return errors.New("url must include a host")
	}
	return nil
}
