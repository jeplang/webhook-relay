// Package logging builds the application's structured logger (slog, JSON).
package logging

import (
	"fmt"
	"io"
	"log/slog"
	"strings"
)

// New builds a JSON slog.Logger at the given level:
// "debug", "info", "warn", "error" (case-insensitive; empty → info).
func New(level string, w io.Writer) (*slog.Logger, error) {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "info", "":
		lvl = slog.LevelInfo
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		return nil, fmt.Errorf("logging: unknown level %q", level)
	}
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: lvl})), nil
}
