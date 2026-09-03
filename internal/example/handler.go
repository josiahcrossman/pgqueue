// Package example provides a sample handler to show how job types are wired.
package example

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
)

// EmailJobType is the registry key for the sample handler.
const EmailJobType = "send_email"

// EmailPayload is the JSON shape the example handler expects in a job payload.
type EmailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

// NewEmailHandler returns a handler that "sends" an email by logging it. It is
// deliberately trivial — the point is to demonstrate decoding a payload and
// returning an error for bad input.
func NewEmailHandler(log *slog.Logger) func(ctx context.Context, payload []byte) error {
	return func(ctx context.Context, payload []byte) error {
		var p EmailPayload
		if err := json.Unmarshal(payload, &p); err != nil {
			return fmt.Errorf("decode email payload: %w", err)
		}
		if p.To == "" {
			return fmt.Errorf("email payload missing 'to'")
		}
		log.InfoContext(ctx, "sending email", "to", p.To, "subject", p.Subject)
		return nil
	}
}
