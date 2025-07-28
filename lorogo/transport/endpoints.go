package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
)

type failedResponse struct {
	Message string `json:"message"`
}

func Purge(ctx context.Context, nc *nats.Conn, documentID string) error {
	slog.Info("purging document", "documentID", documentID)

	resp, err := MakeRequest(ctx, nc, "loro.doc.purge."+documentID, nil, time.Second*60)
	if err != nil {
		return fmt.Errorf("failed to purge document %s: %w", documentID, err)
	}

	if resp.StatusCode != 200 {
		var failure failedResponse
		if err := json.Unmarshal(resp.Payload, &failure); err != nil {
			return fmt.Errorf("failed to unmarshal failed response: %w", err)
		}
		return fmt.Errorf("purge failed with code %d: %s", resp.StatusCode, failure.Message)
	}

	return nil
}
