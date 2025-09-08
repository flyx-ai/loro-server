package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
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

type JSONPatch struct {
	Op    string `json:"op"`   // add,remove,replace
	Path  string `json:"path"` // /a/b/c
	Value any    `json:"value,omitempty"`
}

var ErrTestFailed = fmt.Errorf("test operation failed")

func Patch(ctx context.Context, nc *nats.Conn, documentID string, patch []JSONPatch) error {
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("failed to marshal patch: %w", err)
	}

	resp, err := MakeRequest(ctx, nc, "loro.doc.patch."+documentID, patchBytes, time.Second*30)
	if err != nil {
		return fmt.Errorf("failed to patch document %s: %w", documentID, err)
	}

	if resp.StatusCode != 200 {
		var failure failedResponse
		if err := json.Unmarshal(resp.Payload, &failure); err != nil {
			return fmt.Errorf("failed to unmarshal failed response: %w", err)
		}
		if strings.HasSuffix(failure.Message, "value did not match") {
			return fmt.Errorf("patch failed with code %d: %s: %w", resp.StatusCode, failure.Message, ErrTestFailed)
		}
		return fmt.Errorf("patch failed with code %d: %s", resp.StatusCode, failure.Message)
	}

	return nil
}

func Get(ctx context.Context, nc *nats.Conn, documentID string, paths []string) ([]any, error) {
	pathBytes, err := json.Marshal(paths)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal paths: %w", err)
	}

	resp, err := MakeRequest(ctx, nc, "loro.doc.get."+documentID, pathBytes, time.Second*30)
	if err != nil {
		return nil, fmt.Errorf("failed to get document %s: %w", documentID, err)
	}

	if resp.StatusCode != 200 {
		var failure failedResponse
		if err := json.Unmarshal(resp.Payload, &failure); err != nil {
			return nil, fmt.Errorf("failed to unmarshal failed response: %w", err)
		}
		return nil, fmt.Errorf("get failed with code %d: %s", resp.StatusCode, failure.Message)
	}

	var result []any
	if err := json.Unmarshal(resp.Payload, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal get response: %w", err)
	}

	return result, nil
}
