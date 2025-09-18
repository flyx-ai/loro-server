package web

import (
	"context"
	"fmt"
	"net/http"

	"github.com/flyx-ai/loro-server/lorogo/transport"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func CreateDocumentHandler(documentID string, nc *nats.Conn, documentStatusKV jetstream.KeyValue, w http.ResponseWriter, r *http.Request) error {
	err := transport.PingAndInitDoc(context.Background(), documentID, nc, documentStatusKV)
	if err != nil {
		return fmt.Errorf("failed to ping and initialize document: %w", err)
	}
	return nil
}

func PurgeHandler(documentID string, nc *nats.Conn, w http.ResponseWriter, r *http.Request) error {
	err := transport.Purge(context.Background(), nc, documentID)
	if err != nil {
		return fmt.Errorf("failed to purge document %s: %w", documentID, err)
	}
	return nil
}

func ForkHandler(documentID string, newDocumentID string, nc *nats.Conn, documentStatusKV jetstream.KeyValue, w http.ResponseWriter, r *http.Request) error {
	err := transport.Fork(context.Background(), nc, documentID, newDocumentID, documentStatusKV)
	if err != nil {
		return fmt.Errorf("failed to fork document %s to %s: %w", documentID, newDocumentID, err)
	}
	return nil
}
