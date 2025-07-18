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
