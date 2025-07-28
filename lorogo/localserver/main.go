package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"

	"github.com/flyx-ai/loro-server/lorogo/transport"
	"github.com/flyx-ai/loro-server/lorogo/web"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}
	nc, err := nats.Connect(natsURL)
	if err != nil {
		panic(err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		panic(err)
	}
	documentStatusKV, err := js.CreateOrUpdateKeyValue(context.Background(), jetstream.KeyValueConfig{
		Bucket:      "loro-document-status",
		Description: "Loro Document Status",
	})
	if err != nil {
		panic(err)
	}
	jsonUpdateStream, err := js.CreateOrUpdateStream(context.Background(), jetstream.StreamConfig{
		Name:        "loro-json-update",
		Description: "Stream for storing JSON updates for Loro documents.",
		Subjects:    []string{"loro.json_update.>"},
		Retention:   jetstream.WorkQueuePolicy,
	})
	if err != nil {
		panic(err)
	}
	jsonUpdateChan, err := transport.SubscribeJSONUpdate(context.Background(), js, jsonUpdateStream)
	if err != nil {
		panic(err)
	}
	go func() {
		for update := range jsonUpdateChan {
			slog.Info("Received JSON update", "documentID", update.DocumentID, "updateOps", len(update.UpdateOps))
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/document/{documentID}/ws", func(w http.ResponseWriter, r *http.Request) {
		documentID := r.PathValue("documentID")
		if documentID == "" {
			http.Error(w, "documentID is required", http.StatusBadRequest)
			return
		}

		err := web.TableListenHandler(r.Context(), nc, js, documentStatusKV, documentID, w, r)
		if err != nil {
			slog.Error("Failed to handle WebSocket connection", "documentID", documentID, "error", err)
			return
		}
	})
	mux.HandleFunc("/api/v1/document/{documentID}", func(w http.ResponseWriter, r *http.Request) {
		documentID := r.PathValue("documentID")
		if documentID == "" {
			http.Error(w, "documentID is required", http.StatusBadRequest)
			return
		}

		if r.Method == http.MethodPost {
			err := web.CreateDocumentHandler(documentID, nc, documentStatusKV, w, r)
			if err != nil {
				slog.Error("Failed to create document", "documentID", documentID, "error", err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
	})
	mux.HandleFunc("/api/v1/document/{documentID}/purge", func(w http.ResponseWriter, r *http.Request) {
		documentID := r.PathValue("documentID")
		if documentID == "" {
			http.Error(w, "documentID is required", http.StatusBadRequest)
			return
		}

		if r.Method == http.MethodPost {
			err := web.PurgeHandler(documentID, nc, w, r)
			if err != nil {
				slog.Error("Failed to purge document", "documentID", documentID, "error", err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
	})

	slog.Info("Starting Loro Server", "port", "8080")
	err = http.ListenAndServe("0.0.0.0:8080", mux)
	if err != nil {
		panic(err)
	}
}
