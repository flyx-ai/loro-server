package main

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/flyx-ai/loro-server/lorogo/web"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	nc, err := nats.Connect(nats.DefaultURL)
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
			http.Error(w, err.Error(), http.StatusInternalServerError)
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
	slog.Info("Starting Loro Server", "port", "8080")
	err = http.ListenAndServe("0.0.0.0:8080", mux)
	if err != nil {
		panic(err)
	}
}
