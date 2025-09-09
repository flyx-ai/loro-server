package web

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/flyx-ai/loro-server/lorogo/transport"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func subscribeCRDT(ctx context.Context, nc *nats.Conn, documentID string, clientID string) (chan []byte, error) {
	resChan := make(chan []byte, 256)
	subs, err := transport.Subscribe(ctx, nc, fmt.Sprintf("crdt.%s.update", documentID), "", func(cr transport.CoreResponse) *transport.CoreResponse {
		if cr.Header.Get("X-Client-ID") == clientID {
			return nil
		}
		dat := []byte{byte(wsDocumentResponseTypeCRDT)}
		dat = append(dat, cr.Payload...)
		resChan <- dat
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to CRDT updates: %w", err)
	}
	context.AfterFunc(ctx, func() {
		if err := subs.Unsubscribe(); err != nil {
			slog.Error("Failed to unsubscribe from CRDT updates", "error", err)
		}
	})

	return resChan, nil
}

type wsDocumentBinaryRequestType uint8

const (
	wsDocumentRequestTypeInvalid wsDocumentBinaryRequestType = iota
	wsDocumentRequestTypeCRDT
	wsDocumentRequestTypeSync
)

type wsDocumentBinaryResponseType uint8

const (
	wsDocumentResponseTypeInvalid wsDocumentBinaryResponseType = iota
	wsDocumentResponseTypeCRDT
	wsDocumentResponseTypeVV
)

func documentInputBinaryHandler(
	ctx context.Context,
	nc *nats.Conn,
	js jetstream.JetStream,
	documentStatusKV jetstream.KeyValue,
	data []byte,
	documentID string,
	clientID string,
	binaryChan chan []byte,
) error {
	requestType := data[0]
	data = data[1:]

	switch wsDocumentBinaryRequestType(requestType) {
	case wsDocumentRequestTypeCRDT:
		wg := sync.WaitGroup{}
		wg.Add(2)

		var innerErr error
		go func() {
			defer wg.Done()

			err := transport.SendLog(
				ctx,
				documentID,
				nc,
				js,
				documentStatusKV,
				bytes.NewReader(data),
			)
			if err != nil {
				innerErr = fmt.Errorf("failed to send log: %w", err)
				return
			}
		}()

		go func() {
			defer wg.Done()

			// TODO: batch this
			err := nc.PublishMsg(&nats.Msg{
				Subject: fmt.Sprintf("crdt.%s.update", documentID),
				Data:    data,
				Header:  nats.Header{"X-Client-ID": []string{clientID}},
			})
			if err != nil {
				innerErr = fmt.Errorf("failed to publish CRDT: %w", err)
				return
			}
		}()

		wg.Wait()
		if innerErr != nil {
			return fmt.Errorf("failed to handle CRDT request: %w", innerErr)
		}
	case wsDocumentRequestTypeSync:
		wg := sync.WaitGroup{}
		wg.Add(2)

		var innerErr error
		go func() {
			defer wg.Done()

			resp, err := transport.MakeRequest(ctx, nc, "loro.doc.sync_down."+documentID, data, nil, 60*time.Second)
			if err != nil {
				innerErr = fmt.Errorf("failed to sync down: %w", err)
				return
			}
			if resp.StatusCode != http.StatusOK {
				innerErr = fmt.Errorf("sync down failed with status code: %d", resp.StatusCode)
				return
			}
			// if nothing to sync down
			if len(resp.Payload) == 0 {
				return
			}
			binaryChan <- append([]byte{byte(wsDocumentResponseTypeCRDT)}, resp.Payload...)
		}()

		go func() {
			defer wg.Done()

			resp, err := transport.MakeRequest(ctx, nc, "loro.doc.version_vector."+documentID, nil, nil, 60*time.Second)
			if err != nil {
				innerErr = fmt.Errorf("failed to get version vector: %w", err)
				return
			}
			if resp.StatusCode != http.StatusOK {
				innerErr = fmt.Errorf("version vector request failed with status code: %d", resp.StatusCode)
				return
			}
			binaryChan <- append([]byte{byte(wsDocumentResponseTypeVV)}, resp.Payload...)
		}()

		wg.Wait()
		if innerErr != nil {
			return fmt.Errorf("failed to handle sync request: %w", innerErr)
		}
	case wsDocumentRequestTypeInvalid:
		fallthrough
	default:
		return fmt.Errorf("invalid request type: %d", data[0])
	}

	return nil
}

//nolint:gocyclo
func DocumentListenHandler(
	ctx context.Context,
	nc *nats.Conn,
	js jetstream.JetStream,
	documentStatusKV jetstream.KeyValue,
	documentID string,
	w http.ResponseWriter,
	r *http.Request,
) error {
	ctx, cancel := context.WithCancelCause(ctx)
	clientID := gonanoid.Must()
	defer cancel(errors.New("cancel websocket closed"))

	//nolint:exhaustruct
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: true,
	})
	if err != nil {
		return fmt.Errorf("failed to accept websocket connection: %w", err)
	}

	conn.SetReadLimit(-1)

	var innerErr error
	defer func() {
		if tempErr := conn.CloseNow(); tempErr != nil {
			innerErr = fmt.Errorf("failed to close websocket connection: %w", tempErr)
		}
	}()

	binaryChan, err := subscribeCRDT(ctx, nc, documentID, clientID)
	if err != nil {
		return fmt.Errorf("failed to subscribe to CRDT: %w", err)
	}

	errCh := make(chan error, 1)

	go func() {
		defer cancel(errors.New("cancel websocket read closed"))
		for {
			msgType, data, err := conn.Read(ctx)
			if err != nil {
				errCh <- fmt.Errorf("failed to read websocket message: %w", err)
				return
			}

			go func() {
				switch msgType {
				case websocket.MessageBinary:
					err := documentInputBinaryHandler(ctx, nc, js, documentStatusKV, data, documentID, clientID, binaryChan)
					if err != nil {
						errCh <- fmt.Errorf("failed to handle binary message: %w", err)
						return
					}
				case websocket.MessageText:
					errCh <- fmt.Errorf("text messages are not supported, received: %s", string(data))
				}
			}()
		}
	}()

	go func() {
		resp, err := transport.MakeRequest(ctx, nc, "loro.doc.version_vector."+documentID, nil, nil, 60*time.Second)
		if err != nil {
			slog.Error("Failed to get version vector", "error", err)
			return
		}
		if resp.StatusCode != http.StatusOK {
			slog.Error("Version vector request failed", "status_code", resp.StatusCode)
			return
		}
		binaryChan <- append([]byte{byte(wsDocumentResponseTypeVV)}, resp.Payload...)
	}()

	for {
		select {
		case res := <-binaryChan:
			err := conn.Write(ctx, websocket.MessageBinary, res)
			if err != nil {
				return fmt.Errorf("failed to write binary response: %w", err)
			}
		case <-ctx.Done():
			if tempErr := ctx.Err(); tempErr != nil {
				return fmt.Errorf("context done: %w", tempErr)
			}
			return innerErr
		case err := <-errCh:
			var closeErr websocket.CloseError
			if errors.Is(err, io.EOF) {
				return innerErr
			} else if errors.As(err, &closeErr) {
				if closeErr.Code == websocket.StatusNormalClosure {
					return innerErr
				}

				attrs := []any{}
				attrs = append(attrs, "code", closeErr.Code.String())
				if closeErr.Reason != "" {
					attrs = append(attrs, "reason", closeErr.Reason)
				}
				slog.Info("Websocket closed", attrs...)
				return innerErr
			} else {
				return fmt.Errorf("websocket error: %w", err)
			}
		}
	}
}
