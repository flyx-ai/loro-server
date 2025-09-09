package web

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"
	"github.com/flyx-ai/loro-server/lorogo/transport"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func subscribeAwareness(ctx context.Context, nc *nats.Conn, awarenessID string, clientID string) (chan []byte, error) {
	resChan := make(chan []byte, 256)
	subs, err := transport.Subscribe(
		ctx,
		nc,
		fmt.Sprintf("awareness.%s.update", awarenessID),
		"",
		func(cr transport.CoreResponse) *transport.CoreResponse {
			if cr.Header.Get("X-Client-ID") == clientID {
				return nil
			}
			dat := []byte{byte(wsAwarenessResponseTypeUpdate)}
			dat = append(dat, cr.Payload...)
			resChan <- dat
			return nil
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to awareness updates: %w", err)
	}
	context.AfterFunc(ctx, func() {
		if err := subs.Unsubscribe(); err != nil {
			slog.Error("Failed to unsubscribe from awareness updates", "error", err)
		}
	})

	return resChan, nil
}

type wsAwarenessBinaryRequestType uint8

const (
	wsAwarenessRequestTypeInvalid wsAwarenessBinaryRequestType = iota
	wsAwarenessRequestTypeUpdate
	wsAwarenessRequestTypeSync
)

type wsAwarenessBinaryResponseType uint8

const (
	wsAwarenessResponseTypeInvalid wsAwarenessBinaryResponseType = iota
	wsAwarenessResponseTypeUpdate
	wsAwarenessResponseTypeSync
)

func awarenessInputBinaryHandler(
	nc *nats.Conn,
	data []byte,
	awarenessID string,
	clientID string,
	syncChan chan []byte,
) error {
	requestType := data[0]
	data = data[1:]

	switch wsAwarenessBinaryRequestType(requestType) {
	case wsAwarenessRequestTypeUpdate:
		err := nc.PublishMsg(&nats.Msg{
			Subject: fmt.Sprintf("awareness.%s.update", awarenessID),
			Data:    data,
			Header:  nats.Header{"X-Client-ID": []string{clientID}},
		})
		if err != nil {
			return fmt.Errorf("failed to publish awareness update: %w", err)
		}
	case wsAwarenessRequestTypeSync:
		syncChan <- data
	case wsAwarenessRequestTypeInvalid:
		fallthrough
	default:
		return fmt.Errorf("invalid request type: %d", data[0])
	}

	return nil
}

//nolint:gocyclo
func AwarenessListenHandler(
	ctx context.Context,
	nc *nats.Conn,
	awarenessStatusKV jetstream.KeyValue,
	awarenessID string,
	w http.ResponseWriter,
	r *http.Request,
) error {
	slog.Info("New awareness websocket connection", "awarenessID", awarenessID, "remoteAddr", r.RemoteAddr)

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

	binaryChan, err := subscribeAwareness(ctx, nc, awarenessID, clientID)
	if err != nil {
		return fmt.Errorf("failed to subscribe to awareness: %w", err)
	}

	errCh := make(chan error, 1)
	syncChan := make(chan []byte, 64)

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
					err := awarenessInputBinaryHandler(nc, data, awarenessID, clientID, syncChan)
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

	defer func() {
		ctx = context.Background()
		for {
			awarenessStatus, err := awarenessStatusKV.Get(ctx, awarenessID)
			if err != nil {
				if errors.Is(err, jetstream.ErrKeyNotFound) {
					slog.Error("Awareness status not found on decrement", "awarenessID", awarenessID)
				} else {
					slog.Error("Failed to get awareness status on decrement", "awarenessID", awarenessID, "error", err)
				}
				return
			}
			subs, _ := binary.Uvarint(awarenessStatus.Value())
			newSub := subs - 1
			if newSub > 0 {
				buf := []byte{}
				buf = binary.AppendUvarint(buf, newSub)
				_, err = awarenessStatusKV.Update(ctx, awarenessID, buf, awarenessStatus.Revision())
				if err != nil {
					if strings.Contains(err.Error(), "wrong last sequence") {
						continue
					} else {
						slog.Error("Failed to update awareness status on decrement", "awarenessID", awarenessID, "error", err)
					}
					return
				}
			} else {
				err = awarenessStatusKV.Delete(ctx, awarenessID, jetstream.LastRevision(awarenessStatus.Revision()))
				if err != nil {
					if errors.Is(err, jetstream.ErrKeyNotFound) {
						slog.Error("Awareness status not found on decrement delete", "awarenessID", awarenessID)
					}
					if strings.Contains(err.Error(), "wrong last sequence") {
						continue
					} else {
						slog.Error("Failed to delete awareness status on decrement", "awarenessID", awarenessID, "error", err)
					}
					return
				}
			}
			return
		}
	}()

	go func() {
		initAwareness := true
		for {
			awarenessStatus, err := awarenessStatusKV.Get(ctx, awarenessID)
			if err != nil {
				if errors.Is(err, jetstream.ErrKeyNotFound) {
					buf := []byte{}
					buf = binary.AppendUvarint(buf, 1)
					_, err := awarenessStatusKV.Create(ctx, awarenessID, buf)
					if err != nil {
						if errors.Is(err, jetstream.ErrKeyExists) {
							continue
						}
					}
					initAwareness = false
					break
				} else {
					errCh <- fmt.Errorf("failed to get awareness status: %w", err)
					return
				}
			}
			subs, _ := binary.Uvarint(awarenessStatus.Value())
			newSub := subs + 1
			buf := []byte{}
			buf = binary.AppendUvarint(buf, newSub)
			_, err = awarenessStatusKV.Update(ctx, awarenessID, buf, awarenessStatus.Revision())
			if err != nil {
				if strings.Contains(err.Error(), "wrong last sequence") {
					continue
				} else {
					errCh <- fmt.Errorf("failed to update awareness status: %w", err)
					return
				}
			}
			if newSub == 1 {
				initAwareness = false
			}
			break
		}

		awarenessSubj := fmt.Sprintf("awareness.%s.sync", awarenessID)
		if initAwareness {
			for retryCount := range 3 {
				resp, err := transport.MakeRequest(ctx, nc, awarenessSubj, []byte(clientID), nil, 5*time.Second)
				if err != nil {
					if errors.Is(err, transport.ErrNoResponders) {
						slog.Info("No responders for full awareness sync, assuming no clients connected", "awarenessID", awarenessID)
						break
					}

					slog.Error("Failed to get full awareness", "error", err, "attempt", retryCount+1)
					continue
				}
				if resp.StatusCode != http.StatusOK {
					slog.Error("Awareness request failed", "status_code", resp.StatusCode, "attempt", retryCount+1)
					continue
				}
				binaryChan <- append([]byte{byte(wsAwarenessResponseTypeUpdate)}, resp.Payload...)
				break
			}
		}

		subscription, err := transport.Subscribe(
			ctx,
			nc,
			awarenessSubj,
			"awareness-"+awarenessID,
			func(cr transport.CoreResponse) *transport.CoreResponse {
				clientID := cr.Payload
				binaryChan <- append([]byte{byte(wsAwarenessResponseTypeSync)}, clientID...)
				select {
				case <-ctx.Done():
					return &transport.CoreResponse{
						StatusCode: 499,
						Payload:    []byte("client closed request"),
					}
				case <-time.After(30 * time.Second):
					return &transport.CoreResponse{
						StatusCode: 504,
						Payload:    []byte("timeout waiting for client to sync"),
					}
				case syncContent := <-syncChan:
					return &transport.CoreResponse{
						StatusCode: 200,
						Payload:    syncContent,
					}
				}
			},
		)
		if err != nil {
			errCh <- fmt.Errorf("failed to subscribe to awareness sync: %w", err)
			return
		}
		context.AfterFunc(ctx, func() {
			if err := subscription.Unsubscribe(); err != nil {
				slog.Error("Failed to unsubscribe from awareness sync", "error", err)
			}
		})
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
