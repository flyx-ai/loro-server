package web

import (
	"bytes"
	"context"
	"encoding/json"
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
	"github.com/puzpuzpuz/xsync/v4"
)

func subscribeCRDT(ctx context.Context, nc *nats.Conn, documentID string, clientID string) (chan []byte, error) {
	resChan := make(chan []byte, 256)
	ch := make(chan *nats.Msg, 256)
	subs, err := nc.ChanSubscribe(fmt.Sprintf("crdt.%s.update", documentID), ch)
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to CRDT updates: %w", err)
	}

	go func() {
		<-ctx.Done()
		if err := subs.Unsubscribe(); err != nil {
			slog.Error("Failed to unsubscribe from CRDT updates", "error", err)
		}
		close(ch)
	}()

	go func() {
		for msg := range ch {
			if msg.Header.Get("X-Client-ID") == clientID {
				continue
			}
			dat := []byte{byte(wsResponseTypeCRDT)}
			dat = append(dat, msg.Data...)
			resChan <- dat
		}
	}()

	return resChan, nil
}

type wsBinaryRequestType uint8

const (
	wsRequestTypeInvalid wsBinaryRequestType = iota
	wsRequestTypeCRDT
	wsRequestTypeSync
)

type wsBinaryResponseType uint8

const (
	wsResponseTypeInvalid wsBinaryResponseType = iota
	wsResponseTypeCRDT
	wsResponseTypeVV
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

	switch wsBinaryRequestType(requestType) {
	case wsRequestTypeCRDT:
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
	case wsRequestTypeSync:
		wg := sync.WaitGroup{}
		wg.Add(2)

		var innerErr error
		go func() {
			defer wg.Done()

			resp, err := transport.MakeRequest(ctx, nc, "loro.doc.sync_down."+documentID, data, 60*time.Second)
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
			binaryChan <- append([]byte{byte(wsResponseTypeCRDT)}, resp.Payload...)
		}()

		go func() {
			defer wg.Done()

			resp, err := transport.MakeRequest(ctx, nc, "loro.doc.version_vector."+documentID, nil, 60*time.Second)
			if err != nil {
				innerErr = fmt.Errorf("failed to get version vector: %w", err)
				return
			}
			if resp.StatusCode != http.StatusOK {
				innerErr = fmt.Errorf("version vector request failed with status code: %d", resp.StatusCode)
				return
			}
			binaryChan <- append([]byte{byte(wsResponseTypeVV)}, resp.Payload...)
		}()

		wg.Wait()
		if innerErr != nil {
			return fmt.Errorf("failed to handle sync request: %w", innerErr)
		}
	case wsRequestTypeInvalid:
		fallthrough
	default:
		return fmt.Errorf("invalid request type: %d", data[0])
	}

	return nil
}

type wsTextRequestType uint64

const (
	wsTextRequestTypeInvalid wsTextRequestType = iota
)

// //nolint:tagliatelle
// type wsRequestAwareness struct {
// 	Awareness any `json:"a"`
// }

func documentInputTextHandler(
	ctx context.Context,
	data []byte,
	documentID string,
	clientID string,
	awarenessState *xsync.Map[string, any],
	binaryChan chan []byte,
) error {
	var req []any
	if err := json.Unmarshal(data, &req); err != nil {
		return fmt.Errorf("failed to unmarshal request: %w", err)
	}
	if len(req) != 2 {
		return fmt.Errorf("invalid request length: expected 2, got %d", len(req))
	}
	reqTypeFloat, ok := req[0].(float64)
	if !ok {
		return fmt.Errorf("invalid request type: expected float64, got %T", req[0])
	}
	reqType := wsTextRequestType(reqTypeFloat)

	switch reqType {
	// var reqAwareness wsRequestAwareness
	// if err := json.Unmarshal(data, &reqAwareness); err != nil {
	// 	return ctx.Fail(err)
	// }
	//
	// typ := awarenessTypeUpdate
	//
	// _, loaded := awarenessState.LoadAndStore(clientID, reqAwareness.Awareness)
	// if !loaded {
	// 	typ = awarenessTypeAdd
	// }
	//
	// err := publishAwareness(ctx, tableID, clientID, reqAwareness.Awareness, typ)
	// if err != nil {
	// 	return ctx.Fail(err)
	// }
	case wsTextRequestTypeInvalid:
		fallthrough
	default:
		return fmt.Errorf("invalid request type: %d", reqType)
	}
}

//nolint:gocyclo
func TableListenHandler(
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
	awarenessState := xsync.NewMap[string, any]()
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

	// initAwarenessDone := make(chan struct{})
	// err = subscribeInitAwareness(ctx, documentID, awarenessState, initAwarenessDone)
	// if err != nil {
	// 	return fmt.Errorf("failed to subscribe to init awareness: %w", err)
	// }
	//
	// textChan, err := subscribeAwareness(ctx, documentID, clientID, awarenessState)
	// if err != nil {
	// 	return ctx.Fail(err)
	// }

	errCh := make(chan error, 1)

	// go func() {
	// 	defer close(initAwarenessDone)
	//
	// 	resp, err := getInitAwareness(ctx, infoRow.ID)
	// 	if err != nil {
	// 		if errors.Is(err, context.DeadlineExceeded) {
	// 			return
	// 		}
	// 		errCh <- ctx.Fail(err)
	// 		return
	// 	}
	//
	// 	for key, value := range resp.States {
	// 		awarenessState.Store(key, value)
	// 	}
	//
	// 	msgJSON, err := json.Marshal(resp)
	// 	if err != nil {
	// 		errCh <- ctx.Fail(err)
	// 		return
	// 	}
	//
	// 	err = conn.Write(ctx, websocket.MessageText, msgJSON)
	// 	if err != nil {
	// 		errCh <- ctx.Fail(err)
	// 		return
	// 	}
	// }()

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
					err := documentInputTextHandler(ctx, data, documentID, clientID, awarenessState, binaryChan)
					if err != nil {
						errCh <- fmt.Errorf("failed to handle text message: %w", err)
						return
					}
				}
			}()
		}
	}()

	go func() {
		resp, err := transport.MakeRequest(ctx, nc, "loro.doc.version_vector."+documentID, nil, 60*time.Second)
		if err != nil {
			slog.Error("Failed to get version vector", "error", err)
			return
		}
		if resp.StatusCode != http.StatusOK {
			slog.Error("Version vector request failed", "status_code", resp.StatusCode)
			return
		}
		binaryChan <- append([]byte{byte(wsResponseTypeVV)}, resp.Payload...)
	}()

	// go func() {
	// 	<-ctx.Done()
	// 	err := publishAwareness(ctx, infoRow.ID, clientID, nil, awarenessTypeRemove)
	// 	if err != nil {
	// 		_ = ctx.Fail(err)
	// 		_ = ctx.Capture(err)
	// 		return
	// 	}
	// }()

	for {
		select {
		case res := <-binaryChan:
			err := conn.Write(ctx, websocket.MessageBinary, res)
			if err != nil {
				return fmt.Errorf("failed to write binary response: %w", err)
			}
		// case res := <-textChan:
		// 	msgJSON, err := json.Marshal(res)
		// 	if err != nil {
		// 		return ctx.Fail(err)
		// 	}
		//
		// 	err = conn.Write(ctx, websocket.MessageText, msgJSON)
		// 	if err != nil {
		// 		return ctx.Fail(err)
		// 	}
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
