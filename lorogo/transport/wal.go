package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/matoous/go-nanoid/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func nowTimestamp() string {
	now := time.Now()
	nanoTime := now.UnixNano()
	return fmt.Sprintf("%d.%09d", nanoTime/1e9, nanoTime%1e9)
}

func fromTimestamp(timestamp string) (time.Time, error) {
	parts := strings.Split(timestamp, ".")
	if len(parts) != 2 {
		return time.Time{}, fmt.Errorf("invalid timestamp format: %s", timestamp)
	}
	sec, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid seconds part of timestamp: %w", err)
	}
	nsec, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid nanoseconds part of timestamp: %w", err)
	}
	return time.Unix(sec, nsec), nil
}

func PingAndInitDoc(
	ctx context.Context,
	documentID string,
	nc *nats.Conn,
	documentStatusKV jetstream.KeyValue,
) error {
	oldOperationIDs := []string{}
	for retryCount := range 3 {
		operationID := gonanoid.Must()
		oldOperationIDs = append(oldOperationIDs, operationID)
		for {
			documentEntry, err := documentStatusKV.Get(ctx, documentID)
			if err != nil {
				if errors.Is(err, jetstream.ErrKeyNotFound) {
					_, err := documentStatusKV.Create(
						ctx,
						documentID,
						fmt.Appendf(nil, "STARTING:%s:%d:%s", operationID, retryCount, nowTimestamp()),
					)
					if err == nil {
						err = nc.Publish("loro.init."+documentID, []byte(operationID))
						if err != nil {
							return fmt.Errorf("failed to publish init message: %w", err)
						}
						break
					} else {
						if errors.Is(err, jetstream.ErrKeyExists) {
							continue
						} else {
							return fmt.Errorf("failed to create document status: %w", err)
						}
					}
				} else {
					return fmt.Errorf("failed to get document status: %w", err)
				}
			}

			documentStatus := string(documentEntry.Value())
			splitStatus := strings.Split(documentStatus, ":")
			documentStatus = splitStatus[0]
			switch documentStatus {
			case "UP":
				timestampStr := splitStatus[2]
				timestamp, err := fromTimestamp(timestampStr)
				if err != nil {
					return fmt.Errorf("invalid timestamp in document status: %w", err)
				}
				now := time.Now()
				if now.Sub(timestamp) <= 5*time.Second {
					resp, err := MakeRequest(ctx, nc, "loro.doc.ping."+documentID, []byte("ping"), 5*time.Second)
					if err == nil {
						if string(resp.Payload) == "pong" {
							return nil
						}
					} else {
						if !errors.Is(err, ErrNoResponders) {
							return fmt.Errorf("failed to ping document: %w", err)
						}
					}
				}
			case "DOWN":
			case "STARTING":
				if !slices.Contains(oldOperationIDs, splitStatus[1]) {
					break
				}
			case "ERROR":
				return fmt.Errorf("document %s is in ERROR state: %s", documentID, documentStatus)
			default:
				return fmt.Errorf("unknown document status: %s", documentStatus)
			}
			_, err = documentStatusKV.Update(
				ctx,
				documentID,
				fmt.Appendf(nil, "STARTING:%s:%d:%s", operationID, retryCount, nowTimestamp()),
				documentEntry.Revision(),
			)
			if err == nil {
				err = nc.Publish("loro.init."+documentID, []byte(operationID))
				if err != nil {
					return fmt.Errorf("failed to publish init message: %w", err)
				}
				break
			} else {
				if !strings.Contains(err.Error(), "wrong last sequence") {
					return fmt.Errorf("failed to update document status: %w", err)
				}
				continue
			}
		}
		entries, err := documentStatusKV.Watch(ctx, documentID)
		if err != nil {
			return fmt.Errorf("failed to watch document status: %w", err)
		}
		defer func() {
			err = entries.Stop()
			if err != nil {
				slog.Error("failed to stop watching document status", "error", err)
			}
		}()
		updates := entries.Updates()
		for {
			ctx, cancel := context.WithTimeout(ctx, 6*time.Second)
			defer cancel()

			select {
			case update := <-updates:
				if update == nil {
					continue
				}
				updateVal := update.Value()
				if updateVal == nil {
					continue
				}
				entry := string(update.Value())
				splitStatus := strings.Split(entry, ":")
				documentStatus := splitStatus[0]
				switch documentStatus {
				case "UP":
					resp, err := MakeRequest(ctx, nc, "loro.doc.ping."+documentID, []byte("ping"), 5*time.Second)
					if err == nil {
						if string(resp.Payload) == "pong" {
							return nil
						}
					} else {
						if !errors.Is(err, ErrNoResponders) {
							return fmt.Errorf("failed to ping document: %w", err)
						}
					}
					return nil
				case "DOWN":
					break
				case "STARTING":
				case "ERROR":
					return fmt.Errorf("document %s is in ERROR state: %s", documentID, entry)
				default:
					return fmt.Errorf("unknown document status: %s", entry)
				}
			case <-ctx.Done():
				slog.Warn("Timeout waiting for document status update", "document_id", documentID)
				break
			}
		}
	}

	_, err := documentStatusKV.PutString(ctx, documentID, fmt.Sprintf("ERROR:%s:%s", strings.Repeat("0", 32), nowTimestamp()))
	if err != nil {
		return fmt.Errorf("failed to set document status to ERROR: %w", err)
	}

	return fmt.Errorf("failed to ping and init document %s after 3 retries", documentID)
}

func SendLog(
	ctx context.Context,
	documentID string,
	nc *nats.Conn,
	js jetstream.JetStream,
	documentStatusKV jetstream.KeyValue,
	body io.Reader,
) error {
	logID := gonanoid.Must()
	waitInit := sync.WaitGroup{}
	waitInit.Add(1)

	errChan := make(chan error, 1)

	go func() {
		err := PingAndInitDoc(ctx, documentID, nc, documentStatusKV)
		if err != nil {
			errChan <- fmt.Errorf("failed to ping and init document: %w", err)
			return
		}
		waitInit.Done()
	}()

	currentSize := 0
	hasher := xxhash.New()
	chunkIdx := uint32(0)

	bufferA := make([]byte, CHUNK_SIZE)
	bufferB := make([]byte, CHUNK_SIZE)
	writeA := true
	var prevCount int
	var count int
	var err error
	firstWrite := true
	for {
		if writeA {
			count, err = body.Read(bufferA)
		} else {
			count, err = body.Read(bufferB)
		}
		if count > 0 {
			if writeA {
				_, _ = hasher.Write(bufferA[:count])
			} else {
				_, _ = hasher.Write(bufferB[:count])
			}
			currentSize += prevCount
			if !firstWrite {
				buffer := make([]byte, prevCount)
				// previous buffer
				if writeA {
					copy(buffer, bufferB[:prevCount])
				} else {
					copy(buffer, bufferA[:prevCount])
				}
				headers := nats.Header{}
				headers.Add("LORO_LOG_ID", logID)
				headers.Add("LORO_CHUNK_INDEX", strconv.FormatUint(uint64(chunkIdx), 10))
				chunkIdx++
				_, err = js.PublishMsgAsync(&nats.Msg{
					Subject: "loro.wal." + documentID,
					Header:  headers,
					Data:    buffer,
				})
				if err != nil {
					return fmt.Errorf("failed to publish log chunk: %w", err)
				}
			}
			prevCount = count
			writeA = !writeA
		}
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return fmt.Errorf("failed to read body: %w", err)
			}
		}
		firstWrite = false
	}

	count = prevCount
	responseInbox := "loro.response." + gonanoid.Must()
	responseChan := make(chan struct{})

	_, err = nc.Subscribe(responseInbox, func(msg *nats.Msg) {
		msgStr := string(msg.Data)
		if msgStr == "ACK" {
			responseChan <- struct{}{}
			return
		} else if strings.HasPrefix(msgStr, "NACK:") {
			errChan <- fmt.Errorf("NACK received: %s", msgStr[5:])
			return
		} else {
			errChan <- fmt.Errorf("unexpected response: %s", msgStr)
			return
		}
	})
	if err != nil {
		return fmt.Errorf("failed to subscribe to response inbox %s: %w", responseInbox, err)
	}

	var buffer []byte
	if !writeA {
		buffer = bufferA[:count]
	} else {
		buffer = bufferB[:count]
	}
	headers := nats.Header{}
	headers.Add("LORO_LOG_ID", logID)
	headers.Add("LORO_CHUNK_INDEX", strconv.FormatUint(uint64(chunkIdx), 10))
	headers.Add("LORO_FINAL", "true")
	headers.Add("LORO_DIGEST", strconv.FormatUint(hasher.Sum64(), 10))
	headers.Add("LORO_RESPONSE_INBOX", responseInbox)
	_, err = js.PublishMsgAsync(&nats.Msg{
		Subject: "loro.wal." + documentID,
		Header:  headers,
		Data:    buffer,
	})
	if err != nil {
		return fmt.Errorf("failed to publish final log chunk: %w", err)
	}

	waitInit.Wait()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errChan:
		return fmt.Errorf("error sending log: %w", err)
	case <-responseChan:
		return nil
	}
}
