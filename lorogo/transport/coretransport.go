package transport

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/matoous/go-nanoid/v2"
	"github.com/nats-io/nats.go"
)

type CoreResponse struct {
	StatusCode uint32
	Header     nats.Header
	Payload    []byte
}

const CHUNK_SIZE uint32 = 128 * 1024 // 128 KiB

var ErrNoResponders = errors.New("no responders available for subject")

func SendMessage(nc *nats.Conn, messageID string, subject string, payload []byte, responseInbox string, headers nats.Header) error {
	var innerErr error
	wg := sync.WaitGroup{}

	payloadSize := uint32(len(payload))
	for offset := uint32(0); offset < payloadSize; offset += CHUNK_SIZE {
		var localHeaders nats.Header
		if offset+CHUNK_SIZE >= payloadSize {
			localHeaders = headers
			if localHeaders == nil {
				localHeaders = nats.Header{}
			}
			hasher := xxhash.New()
			_, _ = hasher.Write(payload)
			digest := hasher.Sum64()
			localHeaders.Set("X-Message-Digest", strconv.FormatUint(digest, 10))
		} else {
			localHeaders = nats.Header{}
		}
		localHeaders.Set("X-Message-ID", messageID)
		localHeaders.Set("X-Chunk-Index", strconv.FormatUint(uint64(offset/CHUNK_SIZE), 10))
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := nc.PublishMsg(&nats.Msg{
				Subject: subject,
				Reply:   responseInbox,
				Header:  localHeaders,
				Data:    payload[offset:min(uint32(len(payload)), offset+CHUNK_SIZE)],
			})
			if err != nil {
				innerErr = fmt.Errorf(
					"failed to publish message %d of %d to subject %s: %w",
					offset/CHUNK_SIZE,
					payloadSize/CHUNK_SIZE,
					subject,
					err,
				)
				return
			}
		}()
	}

	if payloadSize == 0 {
		localHeaders := headers
		if localHeaders == nil {
			localHeaders = nats.Header{}
		}
		localHeaders.Set("X-Message-ID", messageID)
		localHeaders.Set("X-Chunk-Index", "0")
		hasher := xxhash.New()
		_, _ = hasher.Write([]byte{})
		localHeaders.Set("X-Message-Digest", strconv.FormatUint(hasher.Sum64(), 10))
		err := nc.PublishMsg(&nats.Msg{
			Subject: subject,
			Reply:   responseInbox,
			Header:  localHeaders,
			Data:    []byte{},
		})
		if err != nil {
			return fmt.Errorf("failed to publish empty message to subject %s: %w", subject, err)
		}
	}

	wg.Wait()
	if innerErr != nil {
		return innerErr
	}

	return nil
}

func MakeRequest(
	ctx context.Context,
	nc *nats.Conn,
	subject string,
	payload []byte,
	headers nats.Header,
	timeout time.Duration,
) (CoreResponse, error) {
	ctx, cancel := context.WithTimeoutCause(ctx, timeout, errors.New("core nats request timeout"))
	defer cancel()

	messageID := gonanoid.Must()
	responseInbox := nats.NewInbox()

	var errChan = make(chan error, 64)
	var resultChan = make(chan CoreResponse, 1)

	go func() {
		resp, _, err := SubscribeOnce(ctx, nc, responseInbox)
		if err != nil {
			errChan <- fmt.Errorf("failed to subscribe to response inbox: %w", err)
			return
		}
		resultChan <- resp
	}()

	err := SendMessage(nc, messageID, subject, payload, responseInbox, headers)
	if err != nil {
		return CoreResponse{}, fmt.Errorf("failed to send request message: %w", err)
	}

	select {
	case err := <-errChan:
		return CoreResponse{}, fmt.Errorf("error processing response: %w", err)
	case res := <-resultChan:
		return res, nil
	case <-ctx.Done():
		return CoreResponse{}, fmt.Errorf("context cancelled: %w", ctx.Err())
	}
}

type messageMeta struct {
	chunks     map[uint32][]byte
	hash       *xxhash.Digest
	hashIdx    uint32
	chunkCount uint32
	digest     uint64
	headers    nats.Header
}

func SubscribeOnce(
	ctx context.Context,
	nc *nats.Conn,
	subject string,
) (CoreResponse, string, error) {
	natsMsgChan := make(chan *nats.Msg, 64)
	subscription, err := nc.ChanSubscribe(subject, natsMsgChan)
	if err != nil {
		return CoreResponse{}, "", fmt.Errorf("failed to subscribe to response inbox: %w", err)
	}
	defer func() {
		err = subscription.Unsubscribe()
		if err != nil {
			if errors.Is(err, nats.ErrConnectionClosed) || errors.Is(err, nats.ErrBadSubscription) {
				return
			}
			slog.Error("failed to unsubscribe from response inbox", "error", err)
		}
	}()

	type resp struct {
		response CoreResponse
		reply    string
	}

	resultChan := make(chan resp, 1)
	errChan := make(chan error, 1)

	go func() {
		meta := messageMeta{
			chunks:  make(map[uint32][]byte),
			hash:    xxhash.New(),
			headers: nats.Header{},
		}
		for msg := range natsMsgChan {
			payload := msg.Data

			if msg.Header.Get("Status") == "503" {
				errChan <- fmt.Errorf("no responders for subject %s: %w", subject, ErrNoResponders)
				return
			}

			chunkIdxStr := msg.Header.Get("X-Chunk-Index")
			chunkIdxU64, err := strconv.ParseUint(chunkIdxStr, 10, 32)
			chunkIdx := uint32(chunkIdxU64)
			if err != nil {
				errChan <- fmt.Errorf("invalid chunk index in response: %w", err)
				return
			}

			messageDigestStr := msg.Header.Get("X-Message-Digest")
			if messageDigestStr != "" {
				meta.digest, err = strconv.ParseUint(messageDigestStr, 10, 64)
				if err != nil {
					errChan <- fmt.Errorf("invalid message digest in response: %w", err)
					return
				}
				meta.chunkCount = chunkIdx + 1
				for k, v := range msg.Header {
					for _, v2 := range v {
						meta.headers.Add(k, v2)
					}
				}
			}
			meta.chunks[chunkIdx] = payload
			for meta.chunks[meta.hashIdx] != nil {
				_, _ = meta.hash.Write(meta.chunks[meta.hashIdx])
				meta.hashIdx++
			}
			if meta.chunkCount > 0 && meta.chunkCount == meta.hashIdx {
				sum := meta.hash.Sum64()
				if sum != meta.digest {
					errChan <- fmt.Errorf("message digest mismatch: expected %d, got %d", meta.digest, sum)
					return
				}
				bufferSize := 0
				for i := uint32(0); i < meta.chunkCount; i++ {
					if meta.chunks[i] == nil {
						errChan <- fmt.Errorf("missing chunk %d of %d", i, meta.chunkCount)
						return
					}
					bufferSize += len(meta.chunks[i])
				}
				buffer := make([]byte, bufferSize)
				bufferOffset := 0
				for i := uint32(0); i < meta.chunkCount; i++ {
					copy(buffer[bufferOffset:], meta.chunks[i])
					bufferOffset += len(meta.chunks[i])
				}
				statusCodeStr := msg.Header.Get("X-Status-Code")
				statusCode := uint32(200)
				if statusCodeStr != "" {
					statusCodeU64, err := strconv.ParseUint(statusCodeStr, 10, 32)
					if err != nil {
						errChan <- fmt.Errorf("invalid status code in response: %w", err)
						return
					}
					statusCode = uint32(statusCodeU64)
				}
				reply := msg.Reply
				resultChan <- resp{
					response: CoreResponse{
						StatusCode: statusCode,
						Header:     meta.headers,
						Payload:    buffer,
					},
					reply: reply,
				}
				err := subscription.Unsubscribe()
				if err != nil {
					errChan <- fmt.Errorf("failed to unsubscribe from response inbox: %w", err)
				}
			}
		}
	}()

	select {
	case err := <-errChan:
		return CoreResponse{}, "", fmt.Errorf("error processing response: %w", err)
	case res := <-resultChan:
		return res.response, res.reply, nil
	case <-ctx.Done():
		return CoreResponse{}, "", fmt.Errorf("context cancelled: %w", ctx.Err())
	}
}

func Subscribe(ctx context.Context, nc *nats.Conn, subject string, queue string, handler func(CoreResponse) *CoreResponse) (*nats.Subscription, error) {
	natsMsgChan := make(chan *nats.Msg, 64)
	var subscription *nats.Subscription
	var err error
	if queue != "" {
		subscription, err = nc.ChanQueueSubscribe(subject, queue, natsMsgChan)
	} else {
		subscription, err = nc.ChanSubscribe(subject, natsMsgChan)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to response inbox: %w", err)
	}

	go func() {
		messages := map[string]messageMeta{}
		for msg := range natsMsgChan {
			payload := msg.Data

			messageID := msg.Header.Get("X-Message-ID")
			meta, ok := messages[messageID]
			if !ok {
				meta = messageMeta{
					chunks:  make(map[uint32][]byte),
					hash:    xxhash.New(),
					headers: nats.Header{},
				}
				messages[messageID] = meta
			}

			chunkIdxStr := msg.Header.Get("X-Chunk-Index")
			chunkIdxU64, err := strconv.ParseUint(chunkIdxStr, 10, 32)
			chunkIdx := uint32(chunkIdxU64)
			if err != nil {
				slog.Error("invalid chunk index in response", "error", err)
				continue
			}

			messageDigestStr := msg.Header.Get("X-Message-Digest")
			if messageDigestStr != "" {
				meta.digest, err = strconv.ParseUint(messageDigestStr, 10, 64)
				if err != nil {
					slog.Error("invalid message digest in response", "error", err)
					continue
				}
				meta.chunkCount = chunkIdx + 1
				for k, v := range msg.Header {
					for _, v2 := range v {
						meta.headers.Add(k, v2)
					}
				}
			}
			meta.chunks[chunkIdx] = payload
			for meta.chunks[meta.hashIdx] != nil {
				_, _ = meta.hash.Write(meta.chunks[meta.hashIdx])
				meta.hashIdx++
			}
			if meta.chunkCount > 0 && meta.chunkCount == meta.hashIdx {
				sum := meta.hash.Sum64()
				if sum != meta.digest {
					slog.Error("message digest mismatch", "expected", meta.digest, "got", sum)
					continue
				}
				bufferSize := 0
				for i := uint32(0); i < meta.chunkCount; i++ {
					if meta.chunks[i] == nil {
						slog.Error("missing chunk in message", "chunk", i, "of", meta.chunkCount)
						continue
					}
					bufferSize += len(meta.chunks[i])
				}
				buffer := make([]byte, bufferSize)
				bufferOffset := 0
				for i := uint32(0); i < meta.chunkCount; i++ {
					copy(buffer[bufferOffset:], meta.chunks[i])
					bufferOffset += len(meta.chunks[i])
				}
				statusCodeStr := msg.Header.Get("X-Status-Code")
				statusCode := uint32(200)
				if statusCodeStr != "" {
					statusCodeU64, err := strconv.ParseUint(statusCodeStr, 10, 32)
					if err != nil {
						slog.Error("invalid status code in response", "error", err)
						continue
					}
					statusCode = uint32(statusCodeU64)
				}
				reply := msg.Reply
				go func() {
					delete(messages, messageID)
					resp := handler(CoreResponse{
						StatusCode: statusCode,
						Header:     meta.headers,
						Payload:    buffer,
					})
					if resp != nil && reply != "" {
						err := SendMessage(nc, gonanoid.Must(), reply, resp.Payload, "", resp.Header)
						if err != nil {
							slog.Error("failed to send reply message", "error", err)
						}
					}
				}()
			}
		}
	}()

	return subscription, nil
}
