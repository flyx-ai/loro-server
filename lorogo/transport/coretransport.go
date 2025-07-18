package transport

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/matoous/go-nanoid/v2"
	"github.com/nats-io/nats.go"
)

type CoreResponse struct {
	StatusCode uint32
	Payload    []byte
}

const CHUNK_SIZE uint32 = 128 * 1024 // 128 KiB

var ErrNoResponders = errors.New("no responders available for subject")

func MakeRequest(
	ctx context.Context,
	nc *nats.Conn,
	subject string,
	payload []byte,
	timeout time.Duration,
) (CoreResponse, error) {
	ctx, cancel := context.WithTimeoutCause(ctx, timeout, errors.New("core nats request timeout"))
	defer cancel()

	messageID := gonanoid.Must()
	responseInbox := nats.NewInbox()
	responseChan := make(chan *nats.Msg)

	var errChan = make(chan error, 1)
	var resultChan = make(chan CoreResponse, 1)

	subscriber, err := nc.ChanSubscribe(responseInbox, responseChan)
	if err != nil {
		return CoreResponse{}, fmt.Errorf("failed to subscribe to response inbox: %w", err)
	}
	defer func() {
		err = subscriber.Unsubscribe()
		if err != nil {
			slog.Error("failed to unsubscribe from response inbox", "error", err)
		}
	}()

	go func() {
		chunks := make(map[uint32][]byte)
		hash := xxhash.New()
		hashIdx := uint32(0)
		chunkCount := uint32(0)
		digest := uint64(0)
		for {
			msg := <-responseChan
			// no responders
			if msg.Header.Get("Status") == "503" {
				errChan <- fmt.Errorf("no responders for subject %s: %w", subject, ErrNoResponders)
				return
			}
			payload := msg.Data
			chunkIdxStr := msg.Header.Get("X-Chunk-Index")
			chunkIdxU64, err := strconv.ParseUint(chunkIdxStr, 10, 32)
			chunkIdx := uint32(chunkIdxU64)
			if err != nil {
				errChan <- fmt.Errorf("invalid chunk index in response: %w", err)
				return
			}
			messageDigestStr := msg.Header.Get("X-Message-Digest")
			if messageDigestStr != "" {
				digest, err = strconv.ParseUint(messageDigestStr, 10, 64)
				if err != nil {
					errChan <- fmt.Errorf("invalid message digest in response: %w", err)
					return
				}
				chunkCount = chunkIdx + 1
			}
			chunks[chunkIdx] = payload
			for chunks[hashIdx] != nil {
				_, _ = hash.Write(chunks[hashIdx])
				hashIdx++
			}
			if chunkCount > 0 && chunkCount == hashIdx {
				sum := hash.Sum64()
				if sum != digest {
					errChan <- fmt.Errorf("message digest mismatch: expected %d, got %d", digest, sum)
					return
				}
				bufferSize := 0
				for i := uint32(0); i < chunkCount; i++ {
					if chunks[i] == nil {
						errChan <- fmt.Errorf("missing chunk %d of %d", i, chunkCount)
						return
					}
					bufferSize += len(chunks[i])
				}
				buffer := make([]byte, bufferSize)
				bufferOffset := 0
				for i := uint32(0); i < chunkCount; i++ {
					copy(buffer[bufferOffset:], chunks[i])
					bufferOffset += len(chunks[i])
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
				resultChan <- CoreResponse{
					StatusCode: statusCode,
					Payload:    buffer,
				}
			}
		}
	}()

	payloadSize := uint32(len(payload))
	for offset := uint32(0); offset < payloadSize; offset += CHUNK_SIZE {
		headers := nats.Header{}
		headers.Set("X-Message-ID", messageID)
		headers.Set("X-Chunk-Index", strconv.FormatUint(uint64(offset/CHUNK_SIZE), 10))
		if offset+CHUNK_SIZE >= payloadSize {
			hasher := xxhash.New()
			_, _ = hasher.Write(payload)
			digest := hasher.Sum64()
			headers.Set("X-Message-Digest", strconv.FormatUint(digest, 10))
		}
		go func() {
			err := nc.PublishMsg(&nats.Msg{
				Subject: subject,
				Reply:   responseInbox,
				Header:  headers,
				Data:    payload[offset:min(uint32(len(payload)), offset+CHUNK_SIZE)],
			})
			if err != nil {
				errChan <- fmt.Errorf(
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
		headers := nats.Header{}
		headers.Set("X-Message-ID", messageID)
		headers.Set("X-Chunk-Index", "0")
		hasher := xxhash.New()
		_, _ = hasher.Write([]byte{})
		headers.Set("X-Message-Digest", strconv.FormatUint(hasher.Sum64(), 10))
		err := nc.PublishMsg(&nats.Msg{
			Subject: subject,
			Reply:   responseInbox,
			Header:  headers,
			Data:    []byte{},
		})
		if err != nil {
			return CoreResponse{}, fmt.Errorf("failed to publish empty message to subject %s: %w", subject, err)
		}
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
