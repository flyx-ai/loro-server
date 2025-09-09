package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/cespare/xxhash/v2"
	"github.com/nats-io/nats.go/jetstream"
)

type ContainerType uint8

const (
	ContainerTypeInvalid ContainerType = iota
	ContainerTypeMap
	ContainerTypeList
	ContainerTypeMovableList
	ContainerTypeText
	ContainerTypeTree
)

// https://github.com/loro-dev/loro/blob/main/docs/JsonSchema.md
type JSONUpdateOpContent struct {
	Type            string `json:"type"` // "insert", "delete", "move", "set", "mark", "mark_end", "create"
	Pos             int    `json:"pos,omitempty"`
	Len             int    `json:"len,omitempty"`
	From            int    `json:"from,omitempty"`
	To              int    `json:"to,omitempty"`
	ElemID          string `json:"elem_id,omitempty"`
	Key             string `json:"key,omitempty"`
	Text            string `json:"text,omitempty"`
	StartID         string `json:"start_id,omitempty"`
	Start           int    `json:"start,omitempty"`
	End             int    `json:"end,omitempty"`
	StyleKey        string `json:"style_key,omitempty"`
	StyleValue      any    `json:"style_value,omitempty"`
	Info            int    `json:"info,omitempty"`
	Target          string `json:"target,omitempty"`
	Parent          string `json:"parent,omitempty"`
	FractionalIndex string `json:"fractional_index,omitempty"`
	Value           any    `json:"value,omitempty"`
}

type JSONUpdateOp struct {
	Path string              `json:"path"`
	Type string              `json:"type"`
	Op   JSONUpdateOpContent `json:"op"`
}

type JSONUpdate struct {
	DocumentID string         `json:"document_id"`
	UpdateOps  []JSONUpdateOp `json:"update_ops"`
}

func processJSONUpdateMessage(
	ctx context.Context,
	js jetstream.JetStream,
	msg jetstream.Msg,
	jsonUpdateStream jetstream.Stream,
) (JSONUpdate, error) {
	messageHeaders := msg.Headers()
	documentID := messageHeaders.Get("X-Document-ID")
	chunkCountStr := messageHeaders.Get("X-Chunk-Count")
	chunkCount, err := strconv.ParseInt(chunkCountStr, 10, 32)
	if err != nil {
		return JSONUpdate{}, fmt.Errorf("invalid chunk count: %w", err)
	}
	messageDigestStr := messageHeaders.Get("X-Message-Digest")
	messageDigest, err := strconv.ParseUint(messageDigestStr, 10, 64)
	if err != nil {
		return JSONUpdate{}, fmt.Errorf("invalid message digest: %w", err)
	}
	jsonUpdateID := messageHeaders.Get("X-JSON-Update-ID")

	messageChunks := make([][]byte, chunkCount)
	messageChunks[0] = msg.Data()

	hasher := xxhash.New()

	messagesSlice := make([]jetstream.Msg, chunkCount)
	messagesSlice[0] = msg
	messageLen := len(msg.Data())

	if chunkCount > 1 {
		err := func() error {
			restConsumerName := "json_update_rest_consumer_" + jsonUpdateID
			restConsumer, err := jsonUpdateStream.CreateConsumer(ctx, jetstream.ConsumerConfig{
				Name:          restConsumerName,
				Description:   "JSON Update Stream Rest Consumer",
				FilterSubject: fmt.Sprintf("loro.json_update.rest.%s", jsonUpdateID),
			})
			if err != nil {
				return fmt.Errorf("failed to create JSON update rest consumer: %w", err)
			}

			messages, err := restConsumer.Messages()
			if err != nil {
				return fmt.Errorf("failed to get messages from JSON update rest consumer: %w", err)
			}
			defer func() {
				messages.Stop()
				err := js.DeleteConsumer(ctx, "loro-json-update", restConsumerName)
				if err != nil {
					slog.Error("Failed to delete JSON update rest consumer", "consumerName", restConsumerName, "error", err)
				}
			}()

			for range chunkCount - 1 {
				msg, err := messages.Next()
				if err != nil {
					return fmt.Errorf("error receiving JSON update message chunk: %w", err)
				}

				chunkIdxStr := msg.Headers().Get("X-Chunk-Index")
				chunkIdx, err := strconv.ParseInt(chunkIdxStr, 10, 32)
				if err != nil {
					return fmt.Errorf("invalid chunk index: %w", err)
				}

				if messageChunks[chunkIdx] != nil {
					return fmt.Errorf("duplicate chunk index %d received for JSON update ID %s", chunkIdx, jsonUpdateID)
				}

				messageChunks[chunkIdx] = msg.Data()
				messagesSlice[chunkIdx] = msg
				messageLen += len(messageChunks[chunkIdx])
			}
			return nil
		}()
		if err != nil {
			return JSONUpdate{}, fmt.Errorf("failed to process JSON update message chunks: %w", err)
		}
	}

	buffer := make([]byte, 0, messageLen)
	for _, chunk := range messageChunks {
		buffer = append(buffer, chunk...)
	}

	_, _ = hasher.Write(buffer)
	if hasher.Sum64() != messageDigest {
		return JSONUpdate{}, fmt.Errorf("message digest mismatch for JSON update ID %s: expected %d, got %d", jsonUpdateID, messageDigest, hasher.Sum64())
	}

	var jsonUpdate JSONUpdate
	if err := json.Unmarshal(buffer, &jsonUpdate.UpdateOps); err != nil {
		return JSONUpdate{}, fmt.Errorf("failed to unmarshal JSON update message: %w", err)
	}
	jsonUpdate.DocumentID = documentID

	for _, msg := range messagesSlice {
		if err := msg.Ack(); err != nil {
			return JSONUpdate{}, fmt.Errorf("failed to acknowledge JSON update message: %w", err)
		}
	}

	return jsonUpdate, nil
}

func SubscribeJSONUpdate(ctx context.Context, js jetstream.JetStream, jsonUpdateStream jetstream.Stream) (chan JSONUpdate, error) {
	// TODO: use batch transport for json update
	consumer, err := jsonUpdateStream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Name:          "json_update_consumer",
		Durable:       "json_update_consumer",
		Description:   "JSON Update Stream Consumer",
		FilterSubject: "loro.json_update.init",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create JSON update consumer: %w", err)
	}

	messages, err := consumer.Messages()
	if err != nil {
		return nil, fmt.Errorf("failed to get messages from JSON update consumer: %w", err)
	}

	updateChan := make(chan JSONUpdate, 64)

	go func() {
		defer messages.Stop()

		for {
			msg, err := messages.Next()
			if err != nil {
				slog.Error("Error receiving JSON update message", "error", err)
			}

			go func() {
				update, err := processJSONUpdateMessage(ctx, js, msg, jsonUpdateStream)
				if err != nil {
					slog.Error("Error processing JSON update message", "error", err)
					return
				}

				updateChan <- update
			}()
		}
	}()

	return updateChan, nil
}
