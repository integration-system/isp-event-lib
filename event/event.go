package event

import (
	"encoding/json"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
	"time"
)

var (
	fastJson = jsoniter.ConfigFastest
)

type Metadata map[string]interface{}

type BaseEvent struct {
	Type     string
	Source   string
	Id       string
	Time     time.Time
	Metadata Metadata
}

type Event struct {
	*BaseEvent
	Payload json.RawMessage
}

type TypedEvent struct {
	*BaseEvent
	Payload interface{}
}

func (e *Event) WithMetadata(md Metadata) Event {
	copied := *e
	copied.Metadata = md
	return copied
}

func New(eventType, source string, payload interface{}) (Event, error) {
	return NewWithId(eventType, source, "", payload)
}

func NewWithId(eventType, source, id string, payload interface{}) (Event, error) {
	bytes, err := marshalPayload(payload)
	if err != nil {
		return Event{}, err
	}
	return newEvent(eventType, source, id, bytes), nil
}

func NewWithJson(eventType, source string, payload []byte) Event {
	return newEvent(eventType, source, "", payload)
}

func NewWithIdJson(eventType, source, id string, payload []byte) Event {
	return newEvent(eventType, source, id, payload)
}

func newEvent(eventType, source, id string, payload json.RawMessage) Event {
	if id == "" {
		id = uuid.NewV1().String()
	}
	return Event{
		BaseEvent: &BaseEvent{
			Type:     eventType,
			Source:   source,
			Id:       id,
			Time:     time.Now(),
			Metadata: make(map[string]interface{}),
		},
		Payload: payload,
	}
}

func marshalPayload(payload interface{}) ([]byte, error) {
	bytes, err := fastJson.Marshal(payload)
	if err != nil {
		return nil, errors.WithMessage(err, "marshal payload")
	}
	return bytes, nil
}
