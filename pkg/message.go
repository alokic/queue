package pkg

import (
	"encoding/json"
)

type (
	//ErrorPolicy is error policy for job. Not setting error policy means no repeat.
	ErrorPolicy struct {
		RepeatCount int `json:"repeat_count"`   // 0 means no repeat
		RepeatQueue int `json:"repeat_backoff"` // -1 MLQ, 0 ... n-1 that queue index frozen, > n-1 last one
	}

	//Message is a single unit of work sent to worker. Do not set ID and Attempt.
	Message struct {
		ID          uint64          `json:"id"`
		ExternalID  string          `json:"external_id"`
		Name        string          `json:"name"`
		Description string          `json:"description"`
		Idempotent  bool            `json:"idempotent"`
		Data        json.RawMessage `json:"data"`
		ErrorPolicy ErrorPolicy     `json:"error_policy"`
		Attempt     int             `json:"attempt"`
	}

	//DLQMessage is job sent to DLQ.
	DLQMessage struct {
		RawJob []byte `json:"raw_job"`
		Reason string `json:"reason"`
	}

	//AckMessage for ackmgr msg.
	AckMessage struct {
		ID         uint64 `json:"id"`
		ExternalID string `json:"external_id"`
		Name       string `json:"name"`
		Attempt    int    `json:"attempt"`
	}
)

/*
We don't provide a Message constructor here as we won't be able to modify it
in future if we add a new field.
*/

//Encode a message to binary.
func (m *Message) Encode() (json.RawMessage, error) {
	return json.Marshal(m)
}

//DecodeMessage is for decoding Message from json binary.
func DecodeMessage(d json.RawMessage) (*Message, error) {
	m := new(Message)
	err := json.Unmarshal(d, m)
	if err != nil {
		return nil, err
	}
	return m, nil
}

//DecodeDLQMessage is for decoding DLQMessage from json binary.
func DecodeDLQMessage(d json.RawMessage) (*DLQMessage, error) {
	m := new(DLQMessage)
	err := json.Unmarshal(d, m)
	if err != nil {
		return nil, err
	}
	return m, nil
}
