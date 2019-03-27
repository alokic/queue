package pkg

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMessage_Encode(t *testing.T) {
	m := Message{
		ID:          1,
		Name:        "test",
		Description: "test",
		Idempotent:  true,
		Data:        json.RawMessage(`{}`),
		ErrorPolicy: ErrorPolicy{RepeatQueue: -1, RepeatCount: 1},
		Attempt:     0,
	}

	b, err := m.Encode()
	assert.NoError(t, err)
	assert.NotNil(t, b)
}

func TestDecodeMessage(t *testing.T) {
	t.Run("decode bad message", func(t *testing.T) {
		_, err := DecodeMessage([]byte(`wow`))
		assert.Error(t, err)
	})

	t.Run("decode good message", func(t *testing.T) {
		m := Message{
			ID:          1,
			Name:        "test",
			Description: "test",
			Idempotent:  true,
			Data:        json.RawMessage(`{}`),
			ErrorPolicy: ErrorPolicy{RepeatQueue: -1, RepeatCount: 1},
			Attempt:     0,
		}

		b, _ := m.Encode()
		msg, err := DecodeMessage(b)
		assert.NoError(t, err)
		assert.Equal(t, *msg, m)
	})
}

func TestDecodeDLQMessage(t *testing.T) {
	t.Run("decode bad message", func(t *testing.T) {
		_, err := DecodeDLQMessage([]byte(`wow`))
		assert.Error(t, err)
	})

	t.Run("decode good message", func(t *testing.T) {
		m := DLQMessage{
			RawJob: []byte("aa"),
			Reason: "alpha",
		}

		b, err := json.Marshal(&m)

		msg, err := DecodeDLQMessage(b)
		assert.NoError(t, err)
		assert.Equal(t, *msg, m)

	})
}
