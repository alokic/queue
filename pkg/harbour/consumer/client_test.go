package consumer

import (
	"github.com/alokic/queue/pkg"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestCountPollStrategy_Candidate(t *testing.T) {
	t.Run("no retry queue", func(t *testing.T) {
		cps := &CountPollStrategy{NumRetryQueues: 0, MainPollStreak: 1}
		assert.Equal(t, -1, cps.Candidate(false))
		assert.Equal(t, uint(0), cps.mainPollCount)

		assert.Equal(t, -1, cps.Candidate(false))
		assert.Equal(t, uint(1), cps.mainPollCount)

		assert.Equal(t, -1, cps.Candidate(false))
		assert.Equal(t, uint(0), cps.mainPollCount)
	})

	t.Run("2 retry queue", func(t *testing.T) {
		cps := &CountPollStrategy{NumRetryQueues: 2, MainPollStreak: 2}
		assert.Equal(t, -1, cps.Candidate(false))
		assert.Equal(t, uint(0), cps.mainPollCount)

		assert.Equal(t, -1, cps.Candidate(false))
		assert.Equal(t, uint(1), cps.mainPollCount)

		assert.Equal(t, -1, cps.Candidate(false))
		assert.Equal(t, uint(2), cps.mainPollCount)

		assert.Equal(t, 1, cps.Candidate(false))
		assert.Equal(t, uint(0), cps.mainPollCount)

		assert.Equal(t, 0, cps.Candidate(false))
		assert.Equal(t, uint(0), cps.mainPollCount)

		assert.Equal(t, -1, cps.Candidate(false))
		assert.Equal(t, uint(0), cps.mainPollCount)

		assert.Equal(t, -1, cps.Candidate(false))
		assert.Equal(t, uint(1), cps.mainPollCount)
	})
}

func TestTimePollStrategy_Candidate(t *testing.T) {
	t.Run("no retry queue", func(t *testing.T) {
		cps := &TimePollStrategy{NumRetryQueues: 0, MainPollStreakMs: 50}
		assert.Equal(t, -1, cps.Candidate(false))

		assert.Equal(t, -1, cps.Candidate(false))

		assert.Equal(t, -1, cps.Candidate(false))
	})

	t.Run("2 retry queue", func(t *testing.T) {
		cps := &TimePollStrategy{NumRetryQueues: 2, MainPollStreakMs: 200}
		assert.Equal(t, -1, cps.Candidate(false))
		time.Sleep(50 * time.Millisecond)
		assert.Equal(t, -1, cps.Candidate(false))
		time.Sleep(50 * time.Millisecond)
		assert.Equal(t, -1, cps.Candidate(false))
		time.Sleep(150 * time.Millisecond)
		assert.Equal(t, 1, cps.Candidate(false))

		assert.Equal(t, 0, cps.Candidate(false))

		assert.Equal(t, -1, cps.Candidate(false))
		time.Sleep(100 * time.Millisecond)
		assert.Equal(t, -1, cps.Candidate(false))
	})
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name      string
		config    *Config
		expectErr bool
		errSubStr string
	}{
		{
			name:      "bad MsgPollInterval",
			config:    &Config{},
			expectErr: true,
			errSubStr: "MsgPollInterval",
		},
		{
			name:      "bad DLQPollInterval",
			config:    &Config{MsgPollInterval: 10 * time.Millisecond},
			expectErr: true,
			errSubStr: "DLQPollInterval",
		},
		{
			name:      "bad PS",
			config:    &Config{MsgPollInterval: 10 * time.Millisecond, DLQPollInterval: 10 * time.Millisecond},
			expectErr: true,
			errSubStr: "PS",
		},
		{
			name:      "bad CPS",
			config:    &Config{MsgPollInterval: 10 * time.Millisecond, DLQPollInterval: 10 * time.Millisecond, PS: &CountPollStrategy{}},
			expectErr: true,
			errSubStr: "MainPollStreak",
		},
		{
			name:      "bad TPS",
			config:    &Config{MsgPollInterval: 10 * time.Millisecond, DLQPollInterval: 10 * time.Millisecond, PS: &TimePollStrategy{MainPollStreakMs: 9}},
			expectErr: true,
			errSubStr: "MainPollStreakMs",
		},
		{
			name:      "bad MainHandler",
			config:    &Config{MsgPollInterval: 10 * time.Millisecond, DLQPollInterval: 10 * time.Millisecond, PS: &TimePollStrategy{MainPollStreakMs: 90}},
			expectErr: true,
			errSubStr: "MainHandler",
		},
		{
			name: "bad DLQHandler",
			config: &Config{MsgPollInterval: 10 * time.Millisecond, DLQPollInterval: 10 * time.Millisecond, PS: &TimePollStrategy{MainPollStreakMs: 90}, MainHandler: func(message pkg.Message) error {
				return nil
			}},
			expectErr: true,
			errSubStr: "DLQHandler",
		},
		{
			name: "AllGoodWithTPS",
			config: &Config{MsgPollInterval: 10 * time.Millisecond, DLQPollInterval: 10 * time.Millisecond, PS: &TimePollStrategy{MainPollStreakMs: 90}, MainHandler: func(message pkg.Message) error {
				return nil
			}, DLQHandler: func(message pkg.DLQMessage) {}},
			expectErr: false,
		},
		{
			name: "AllGoodWithCPS",
			config: &Config{MsgPollInterval: 10 * time.Millisecond, DLQPollInterval: 10 * time.Millisecond, PS: &CountPollStrategy{MainPollStreak: 90}, MainHandler: func(message pkg.Message) error {
				return nil
			}, DLQHandler: func(message pkg.DLQMessage) {}},
			expectErr: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.config.Validate()
			if test.expectErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), test.errSubStr, err)
			}
		})
	}
}
