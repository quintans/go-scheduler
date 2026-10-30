package trigger_test

import (
	"testing"
	"time"

	"github.com/quintans/go-scheduler/trigger"
	"github.com/stretchr/testify/require"
)

var fromEpoch = time.Unix(0, int64(1577836800000000000))

func TestSimpleTrigger(t *testing.T) {
	trg := trigger.NewSimpleTrigger(time.Second * 5)

	next := trg.Next(fromEpoch)
	require.Equal(t, int64(1577836805000000000), next.UnixNano())

	next = trg.Next(next)
	require.Equal(t, int64(1577836810000000000), next.UnixNano())

	next = trg.Next(next)
	require.Equal(t, int64(1577836815000000000), next.UnixNano())
}

func TestParse(t *testing.T) {
	t.Run("every expression produces a simple trigger", func(t *testing.T) {
		trg, err := trigger.Parse("@every 1h30m")
		require.NoError(t, err)

		simple, ok := trg.(*trigger.SimpleTrigger)
		require.True(t, ok)
		require.Equal(t, 90*time.Minute, simple.Interval)
	})

	t.Run("cron expression produces a cron trigger", func(t *testing.T) {
		trg, err := trigger.Parse("0 0 * * * *")
		require.NoError(t, err)

		cron, ok := trg.(*trigger.CronTrigger)
		require.True(t, ok)
		require.Equal(t, "0 0 * * * *", cron.Expression())
		require.Equal(t, fromEpoch.Add(time.Hour), cron.Next(fromEpoch))
	})

	t.Run("invalid expressions", func(t *testing.T) {
		for _, expr := range []string{"", "   ", "@every", "@every abc", "not a cron"} {
			_, err := trigger.Parse(expr)
			require.Error(t, err, expr)
		}
	})
}

func TestExpressionRoundTrip(t *testing.T) {
	cronTrigger, err := trigger.NewCronTrigger("1/5 * * * * *")
	require.NoError(t, err)

	for _, trg := range []trigger.Trigger{
		trigger.NewSimpleTrigger(700 * time.Millisecond),
		trigger.NewSimpleTrigger(2 * time.Hour),
		cronTrigger,
	} {
		expr := trigger.Expression(trg)
		require.NotEmpty(t, expr)

		parsed, err := trigger.Parse(expr)
		require.NoError(t, err)
		require.Equal(t, expr, trigger.Expression(parsed))
		require.Equal(t, trg.Next(fromEpoch), parsed.Next(fromEpoch))
	}
}

func TestExpressionUnsupportedTrigger(t *testing.T) {
	require.Empty(t, trigger.Expression(opaqueTrigger{}))
}

type opaqueTrigger struct{}

func (opaqueTrigger) Next(prev time.Time) time.Time { return prev }
