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

func TestParseSimpleTrigger(t *testing.T) {
	trg, err := trigger.ParseSimpleTrigger(" 1h30m ")
	require.NoError(t, err)

	simple, ok := trg.(*trigger.SimpleTrigger)
	require.True(t, ok)
	require.Equal(t, 90*time.Minute, simple.Interval)
	require.Equal(t, trigger.SimpleName, simple.Name())

	for _, expr := range []string{"", "   ", "abc", "@every 1s"} {
		_, err := trigger.ParseSimpleTrigger(expr)
		require.Error(t, err, expr)
	}
}

func TestParseCronTrigger(t *testing.T) {
	trg, err := trigger.ParseCronTrigger(" 0 0 * * * * ")
	require.NoError(t, err)

	cron, ok := trg.(*trigger.CronTrigger)
	require.True(t, ok)
	require.Equal(t, "0 0 * * * *", cron.Expression())
	require.Equal(t, trigger.CronName, cron.Name())
	require.Equal(t, fromEpoch.Add(time.Hour), cron.Next(fromEpoch))

	for _, expr := range []string{"", "   ", "not a cron", "@every"} {
		_, err := trigger.ParseCronTrigger(expr)
		require.Error(t, err, expr)
	}
}

func TestExpressionRoundTrip(t *testing.T) {
	cronTrigger, err := trigger.NewCronTrigger("1/5 * * * * *")
	require.NoError(t, err)

	factories := map[string]func(string) (trigger.Trigger, error){
		trigger.SimpleName: trigger.ParseSimpleTrigger,
		trigger.CronName:   trigger.ParseCronTrigger,
	}
	for _, trg := range []trigger.Trigger{
		trigger.NewSimpleTrigger(700 * time.Millisecond),
		trigger.NewSimpleTrigger(2 * time.Hour),
		cronTrigger,
	} {
		expr := trg.Expression()
		require.NotEmpty(t, expr)

		parsed, err := factories[trg.Name()](expr)
		require.NoError(t, err)
		require.Equal(t, trg.Name(), parsed.Name())
		require.Equal(t, expr, parsed.Expression())
		require.Equal(t, trg.Next(fromEpoch), parsed.Next(fromEpoch))
	}
}
