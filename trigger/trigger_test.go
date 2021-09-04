package trigger_test

import (
	"testing"
	"time"

	"github.com/quintans/go-scheduler/trigger"
	"github.com/stretchr/testify/require"
)

var from_epoch int64 = 1577836800000000000

func TestSimpleTrigger(t *testing.T) {
	trigger := trigger.NewSimpleTrigger(time.Second * 5)
	trigger.Description()

	next, err := trigger.NextFireTime(from_epoch)
	require.Equal(t, int64(1577836805000000000), next)
	require.NoError(t, err)

	next, err = trigger.NextFireTime(next)
	require.Equal(t, int64(1577836810000000000), next)
	require.NoError(t, err)

	next, err = trigger.NextFireTime(next)
	require.Equal(t, int64(1577836815000000000), next)
	require.NoError(t, err)
}

func TestRunOnceTrigger(t *testing.T) {
	trigger := trigger.NewRunOnceTrigger(time.Second * 5)
	trigger.Description()

	next, err := trigger.NextFireTime(from_epoch)
	require.Equal(t, int64(1577836805000000000), next)
	require.NoError(t, err)

	next, err = trigger.NextFireTime(next)
	require.Equal(t, int64(0), next)
	require.Error(t, err)
}
