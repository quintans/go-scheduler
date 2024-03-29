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

	next, err := trg.NextFireTime(fromEpoch)
	require.Equal(t, int64(1577836805000000000), next.UnixNano())
	require.NoError(t, err)

	next, err = trg.NextFireTime(next)
	require.Equal(t, int64(1577836810000000000), next.UnixNano())
	require.NoError(t, err)

	next, err = trg.NextFireTime(next)
	require.Equal(t, int64(1577836815000000000), next.UnixNano())
	require.NoError(t, err)
}
