package trigger_test

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/quintans/go-scheduler/trigger"
	"github.com/stretchr/testify/require"
)

func TestCronExpression1(t *testing.T) {
	prev := time.Unix(0, int64(1555351200000000000))
	result := ""
	cronTrigger, err := trigger.NewCronTrigger("10/20 15 14 5-10 * ? *")
	cronTrigger.Description()
	if err != nil {
		t.Fatal(err)
	} else {
		result, _ = iterate(prev, cronTrigger, 1000)
	}
	require.Equal(t, "Fri Dec 8 14:15:10 2023", result)
}

func TestCronExpression2(t *testing.T) {
	prev := time.Unix(0, int64(1555351200000000000))
	result := ""
	cronTrigger, err := trigger.NewCronTrigger("* 5,7,9 14-16 * * ? *")
	if err != nil {
		t.Fatal(err)
	} else {
		result, _ = iterate(prev, cronTrigger, 1000)
	}
	require.Equal(t, "Mon Aug 5 14:05:00 2019", result)
}

func TestCronExpression3(t *testing.T) {
	prev := time.Unix(0, int64(1555351200000000000))
	result := ""
	cronTrigger, err := trigger.NewCronTrigger("* 5,7,9 14/2 * * Wed,Sat *")
	if err != nil {
		t.Fatal(err)
	} else {
		result, _ = iterate(prev, cronTrigger, 1000)
	}
	require.Equal(t, "Sat Dec 7 14:05:00 2019", result)
}

func TestCronExpression4(t *testing.T) {
	expression := "0 5,7 14 1 * Sun *"
	_, err := trigger.NewCronTrigger(expression)
	if err == nil {
		t.Fatalf("%s should fail", expression)
	}
}

func TestCronExpression5(t *testing.T) {
	prev := time.Unix(0, int64(1555351200000000000))
	result := ""
	cronTrigger, err := trigger.NewCronTrigger("* * * * * ? *")
	if err != nil {
		t.Fatal(err)
	} else {
		result, _ = iterate(prev, cronTrigger, 1000)
	}
	require.Equal(t, "Mon Apr 15 18:16:40 2019", result)
}

func TestCronExpression6(t *testing.T) {
	prev := time.Unix(0, int64(1555351200000000000))
	result := ""
	cronTrigger, err := trigger.NewCronTrigger("* * 14/2 * * Mon/3 *")
	if err != nil {
		t.Fatal(err)
	} else {
		result, _ = iterate(prev, cronTrigger, 1000)
	}
	require.Equal(t, "Mon Mar 15 18:00:00 2021", result)
}

func TestCronExpression7(t *testing.T) {
	prev := time.Unix(0, int64(1555351200000000000))
	result := ""
	cronTrigger, err := trigger.NewCronTrigger("* 5-9 14/2 * * 0-2 *")
	if err != nil {
		t.Fatal(err)
	} else {
		result, _ = iterate(prev, cronTrigger, 1000)
	}
	require.Equal(t, "Tue Jul 16 16:09:00 2019", result)
}

func TestCronDaysOfWeek(t *testing.T) {
	daysOfWeek := []string{"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"}
	expected := []string{
		"Sun Apr 21 00:00:00 2019",
		"Mon Apr 22 00:00:00 2019",
		"Tue Apr 23 00:00:00 2019",
		"Wed Apr 24 00:00:00 2019",
		"Thu Apr 18 00:00:00 2019",
		"Fri Apr 19 00:00:00 2019",
		"Sat Apr 20 00:00:00 2019",
	}

	for i := 0; i < len(daysOfWeek); i++ {
		cronDayOfWeek(t, daysOfWeek[i], expected[i])
		cronDayOfWeek(t, strconv.Itoa(i), expected[i])
	}
}

func cronDayOfWeek(t *testing.T, dayOfWeek, expected string) {
	prev := time.Unix(0, int64(1555524000000000000)) // Wed Apr 17 18:00:00 2019
	expression := fmt.Sprintf("0 0 0 * * %s", dayOfWeek)
	cronTrigger, err := trigger.NewCronTrigger(expression)
	if err != nil {
		t.Fatal(err)
	} else {
		nextFireTime, err := cronTrigger.NextFireTime(prev)
		if err != nil {
			t.Fatal(err)
		} else {
			require.Equal(t, expected, time.Unix(nextFireTime.Unix(), 0).UTC().Format(readDateLayout))
		}
	}
}

func TestCronYearly(t *testing.T) {
	prev := time.Unix(0, int64(1555351200000000000))
	result := ""
	cronTrigger, err := trigger.NewCronTrigger("@yearly")
	if err != nil {
		t.Fatal(err)
	} else {
		result, _ = iterate(prev, cronTrigger, 100)
	}
	require.Equal(t, "Sun Jan 1 00:00:00 2119", result)
}

func TestCronMonthly(t *testing.T) {
	prev := time.Unix(0, int64(1555351200000000000))
	result := ""
	cronTrigger, err := trigger.NewCronTrigger("@monthly")
	if err != nil {
		t.Fatal(err)
	} else {
		result, _ = iterate(prev, cronTrigger, 100)
	}
	require.Equal(t, "Sun Aug 1 00:00:00 2027", result)
}

func TestCronWeekly(t *testing.T) {
	prev := time.Unix(0, int64(1555351200000000000))
	result := ""
	cronTrigger, err := trigger.NewCronTrigger("@weekly")
	if err != nil {
		t.Fatal(err)
	} else {
		result, _ = iterate(prev, cronTrigger, 100)
	}
	require.Equal(t, "Sun Mar 14 00:00:00 2021", result)
}

func TestCronDaily(t *testing.T) {
	prev := time.Unix(0, int64(1555351200000000000))
	result := ""
	cronTrigger, err := trigger.NewCronTrigger("@daily")
	if err != nil {
		t.Fatal(err)
	} else {
		result, _ = iterate(prev, cronTrigger, 1000)
	}
	require.Equal(t, "Sun Jan 9 00:00:00 2022", result)
}

func TestCronHourly(t *testing.T) {
	prev := time.Unix(0, int64(1555351200000000000))
	result := ""
	cronTrigger, err := trigger.NewCronTrigger("@hourly")
	if err != nil {
		t.Fatal(err)
	} else {
		result, _ = iterate(prev, cronTrigger, 1000)
	}
	require.Equal(t, "Wed May 29 06:00:00 2019", result)
}

var readDateLayout = "Mon Jan 2 15:04:05 2006"

func iterate(prev time.Time, cronTrigger *trigger.CronTrigger, iterations int) (string, error) {
	var err error
	for i := 0; i < iterations; i++ {
		prev, err = cronTrigger.NextFireTime(prev)
		// fmt.Println(time.Unix(prev/int64(time.Second), 0).UTC().Format(readDateLayout))
		if err != nil {
			return "", err
		}
	}
	return time.Unix(prev.Unix(), 0).UTC().Format(readDateLayout), nil
}
