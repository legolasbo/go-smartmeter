package smartmeter_test

import (
	"testing"
	"time"

	"github.com/legolasbo/go-smartmeter"
)

// TestStringToTime tests parsing the telegram header
func TestStringToTime(t *testing.T) {
	t.Run("Correct input", func(t *testing.T) { stringToTimeRunner(t, "2020-02-03 13:22:33", "2020-02-03 13:22:33") })
	t.Run("Missing time", func(t *testing.T) { stringToTimeRunner(t, "2020-02-03 01:02:03", "2020-02-03") })
	t.Run("Missing date", func(t *testing.T) { stringToTimeRunner(t, time.Now().Format("2006-01-02")+" 13:22:33", "13:22:33") })
	t.Run("Empty input", func(t *testing.T) { stringToTimeRunner(t, time.Now().Format("2006-01-02")+" 01:02:03", "") })
	t.Run("Incorrect input", func(t *testing.T) { stringToTimeRunner(t, time.Now().Format("2006-01-02")+" 01:02:03", "lsbhewr") })
}

func stringToTimeRunner(t *testing.T, expectation string, input string) {
	loc, _ := time.LoadLocation("europe/Amsterdam")
	pf := "2006-01-02 15:04:05"
	expected, err := time.ParseInLocation(pf, expectation, loc)
	if err != nil {
		t.Fatal("User error! Invalid expectation string. Expecting something that matches '2006-01-02 15:04:05'")
	}

	actual, _ := smartmeter.StringToTime(input, loc, "01:02:03")
	if !expected.Equal(actual) {
		t.Errorf("Expected %s, got %s", expected, actual)
	}
}
