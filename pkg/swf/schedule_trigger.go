package swf

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

func InitialScheduleFire(trigger ScheduleTrigger, now time.Time) (*time.Time, error) {
	now = now.UTC()
	switch trigger.Kind {
	case ScheduleTriggerInterval:
		next := now
		if trigger.StartAt != nil && trigger.StartAt.UTC().After(now) {
			next = trigger.StartAt.UTC()
		}
		if trigger.EndAt != nil && next.After(trigger.EndAt.UTC()) {
			return nil, nil
		}
		return &next, nil
	case ScheduleTriggerCron:
		after := now.Add(-time.Nanosecond)
		if trigger.StartAt != nil && trigger.StartAt.UTC().After(now) {
			after = trigger.StartAt.UTC().Add(-time.Minute)
		}
		return NextScheduleFire(trigger, after)
	default:
		return nil, fmt.Errorf("unsupported trigger kind %q", trigger.Kind)
	}
}

func NextScheduleFire(trigger ScheduleTrigger, after time.Time) (*time.Time, error) {
	after = after.UTC()
	switch trigger.Kind {
	case ScheduleTriggerInterval:
		if trigger.Interval <= 0 {
			return nil, fmt.Errorf("interval trigger requires a positive interval")
		}
		next := after.Add(trigger.Interval)
		if trigger.StartAt != nil && next.Before(trigger.StartAt.UTC()) {
			next = trigger.StartAt.UTC()
		}
		if trigger.EndAt != nil && next.After(trigger.EndAt.UTC()) {
			return nil, nil
		}
		return &next, nil
	case ScheduleTriggerCron:
		spec, err := parseCronSpec(trigger.Expression)
		if err != nil {
			return nil, err
		}
		loc := time.UTC
		if strings.TrimSpace(trigger.Timezone) != "" {
			loaded, err := time.LoadLocation(trigger.Timezone)
			if err != nil {
				return nil, fmt.Errorf("load schedule timezone: %w", err)
			}
			loc = loaded
		}
		cursor := after.In(loc).Truncate(time.Minute).Add(time.Minute)
		if trigger.StartAt != nil && cursor.Before(trigger.StartAt.In(loc)) {
			cursor = trigger.StartAt.In(loc).Truncate(time.Minute)
		}
		end := time.Time{}
		if trigger.EndAt != nil {
			end = trigger.EndAt.In(loc)
		}
		for i := 0; i < 366*24*60*5; i++ {
			if !end.IsZero() && cursor.After(end) {
				return nil, nil
			}
			if spec.matches(cursor) {
				next := cursor.UTC()
				return &next, nil
			}
			cursor = cursor.Add(time.Minute)
		}
		return nil, fmt.Errorf("no cron fire time found within search window")
	default:
		return nil, fmt.Errorf("unsupported trigger kind %q", trigger.Kind)
	}
}

type cronSpec struct {
	minutes  map[int]bool
	hours    map[int]bool
	days     map[int]bool
	months   map[int]bool
	weekdays map[int]bool
}

func parseCronSpec(expr string) (cronSpec, error) {
	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return cronSpec{}, fmt.Errorf("cron expression must have five fields")
	}
	minutes, err := parseCronField(fields[0], 0, 59, false)
	if err != nil {
		return cronSpec{}, fmt.Errorf("cron minute: %w", err)
	}
	hours, err := parseCronField(fields[1], 0, 23, false)
	if err != nil {
		return cronSpec{}, fmt.Errorf("cron hour: %w", err)
	}
	days, err := parseCronField(fields[2], 1, 31, false)
	if err != nil {
		return cronSpec{}, fmt.Errorf("cron day: %w", err)
	}
	months, err := parseCronField(fields[3], 1, 12, false)
	if err != nil {
		return cronSpec{}, fmt.Errorf("cron month: %w", err)
	}
	weekdays, err := parseCronField(fields[4], 0, 7, true)
	if err != nil {
		return cronSpec{}, fmt.Errorf("cron weekday: %w", err)
	}
	return cronSpec{minutes: minutes, hours: hours, days: days, months: months, weekdays: weekdays}, nil
}

func parseCronField(field string, min int, max int, sundayAlias bool) (map[int]bool, error) {
	out := map[int]bool{}
	for _, part := range strings.Split(field, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, fmt.Errorf("empty segment")
		}
		step := 1
		if strings.Contains(part, "/") {
			pieces := strings.Split(part, "/")
			if len(pieces) != 2 {
				return nil, fmt.Errorf("invalid step %q", part)
			}
			part = pieces[0]
			parsed, err := strconv.Atoi(pieces[1])
			if err != nil || parsed <= 0 {
				return nil, fmt.Errorf("invalid step %q", pieces[1])
			}
			step = parsed
		}
		start, end, err := cronRange(part, min, max)
		if err != nil {
			return nil, err
		}
		for v := start; v <= end; v += step {
			normalized := v
			if sundayAlias && normalized == 7 {
				normalized = 0
			}
			out[normalized] = true
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("empty field")
	}
	return out, nil
}

func cronRange(part string, min int, max int) (int, int, error) {
	if part == "*" {
		return min, max, nil
	}
	if strings.Contains(part, "-") {
		pieces := strings.Split(part, "-")
		if len(pieces) != 2 {
			return 0, 0, fmt.Errorf("invalid range %q", part)
		}
		start, err := strconv.Atoi(pieces[0])
		if err != nil {
			return 0, 0, fmt.Errorf("invalid range start %q", pieces[0])
		}
		end, err := strconv.Atoi(pieces[1])
		if err != nil {
			return 0, 0, fmt.Errorf("invalid range end %q", pieces[1])
		}
		if start > end {
			return 0, 0, fmt.Errorf("range start greater than end")
		}
		if start < min || end > max {
			return 0, 0, fmt.Errorf("range out of bounds")
		}
		return start, end, nil
	}
	value, err := strconv.Atoi(part)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid value %q", part)
	}
	if value < min || value > max {
		return 0, 0, fmt.Errorf("value %d out of bounds", value)
	}
	return value, value, nil
}

func (s cronSpec) matches(t time.Time) bool {
	weekday := int(t.Weekday())
	return s.minutes[t.Minute()] &&
		s.hours[t.Hour()] &&
		s.days[t.Day()] &&
		s.months[int(t.Month())] &&
		s.weekdays[weekday]
}
