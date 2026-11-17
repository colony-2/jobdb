package runtimeapi

import "encoding/json"

// JSON null is an append value, distinct from an absent optional append.
func (v *JobStartChapter) UnmarshalJSON(data []byte) error {
	type alias JobStartChapter
	var out alias
	if err := json.Unmarshal(data, &out); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	if raw, ok := fields["append"]; ok {
		out.Append = &raw
	}
	*v = JobStartChapter(out)
	return nil
}
