package projectwire

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// These plans are compiled from typed OpenAPI envelopes. Only declared identity
// fields are converted. Metadata, application JSON, schema documents, signatures,
// artifacts, lease tokens and unknown fields are never traversed or reencoded.
type projectWireSchema struct {
	identity bool
	fields   map[string]string
	element  string
}
type wireEdit struct {
	start, end int
	value      []byte
}

func applyWireEdits(raw []byte, edits []wireEdit) []byte {
	var out []byte
	start := 0
	for _, e := range edits {
		out = append(out, raw[start:e.start]...)
		out = append(out, e.value...)
		start = e.end
	}
	return append(out, raw[start:]...)
}
func skipWireSeparators(raw []byte, start int, colon bool) int {
	for start < len(raw) {
		c := raw[start]
		if c == ' ' || c == '\n' || c == '\r' || c == '\t' || c == ',' || colon && c == ':' {
			start++
			continue
		}
		break
	}
	return start
}
func JSON(raw []byte, name string, toProject bool) ([]byte, error) {
	if name == "" || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return raw, nil
	}
	schema, ok := schemas[name]
	if !ok {
		return nil, fmt.Errorf("unknown project envelope %s", name)
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	tok, e := d.Token()
	if e != nil {
		return nil, e
	}
	var edits []wireEdit
	if schema.element != "" {
		if tok != json.Delim('[') {
			return nil, fmt.Errorf("invalid project array")
		}
		for d.More() {
			start := skipWireSeparators(raw, int(d.InputOffset()), false)
			var v json.RawMessage
			if e = d.Decode(&v); e != nil {
				return nil, e
			}
			changed, e := JSON(v, schema.element, toProject)
			if e != nil {
				return nil, e
			}
			edits = append(edits, wireEdit{start, int(d.InputOffset()), changed})
		}
	} else {
		if tok != json.Delim('{') {
			return nil, fmt.Errorf("invalid project object")
		}
		seen := map[string]bool{}
		for d.More() {
			start := skipWireSeparators(raw, int(d.InputOffset()), false)
			key, e := d.Token()
			if e != nil {
				return nil, e
			}
			k, ok := key.(string)
			if !ok || seen[k] {
				return nil, fmt.Errorf("invalid duplicate project envelope field")
			}
			seen[k] = true
			keyEnd := int(d.InputOffset())
			valueStart := skipWireSeparators(raw, keyEnd, true)
			var v json.RawMessage
			if e = d.Decode(&v); e != nil {
				return nil, e
			}
			if schema.identity {
				from, to := "tenantId", "projectId"
				if !toProject {
					from, to = to, from
				}
				if k == to {
					return nil, fmt.Errorf("unexpected %s in identity envelope", to)
				}
				if k == from {
					encoded, _ := json.Marshal(to)
					edits = append(edits, wireEdit{start, keyEnd, encoded})
				}
			}
			if child := schema.fields[k]; child != "" {
				changed, e := JSON(v, child, toProject)
				if e != nil {
					return nil, e
				}
				edits = append(edits, wireEdit{valueStart, int(d.InputOffset()), changed})
			}
		}
	}
	if _, e = d.Token(); e != nil {
		return nil, e
	}
	if _, e = d.Token(); e != io.EOF {
		return nil, fmt.Errorf("trailing project envelope data")
	}
	return applyWireEdits(raw, edits), nil
}

func Encode(w io.Writer, shape string, value any) error {
	raw, e := json.Marshal(value)
	if e != nil {
		return e
	}
	raw, e = JSON(raw, shape, true)
	if e != nil {
		return e
	}
	_, e = w.Write(append(raw, '\n'))
	return e
}
