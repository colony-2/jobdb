// Package clientpayload implements opaque, lossless job state updates. It has no
// dependency on a runtime or storage backend.
package clientpayload

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

const MaxBytes = 65536
const MaxDepth = 128

var ErrConflict = errors.New("workflow state conflict")
var ErrInvalid = errors.New("invalid client payload")
var ErrTooLarge = errors.New("client payload exceeds size limit")

// Update is an explicit patch/reset. Nil *Update preserves stored state.
// Value nil with reset clears; the bytes "null" store a present JSON null.
type Update struct {
	Mode             string          `json:"mode"`
	Value            json.RawMessage `json:"value,omitempty"`
	ExpectedRevision *int64          `json:"expectedRevision,omitempty,string"`
}

// UnmarshalJSON rejects ambiguous or unrecognized update shapes.
func (u *Update) UnmarshalJSON(raw []byte) error {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return fmt.Errorf("%w: update cannot be null", ErrInvalid)
	}
	dup := json.NewDecoder(bytes.NewReader(raw))
	dup.UseNumber()
	shape, err := readValue(dup, -1)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if fields, ok := shape.(map[string]any); ok {
		if revision, present := fields["expectedRevision"]; present && revision == nil {
			return fmt.Errorf("%w: expectedRevision cannot be null", ErrInvalid)
		}
	}
	type wire Update
	var v wire
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&v); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	*u = Update(v)
	return nil
}

func Clone(raw json.RawMessage) json.RawMessage { return append(json.RawMessage(nil), raw...) }

// ValidateUpdate validates shape and revision without examining stored state.
func ValidateUpdate(u *Update, initial bool) error {
	if u == nil {
		return nil
	}
	if initial {
		if u.ExpectedRevision != nil {
			return fmt.Errorf("%w: submit cannot specify expected revision", ErrInvalid)
		}
	} else if u.ExpectedRevision == nil || *u.ExpectedRevision < 0 {
		return fmt.Errorf("%w: expected revision is required", ErrInvalid)
	}
	switch u.Mode {
	case "patch":
		if u.Value == nil {
			return fmt.Errorf("%w: patch value required", ErrInvalid)
		}
	case "reset":
	default:
		return fmt.Errorf("%w: mode must be patch or reset", ErrInvalid)
	}
	if u.Value != nil {
		_, err := parse(u.Value)
		return err
	}
	return nil
}

// Apply validates and computes an update. Call it under the operation's storage
// lock/transaction, after checking execution ownership or waiting-task identity.
func Apply(current json.RawMessage, revision int64, u *Update) (json.RawMessage, int64, error) {
	if err := ValidateUpdate(u, false); err != nil {
		return nil, revision, err
	}
	if u == nil {
		return Clone(current), revision, nil
	}
	if revision != *u.ExpectedRevision {
		return nil, revision, fmt.Errorf("%w: client payload revision is %d, expected %d", ErrConflict, revision, *u.ExpectedRevision)
	}
	if revision == math.MaxInt64 {
		return nil, revision, fmt.Errorf("%w: revision exhausted", ErrConflict)
	}
	value, err := applyValue(current, u)
	if err != nil {
		return nil, revision, err
	}
	return value, revision + 1, nil
}

func Initial(u *Update) (json.RawMessage, int64, error) {
	if err := ValidateUpdate(u, true); err != nil {
		return nil, 0, err
	}
	if u == nil {
		return nil, 0, nil
	}
	value, err := applyValue(nil, u)
	if err != nil {
		return nil, 0, err
	}
	if value == nil {
		return nil, 0, nil
	}
	return value, 1, nil
}

func applyValue(current json.RawMessage, u *Update) (json.RawMessage, error) {
	if u.Mode == "reset" {
		return Clone(u.Value), nil
	}
	patch, err := parse(u.Value)
	if err != nil {
		return nil, err
	}
	var target any
	if current != nil {
		target, err = parse(current)
		if err != nil {
			return nil, err
		}
	}
	out, err := json.Marshal(merge(target, patch))
	if err != nil {
		return nil, err
	}
	if _, err := parse(out); err != nil {
		return nil, err
	}
	return out, nil
}

func merge(target, patch any) any {
	obj, ok := patch.(map[string]any)
	if !ok {
		return patch
	}
	dst, ok := target.(map[string]any)
	if !ok {
		dst = map[string]any{}
	}
	for k, v := range obj {
		if v == nil {
			delete(dst, k)
		} else {
			dst[k] = merge(dst[k], v)
		}
	}
	return dst
}

// Digest identifies a JSON value independently of whitespace, object order and
// equivalent decimal spellings. Absence is distinct from JSON null.
func Digest(raw json.RawMessage) (string, error) {
	if raw == nil {
		sum := sha256.Sum256([]byte("absent"))
		return hex.EncodeToString(sum[:]), nil
	}
	v, err := parse(raw)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	canonical(&b, v)
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:]), nil
}

func canonical(b *strings.Builder, v any) {
	switch x := v.(type) {
	case nil:
		b.WriteString("null")
	case bool:
		b.WriteString(strconv.FormatBool(x))
	case string:
		raw, _ := json.Marshal(x)
		b.Write(raw)
	case json.Number:
		b.WriteString(normalNumber(string(x)))
	case []any:
		b.WriteByte('[')
		for i, item := range x {
			if i > 0 {
				b.WriteByte(',')
			}
			canonical(b, item)
		}
		b.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		b.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				b.WriteByte(',')
			}
			canonical(b, k)
			b.WriteByte(':')
			canonical(b, x[k])
		}
		b.WriteByte('}')
	}
}

func normalNumber(s string) string {
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	e := new(big.Int)
	if i := strings.IndexAny(s, "eE"); i >= 0 {
		e.SetString(strings.TrimPrefix(s[i+1:], "+"), 10)
		s = s[:i]
	}
	if i := strings.IndexByte(s, '.'); i >= 0 {
		e.Sub(e, big.NewInt(int64(len(s)-i-1)))
		s = s[:i] + s[i+1:]
	}
	s = strings.TrimLeft(s, "0")
	if s == "" {
		return "0"
	}
	n := len(s)
	s = strings.TrimRight(s, "0")
	e.Add(e, big.NewInt(int64(n-len(s))))
	if neg {
		s = "-" + s
	}
	return s + "e" + e.String()
}

func parse(raw []byte) (any, error) {
	if len(raw) > MaxBytes {
		return nil, ErrTooLarge
	}
	if !utf8.Valid(raw) || !json.Valid(raw) {
		return nil, fmt.Errorf("%w: expected one UTF-8 JSON value", ErrInvalid)
	}
	// encoding/json otherwise silently substitutes unpaired surrogate escapes.
	for i := 0; i < len(raw); i++ {
		if raw[i] != '"' {
			continue
		}
		for i++; i < len(raw) && raw[i] != '"'; i++ {
			if raw[i] != '\\' {
				continue
			}
			i++
			if raw[i] != 'u' {
				continue
			}
			n, _ := strconv.ParseUint(string(raw[i+1:i+5]), 16, 16)
			i += 4
			if n == 0 {
				return nil, fmt.Errorf("%w: U+0000 is unsupported", ErrInvalid)
			}
			if n >= 0xD800 && n <= 0xDBFF {
				if i+6 >= len(raw) || raw[i+1] != '\\' || raw[i+2] != 'u' {
					return nil, fmt.Errorf("%w: unpaired surrogate", ErrInvalid)
				}
				low, err := strconv.ParseUint(string(raw[i+3:i+7]), 16, 16)
				if err != nil || low < 0xDC00 || low > 0xDFFF {
					return nil, fmt.Errorf("%w: unpaired surrogate", ErrInvalid)
				}
				i += 6
			} else if n >= 0xDC00 && n <= 0xDFFF {
				return nil, fmt.Errorf("%w: unpaired surrogate", ErrInvalid)
			}
		}
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	v, err := readValue(d, 0)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if _, err = d.Token(); err != io.EOF {
		return nil, fmt.Errorf("%w: trailing JSON", ErrInvalid)
	}
	return v, nil
}

func readValue(d *json.Decoder, depth int) (any, error) {
	t, err := d.Token()
	if err != nil {
		return nil, err
	}
	delim, ok := t.(json.Delim)
	if !ok {
		return t, nil
	}
	if depth >= MaxDepth {
		return nil, fmt.Errorf("nesting exceeds %d", MaxDepth)
	}
	switch delim {
	case '{':
		out := map[string]any{}
		for d.More() {
			key, err := d.Token()
			if err != nil {
				return nil, err
			}
			k := key.(string)
			if _, ok := out[k]; ok {
				return nil, fmt.Errorf("duplicate object name %q", k)
			}
			v, err := readValue(d, depth+1)
			if err != nil {
				return nil, err
			}
			out[k] = v
		}
		_, err = d.Token()
		return out, err
	case '[':
		out := []any{}
		for d.More() {
			v, err := readValue(d, depth+1)
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		_, err = d.Token()
		return out, err
	}
	return nil, fmt.Errorf("unexpected delimiter")
}
