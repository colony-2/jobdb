package clientpayload

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestMergePatchAndPresence(t *testing.T) {
	r := int64(4)
	got, rev, err := Apply(json.RawMessage(`{"n":9007199254740993,"nested":{"a":1,"b":2},"array":[1]}`), r, &Update{Mode: "patch", ExpectedRevision: &r, Value: json.RawMessage(`{"nested":{"a":null,"c":null},"array":[null],"run_policy":{"user":true}}`)})
	want := `{"array":[null],"n":9007199254740993,"nested":{"b":2},"run_policy":{"user":true}}`
	if err != nil || rev != 5 || string(got) != want {
		t.Fatalf("merge: %s %d %v", got, rev, err)
	}
	r = rev
	got, rev, err = Apply(got, r, &Update{Mode: "reset", ExpectedRevision: &r, Value: json.RawMessage(`null`)})
	if err != nil || string(got) != "null" || rev != 6 {
		t.Fatalf("null: %s %d %v", got, rev, err)
	}
	r = rev
	got, rev, err = Apply(got, r, &Update{Mode: "reset", ExpectedRevision: &r})
	if err != nil || got != nil || rev != 7 {
		t.Fatalf("clear: %s %d %v", got, rev, err)
	}
	got, rev, err = Apply(got, rev, nil)
	if err != nil || got != nil || rev != 7 {
		t.Fatal("preserve changed state")
	}
}
func TestDigestLossless(t *testing.T) {
	for _, pair := range [][2]string{{`{"b":1.20e3,"a":"<"}`, `{"a":"\u003c","b":1200}`}, {`-0e999`, `0`}, {`1e999999999999999999999`, `10e999999999999999999998`}} {
		a, err := Digest(json.RawMessage(pair[0]))
		if err != nil {
			t.Fatal(err)
		}
		b, err := Digest(json.RawMessage(pair[1]))
		if err != nil || a != b {
			t.Fatalf("equivalent values differ: %v %v", pair, err)
		}
	}
	a, _ := Digest(nil)
	b, _ := Digest(json.RawMessage(`null`))
	if a == b {
		t.Fatal("absent equals null")
	}
	a, _ = Digest(json.RawMessage(`9007199254740992`))
	b, _ = Digest(json.RawMessage(`9007199254740993`))
	if a == b {
		t.Fatal("numbers rounded")
	}
}
func TestValidationAndConflicts(t *testing.T) {
	for _, raw := range []string{`{"a":1,"\u0061":2}`, `"\ud800"`, `"\udc00"`, `"\u0000"`, `true false`, strings.Repeat("[", 129) + "0" + strings.Repeat("]", 129), string([]byte{'"', 255, '"'})} {
		if _, _, err := Initial(&Update{Mode: "reset", Value: json.RawMessage(raw)}); !errors.Is(err, ErrInvalid) {
			t.Fatalf("accepted %q: %v", raw, err)
		}
	}
	if _, _, err := Initial(&Update{Mode: "reset", Value: json.RawMessage(`"` + strings.Repeat("a", MaxBytes) + `"`)}); !errors.Is(err, ErrTooLarge) {
		t.Fatal(err)
	}
	r := int64(0)
	if _, _, err := Apply(nil, 1, &Update{Mode: "reset", ExpectedRevision: &r}); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	if _, _, err := Apply(nil, 0, &Update{Mode: "reset"}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, _, err := Initial(&Update{Mode: "patch"}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, _, err := Initial(&Update{Mode: "reset", ExpectedRevision: &r}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, _, err := Initial(&Update{Mode: "reset", Value: json.RawMessage(`"\ud83d\ude00"`)}); err != nil {
		t.Fatal(err)
	}
}
