package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"

	"github.com/colony-2/jobdb/pkg/jobdb/internal/runtimeapi"
)

type requestBodyKey struct{}

// Reject unknown API fields without decoding client JSON into interface values.
func strictRequestFields(next runtimeapi.StrictHandlerFunc, _ string) runtimeapi.StrictHandlerFunc {
	return func(ctx context.Context, w http.ResponseWriter, r *http.Request, request interface{}) (interface{}, error) {
		raw, _ := ctx.Value(requestBodyKey{}).([]byte)
		v := reflect.ValueOf(request)
		if v.Kind() == reflect.Struct {
			body := v.FieldByName("Body")
			if body.IsValid() && len(raw) > 0 {
				if err := validateRequestFields(raw, body.Type(), ""); err != nil {
					return nil, badRequest(err.Error())
				}
			}
		}
		return next(ctx, w, r, request)
	}
}

func validateRequestFields(raw []byte, t reflect.Type, path string) error {
	if path == "clientPayloadUpdate" || strings.HasSuffix(path, ".clientPayloadUpdate") {
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return fmt.Errorf("%s cannot be null", path)
		}
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	// Raw messages, custom unions, application objects and scalar codecs stay opaque.
	if t.Kind() == reflect.Slice && t.Elem().Kind() != reflect.Uint8 {
		var values []json.RawMessage
		if json.Unmarshal(raw, &values) != nil {
			return nil
		}
		for _, v := range values {
			if err := validateRequestFields(v, t.Elem(), path+"[]"); err != nil {
				return err
			}
		}
	}
	if t.Kind() != reflect.Struct {
		return nil
	}
	fields := map[string]reflect.Type{}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		tag := strings.Split(f.Tag.Get("json"), ",")[0]
		if tag != "" && tag != "-" {
			fields[tag] = f.Type
		}
	}
	if len(fields) == 0 {
		return nil
	}
	var values map[string]json.RawMessage
	if json.Unmarshal(raw, &values) != nil {
		return nil
	}
	for name, v := range values {
		ft, ok := fields[name]
		if !ok {
			return fmt.Errorf("unknown field %s%s", path+".", name)
		}
		child := name
		if path != "" {
			child = path + "." + name
		}
		if err := validateRequestFields(v, ft, child); err != nil {
			return err
		}
	}
	return nil
}
