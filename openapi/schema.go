// Package openapi turns a declared event catalog into an OpenAPI 3.2 document.
//
// No format is invented here. OpenAPI 3.2 gained first-class support for
// server-sent events: a sequential media type carries an itemSchema describing
// one event rather than the whole stream, and the specification publishes a
// generic schema for text/event-stream along with the pattern for a stream that
// carries several event types — a oneOf discriminated by a const event name,
// with contentMediaType and contentSchema describing the JSON inside data.
//
// That maps one to one onto a catalog, which is why the catalog is shaped the
// way it is.
//
// # Reflection lives here, on purpose
//
// Deriving a JSON Schema from a Go type needs reflection. This package runs at
// startup or from a command, never per event, so that is the right place for
// it — and it is why the core carries a zero value of the payload type rather
// than a reflect.Type: nothing on the delivery path ever imports reflect.
package openapi

import (
	"encoding/json"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// Schema is a JSON Schema, kept as an ordered-ish map so the output is stable.
type Schema map[string]any

// schemaFor derives a JSON Schema from a Go value's type.
//
// Named struct types become references into components/schemas so a type used
// by several events appears once. defs collects them.
func schemaFor(t reflect.Type, defs map[string]Schema, seen map[reflect.Type]string) Schema {
	if t == nil {
		return Schema{}
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	// time.Time is a struct, but describing its fields would be nonsense.
	if t == reflect.TypeOf(time.Time{}) {
		return Schema{"type": "string", "format": "date-time"}
	}
	if t == reflect.TypeOf(json.RawMessage{}) {
		return Schema{}
	}

	switch t.Kind() {
	case reflect.Bool:
		return Schema{"type": "boolean"}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		s := Schema{"type": "integer"}
		if t.Kind() == reflect.Int64 || t.Kind() == reflect.Uint64 {
			s["format"] = "int64"
		}
		if strings.HasPrefix(t.Kind().String(), "uint") {
			s["minimum"] = 0
		}
		return s
	case reflect.Float32, reflect.Float64:
		return Schema{"type": "number"}
	case reflect.String:
		return Schema{"type": "string"}

	case reflect.Slice, reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 {
			// []byte marshals to base64 in encoding/json.
			return Schema{"type": "string", "contentEncoding": "base64"}
		}
		return Schema{"type": "array", "items": schemaFor(t.Elem(), defs, seen)}

	case reflect.Map:
		return Schema{
			"type":                 "object",
			"additionalProperties": schemaFor(t.Elem(), defs, seen),
		}

	case reflect.Struct:
		if t.Name() == "" {
			return structSchema(t, defs, seen)
		}
		name, known := seen[t]
		if !known {
			name = componentName(t, defs)
			// Reserve the name before recursing, so a self-referential type
			// terminates instead of blowing the stack.
			seen[t] = name
			defs[name] = Schema{}
			defs[name] = structSchema(t, defs, seen)
		}
		return Schema{"$ref": "#/components/schemas/" + name}

	case reflect.Interface:
		return Schema{} // anything

	default:
		return Schema{}
	}
}

// componentName picks the key a type appears under in components/schemas.
//
// The bare type name is used while it is unambiguous, because that is what a
// reader wants to see. Two different types that happen to share a name — from
// different packages — must not collapse into one entry: the document would
// then describe the wrong shape for one of them, and a generated client would
// be wrong in a way nothing checks. So the second one is qualified by its
// package, and a further clash is numbered.
func componentName(t reflect.Type, defs map[string]Schema) string {
	name := t.Name()
	if _, taken := defs[name]; !taken {
		return name
	}

	if pkg := t.PkgPath(); pkg != "" {
		qualified := sanitiseComponent(pkg[strings.LastIndexByte(pkg, '/')+1:]) + "." + name
		if _, taken := defs[qualified]; !taken {
			return qualified
		}
		name = qualified
	}
	for i := 2; ; i++ {
		candidate := name + "." + strconv.Itoa(i)
		if _, taken := defs[candidate]; !taken {
			return candidate
		}
	}
}

// sanitiseComponent keeps a key within what OpenAPI allows for a component
// name: letters, digits, dot, dash and underscore.
func sanitiseComponent(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9', r == '.', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

func structSchema(t reflect.Type, defs map[string]Schema, seen map[reflect.Type]string) Schema {
	props := map[string]any{}
	var required []string

	for i := range t.NumField() {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		name, opts := jsonName(f)
		if name == "-" {
			continue
		}
		if f.Anonymous && name == "" {
			// Embedded struct: its fields are inlined by encoding/json.
			inner := structSchema(f.Type, defs, seen)
			if p, ok := inner["properties"].(map[string]any); ok {
				for k, v := range p {
					props[k] = v
				}
			}
			if r, ok := inner["required"].([]string); ok {
				required = append(required, r...)
			}
			continue
		}
		if name == "" {
			name = f.Name
		}

		props[name] = schemaFor(f.Type, defs, seen)
		// A field is required unless it can be absent from the output.
		if !opts.omitempty && !opts.omitzero && f.Type.Kind() != reflect.Pointer {
			required = append(required, name)
		}
	}

	s := Schema{"type": "object", "properties": props}
	if len(required) > 0 {
		s["required"] = required
	}
	return s
}

type jsonOpts struct {
	omitempty bool
	omitzero  bool
	str       bool
}

func jsonName(f reflect.StructField) (string, jsonOpts) {
	tag, ok := f.Tag.Lookup("json")
	if !ok {
		return f.Name, jsonOpts{}
	}
	parts := strings.Split(tag, ",")
	var o jsonOpts
	for _, p := range parts[1:] {
		switch p {
		case "omitempty":
			o.omitempty = true
		case "omitzero":
			o.omitzero = true
		case "string":
			o.str = true
		}
	}
	name := parts[0]
	if name == "" && !f.Anonymous {
		name = f.Name
	}
	return name, o
}

func typeOf(sample any) reflect.Type {
	if sample == nil {
		return nil
	}
	return reflect.TypeOf(sample)
}
