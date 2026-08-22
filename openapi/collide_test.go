package openapi_test

import (
	"encoding/json"
	"testing"

	"github.com/imlargo/sse"
	"github.com/imlargo/sse/openapi"
	other "github.com/imlargo/sse/openapi/internal/othertickets"
)

type LocalTicket struct {
	Title string `json:"title"`
}

// Two different types that happen to share a name must not collapse into one
// component. The document would describe the wrong shape for one of the two
// events, and a client generated from it would be wrong in a way nothing here
// or downstream would catch.
func TestSameTypeNameFromTwoPackages(t *testing.T) {
	cat := sse.NewCatalog(
		sse.Declare[LocalTicket]("a"),
		sse.Declare[other.LocalTicket]("b"),
	)
	doc, err := openapi.Generate(openapi.Options{OmitReservedEvents: true},
		openapi.Stream{Path: "/e", Catalog: cat})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := doc.JSON()
	var m map[string]any
	json.Unmarshal(raw, &m)
	schemas := m["components"].(map[string]any)["schemas"].(map[string]any)
	t.Logf("schemas: %v", keysOf(schemas))
	if len(schemas) != 3 {
		t.Fatalf("two distinct types sharing a name produced %v; one of the two "+
			"events is described by the wrong schema", keysOf(schemas))
	}
	// Whatever they are called, the two shapes must both be present and distinct.
	var sawTitle, sawReference bool
	for _, v := range schemas {
		props, ok := v.(map[string]any)["properties"].(map[string]any)
		if !ok {
			continue
		}
		if _, ok := props["title"]; ok {
			sawTitle = true
		}
		if _, ok := props["reference"]; ok {
			sawReference = true
		}
	}
	if !sawTitle || !sawReference {
		t.Errorf("the two shapes did not both survive: %v", keysOf(schemas))
	}
	t.Logf("schemas kept distinct: %v", keysOf(schemas))
}

func keysOf(m map[string]any) []string {
	var o []string
	for k := range m {
		o = append(o, k)
	}
	return o
}
