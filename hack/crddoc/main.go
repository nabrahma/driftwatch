// Command crddoc reports every CRD property that has no description.
//
// `kubectl explain driftcheck.spec.policy.settlementWindow` renders whatever is
// in the schema's description, and an empty one means an operator has to read
// the Go source to configure the field. A description on every field is a
// release gate, and a gate nothing enforces decays: a
// field added in six months would get no comment and nobody would notice until
// someone ran kubectl explain on it.
//
// So this walks the generated schema and fails if anything is undocumented.
// hack/verify-crd-docs.sh runs it in CI.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"sigs.k8s.io/yaml"
)

// schema is the subset of an OpenAPI v3 schema that carries documentation.
type schema struct {
	Description string             `json:"description"`
	Type        string             `json:"type"`
	Properties  map[string]*schema `json:"properties"`
	Items       *schema            `json:"items"`
}

type crd struct {
	Spec struct {
		Names struct {
			Kind string `json:"kind"`
		} `json:"names"`
		Versions []struct {
			Name   string `json:"name"`
			Schema struct {
				OpenAPIV3Schema *schema `json:"openAPIV3Schema"`
			} `json:"schema"`
		} `json:"versions"`
	} `json:"spec"`
}

// exempt are the paths whose descriptions come from Kubernetes itself, or where
// controller-gen omits one by design.
//
// metadata is the embedded ObjectMeta: its schema is generated with
// generateEmbeddedObjectMeta=false precisely so the CRD does not carry a second
// copy of the core API's documentation. Requiring a description there would be
// asking driftwatch to document a type it does not own.
var exempt = map[string]bool{
	"metadata": true,
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: crddoc <crd.yaml>...")
		os.Exit(2)
	}

	var undocumented []string
	total := 0

	for _, path := range os.Args[1:] {
		raw, err := os.ReadFile(path) //nolint:gosec // a path this tool was pointed at
		if err != nil {
			fmt.Fprintf(os.Stderr, "crddoc: %v\n", err)
			os.Exit(2)
		}

		var doc crd
		if err := yaml.Unmarshal(raw, &doc); err != nil {
			fmt.Fprintf(os.Stderr, "crddoc: %s: %v\n", path, err)
			os.Exit(2)
		}

		for _, version := range doc.Spec.Versions {
			root := strings.ToLower(doc.Spec.Names.Kind)
			missing, count := walk(root, version.Schema.OpenAPIV3Schema)
			undocumented = append(undocumented, missing...)
			total += count

			fmt.Printf("%s %s/%s: %d properties\n",
				filepath.Base(path), doc.Spec.Names.Kind, version.Name, count)
		}
	}

	if len(undocumented) == 0 {
		fmt.Printf("\nall %d properties documented\n", total)
		return
	}

	sort.Strings(undocumented)
	fmt.Fprintf(os.Stderr, "\n%d of %d properties have no description:\n",
		len(undocumented), total)
	for _, path := range undocumented {
		fmt.Fprintf(os.Stderr, "  %s\n", path)
	}
	fmt.Fprintln(os.Stderr,
		"\nAdd a doc comment to the Go field: kubectl explain renders it verbatim.")
	os.Exit(1)
}

// walk returns the undocumented paths beneath a schema, and how many properties
// it visited.
func walk(path string, s *schema) (missing []string, count int) {
	if s == nil {
		return nil, 0
	}

	if s.Items != nil {
		sub, n := walk(path+"[]", s.Items)
		missing, count = append(missing, sub...), count+n
	}

	names := make([]string, 0, len(s.Properties))
	for name := range s.Properties {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		child := s.Properties[name]
		childPath := path + "." + name
		count++

		if exempt[name] {
			continue
		}
		if strings.TrimSpace(child.Description) == "" {
			missing = append(missing, childPath)
			continue
		}

		sub, n := walk(childPath, child)
		missing, count = append(missing, sub...), count+n
	}
	return missing, count
}
