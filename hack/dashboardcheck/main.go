// Command dashboardcheck verifies the Grafana dashboard against the metric
// registry.
//
// A panel querying a metric that no longer exists does not fail. It renders an
// empty graph, indefinitely, and looks exactly like a metric that is legitimately
// at zero — which on this dashboard is the *good* value for half the panels. A
// renamed metric would leave the divergence panel showing a reassuring flat line
// forever.
//
// So this extracts every driftwatch_* identifier from the dashboard JSON and
// asserts each one is registered. Histogram suffixes are resolved back to their
// base metric, since _bucket and _count are Prometheus's, not driftwatch's.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"

	"github.com/nabrahma/driftwatch/pkg/metrics"
)

// metricRef matches a driftwatch metric name wherever it appears in the JSON —
// inside a PromQL expression, a legend, or a variable definition.
var metricRef = regexp.MustCompile(`driftwatch_[a-z0-9_]+`)

// histogramSuffixes are appended by Prometheus rather than declared by
// driftwatch, so a reference to one is a reference to the base metric.
var histogramSuffixes = []string{"_bucket", "_count", "_sum"}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: dashboardcheck <dashboard.json>...")
		os.Exit(2)
	}

	registered := map[string]bool{}
	for _, name := range metrics.Names() {
		registered[name] = true
	}

	var problems []string
	total := 0

	for _, path := range os.Args[1:] {
		raw, err := os.ReadFile(path) //nolint:gosec // a path this tool was pointed at
		if err != nil {
			fmt.Fprintf(os.Stderr, "dashboardcheck: %v\n", err)
			os.Exit(2)
		}

		// Parsed as well as scanned: a dashboard that is not valid JSON will not
		// import, and finding that out here is much cheaper than finding it out
		// from Grafana's "Dashboard title cannot be empty" style of error.
		var doc map[string]any
		if err := json.Unmarshal(raw, &doc); err != nil {
			fmt.Fprintf(os.Stderr, "dashboardcheck: %s is not valid JSON: %v\n", path, err)
			os.Exit(1)
		}

		if err := checkStructure(doc); err != nil {
			problems = append(problems, fmt.Sprintf("%s: %v", path, err))
		}

		seen := map[string]bool{}
		for _, ref := range metricRef.FindAllString(string(raw), -1) {
			seen[base(ref)] = true
		}

		names := sortedKeys(seen)
		total += len(names)

		for _, name := range names {
			if !registered[name] {
				problems = append(problems, fmt.Sprintf(
					"%s: panel queries %s, which is not in the registry", path, name))
			}
		}

		fmt.Printf("%s: %d distinct metrics referenced\n", path, len(names))
	}

	if len(problems) > 0 {
		fmt.Fprintf(os.Stderr, "\n%d problem(s):\n", len(problems))
		for _, p := range problems {
			fmt.Fprintf(os.Stderr, "  %s\n", p)
		}
		fmt.Fprintln(os.Stderr,
			"\nA panel querying a metric that does not exist renders an empty graph "+
				"forever, which on this dashboard looks identical to healthy.")
		os.Exit(1)
	}

	fmt.Printf("\nall %d metric references resolve to registered metrics\n", total)
}

// checkStructure asserts the parts that make the dashboard importable and
// templated, which §12.1 requires by name.
func checkStructure(doc map[string]any) error {
	// Every assertion below reads a missing or wrongly-typed field as absent,
	// which is exactly the condition being reported. The JSON parse in main has
	// already established the document is well formed.
	//nolint:errcheck // see above
	templating, _ := doc["templating"].(map[string]any)
	list, _ := templating["list"].([]any) //nolint:errcheck // see above

	var haveDatasource, haveCheck bool
	for _, item := range list {
		v, _ := item.(map[string]any) //nolint:errcheck // a nil map reads as absent, which is the check
		switch v["name"] {
		case "DS_PROMETHEUS":
			haveDatasource = v["type"] == "datasource"
		case "check":
			haveCheck = v["type"] == "query"
		}
	}

	if !haveDatasource {
		return fmt.Errorf("no DS_PROMETHEUS datasource variable; the dashboard " +
			"will not import into a Grafana whose datasource has a different uid")
	}
	if !haveCheck {
		return fmt.Errorf("no `check` query variable; every panel would show " +
			"every check's series summed together")
	}

	panels, _ := doc["panels"].([]any) //nolint:errcheck // a malformed dashboard is caught by the JSON parse above

	rows := 0
	for _, p := range panels {
		v, _ := p.(map[string]any) //nolint:errcheck // ditto
		if v["type"] == "row" {
			rows++
		}
	}
	if rows != 5 {
		return fmt.Errorf("found %d rows, §12.1 specifies 5", rows)
	}

	// The two panels §12.1 singles out. Both are load-bearing rather than
	// decorative — one stops the dashboard overstating its own verdict, the
	// other is the entire false-positive argument in one picture — and both are
	// exactly the kind of thing a well-meaning tidy-up deletes.
	body := fmt.Sprint(doc)
	for _, required := range []struct{ query, why string }{
		{
			"driftwatch_coverage_ratio",
			"the coverage panel is what stops a zero-divergence dashboard from lying",
		},
		{
			"driftwatch_settlement_window_seconds",
			"the settlement-window overlay is what shows W sitting above p99",
		},
	} {
		if !strings.Contains(body, required.query) {
			return fmt.Errorf("%s is not queried anywhere: %s", required.query, required.why)
		}
	}
	return nil
}

func base(name string) string {
	for _, suffix := range histogramSuffixes {
		if strings.HasSuffix(name, suffix) {
			return strings.TrimSuffix(name, suffix)
		}
	}
	return name
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
