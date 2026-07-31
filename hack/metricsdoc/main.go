// Command metricsdoc prints docs/METRICS.md from the metric registry.
//
// It exists so that the documentation cannot drift from the code: the metric
// names, help text and label enums all come from pkg/metrics.Defs(), and
// hack/verify-metrics-docs.sh fails CI when the committed file no longer
// matches what this prints.
package main

import (
	"fmt"
	"os"

	"github.com/nabrahma/driftwatch/pkg/metrics"
)

func main() {
	if _, err := fmt.Fprint(os.Stdout, metrics.Markdown()); err != nil {
		fmt.Fprintln(os.Stderr, "metricsdoc:", err)
		os.Exit(1)
	}
}
