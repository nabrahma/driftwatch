# Metrics

_Not yet written. Generated in Phase 5 from `pkg/metrics`._

This file is generated, and `hack/verify-metrics-docs.sh` fails CI when it drifts
from the metrics actually declared in `pkg/metrics`. Do not edit it by hand.

Will list, per PRD §12 and §21.5: every exported metric, its type, its labels,
and its cardinality bound.

The cardinality bound is not decoration. PRD §23 A3: a single metric labelled
with a key name turns driftwatch into a cardinality bomb that takes down the
monitoring system it exists to inform.
