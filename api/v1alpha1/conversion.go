package v1alpha1

import (
	"github.com/nabrahma/driftwatch/pkg/check"
)

// The CRD and check.Spec are the same schema in two dialects: json tags with
// kubebuilder markers here, yaml tags there. §11 makes that deliberate — a YAML
// file the CLI runs must be the file that goes into the cluster unchanged.
//
// The translation lives here rather than the validation moving into the API
// package, because every rule in §10.2 already has an implementation in
// pkg/check that the CLI enforces. Reimplementing them for the webhook would
// give two rule sets that drift apart, and the one an operator hits first
// depends on how they deployed. So the webhook converts and delegates, and the
// only thing that can differ between `driftwatch watch -f` and `kubectl apply`
// is the shape of the error message, not the verdict.

// ToCheckSpec renders the CRD spec as the runtime spec, with defaults applied.
//
// Secrets are not resolved here: a secret ref becomes a ref, and the controller
// substitutes the value once it has read it. That keeps a resolved password out
// of everything that renders a spec, which includes the startup log line §12.3
// asks for.
func (in *DriftCheckSpec) ToCheckSpec(name, namespace string) check.Spec {
	out := check.Spec{
		Name:      name,
		Namespace: namespace,
		Source: check.SourceSpec{
			Type:             in.Source.Type,
			IngestBufferSize: in.Source.IngestBufferSize,
		},
		Codec: check.CodecSpec{
			Type:            in.Codec.Type,
			MaxPayloadBytes: in.Codec.MaxPayloadBytes,
			RetainRaw:       in.Codec.RetainRaw,
			FieldMapping:    copyStringMap(in.Codec.FieldMapping),
			OpMapping:       copyStringMap(in.Codec.OpMapping),
		},
		Projection: check.ProjectionSpec{
			Type:             in.Projection.Type,
			KeyTemplate:      in.Projection.KeyTemplate,
			MemberTemplate:   in.Projection.MemberTemplate,
			MaxMembersPerKey: in.Projection.MaxMembersPerKey,
			IncrOnly:         in.Projection.IncrOnly,
		},
		Target: check.TargetSpec{
			Type: in.Target.Type,
		},
		Policy: check.PolicySpec{
			SettlementWindow: check.SettlementWindowSpec{
				Mode:         in.Policy.SettlementWindow.Mode,
				Static:       check.Duration(in.Policy.SettlementWindow.Static.Duration),
				Min:          check.Duration(in.Policy.SettlementWindow.Min.Duration),
				Max:          check.Duration(in.Policy.SettlementWindow.Max.Duration),
				SafetyFactor: parseDecimal(in.Policy.SettlementWindow.SafetyFactor),
			},
			SweepInterval:         check.Duration(in.Policy.SweepInterval.Duration),
			ExtraScanInterval:     check.Duration(in.Policy.ExtraScanInterval.Duration),
			Bootstrap:             in.Policy.Bootstrap,
			ExpiryPolicy:          in.Policy.ExpiryPolicy,
			AssumedTTL:            check.Duration(in.Policy.AssumedTTL.Duration),
			TTLTolerance:          check.Duration(in.Policy.TTLTolerance.Duration),
			RequirePrimary:        in.Policy.RequirePrimary,
			ReorderWindow:         check.Duration(in.Policy.ReorderWindow.Duration),
			MaxTrackedKeys:        in.Policy.MaxTrackedKeys,
			RingSize:              in.Policy.RingSize,
			MaxConfirmQueue:       in.Policy.MaxConfirmQueue,
			MaxFindings:           in.Policy.MaxFindings,
			MaxExtrasTracked:      in.Policy.MaxExtrasTracked,
			MaxPublishers:         in.Policy.MaxPublishers,
			OracleShards:          in.Policy.OracleShards,
			NeverSettledThreshold: in.Policy.NeverSettledThreshold,
			Paused:                in.Policy.Paused,
		},
		Alert: check.AlertSpec{
			DivergentKeysThreshold:  in.Alert.DivergentKeysThreshold,
			DivergentRatioThreshold: parseDecimal(in.Alert.DivergentRatioThreshold),
			ForDuration:             check.Duration(in.Alert.ForDuration.Duration),
			IncludeSuspect:          in.Alert.IncludeSuspect,
		},
	}

	if o := in.Projection.Ownership; o != nil {
		out.Projection.Ownership = &check.OwnershipSpec{
			Partitioned: o.Partitioned,
			KeyPattern:  o.KeyPattern,
		}
	}

	in.convertSource(&out)
	in.convertTarget(&out)

	out.ApplyDefaults()
	return out
}

func (in *DriftCheckSpec) convertSource(out *check.Spec) {
	if z := in.Source.ZMQ; z != nil {
		out.Source.ZMQ = &check.ZMQSpec{
			Endpoints:            append([]string(nil), z.Endpoints...),
			Topics:               append([]string(nil), z.Topics...),
			RecvHWM:              z.RecvHWM,
			ConnectTimeout:       check.Duration(z.ConnectTimeout.Duration),
			ReconnectIntervalMax: check.Duration(z.ReconnectIntervalMax.Duration),
			IdleTimeout:          check.Duration(z.IdleTimeout.Duration),
			Multipart:            z.Multipart,
		}
	}
	if n := in.Source.NATS; n != nil {
		out.Source.NATS = &check.NATSSpec{
			URL:                  n.URL,
			Subjects:             append([]string(nil), n.Subjects...),
			QueueGroup:           n.QueueGroup,
			CredentialsSecretRef: toSecretRef(n.CredentialsSecretRef),
		}
	}
	if f := in.Source.File; f != nil {
		out.Source.File = &check.FileSpec{Path: f.Path, Speed: f.Speed, Loop: f.Loop}
	}
}

func (in *DriftCheckSpec) convertTarget(out *check.Spec) {
	r := in.Target.Redis
	if r == nil {
		return
	}

	out.Target.Redis = &check.RedisSpec{
		Mode:              r.Mode,
		Addr:              r.Addr,
		Addrs:             append([]string(nil), r.Addrs...),
		MasterName:        r.MasterName,
		DB:                r.DB,
		Username:          r.Username,
		PasswordSecretRef: toSecretRef(r.PasswordSecretRef),
		KeyPattern:        r.KeyPattern,
		ReadBatchSize:     r.ReadBatchSize,
		ScanCount:         r.ScanCount,
		DialTimeout:       check.Duration(r.DialTimeout.Duration),
		ReadTimeout:       check.Duration(r.ReadTimeout.Duration),
		PoolSize:          r.PoolSize,
	}

	if t := r.TLS; t != nil {
		out.Target.Redis.TLS = &check.TLSSpec{
			Enabled:            t.Enabled,
			InsecureSkipVerify: t.InsecureSkipVerify,
			CASecretRef:        toSecretRef(t.CASecretRef),
		}
	}
}

// SecretRefs returns every secret this spec depends on, so the controller can
// resolve them without knowing where in the spec they live.
func (in *DriftCheckSpec) SecretRefs() map[string]*SecretKeyRef {
	out := map[string]*SecretKeyRef{}

	if r := in.Target.Redis; r != nil {
		if r.PasswordSecretRef != nil {
			out["target.redis.passwordSecretRef"] = r.PasswordSecretRef
		}
		if r.TLS != nil && r.TLS.CASecretRef != nil {
			out["target.redis.tls.caSecretRef"] = r.TLS.CASecretRef
		}
	}
	if n := in.Source.NATS; n != nil && n.CredentialsSecretRef != nil {
		out["source.nats.credentialsSecretRef"] = n.CredentialsSecretRef
	}
	return out
}

func toSecretRef(in *SecretKeyRef) *check.SecretRef {
	if in == nil {
		return nil
	}
	return &check.SecretRef{Name: in.Name, Key: in.Key}
}

func copyStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// parseDecimal reads a fractional value the CRD spells as a string.
//
// An unparseable value becomes zero rather than an error, which is what makes
// the validation message good: zero fails the "must be >= 1.0" rule with the
// operator's own text quoted back, instead of a type error from a layer that
// does not know what the field means.
func parseDecimal(raw string) check.Decimal {
	d, err := check.ParseDecimal(raw)
	if err != nil {
		return 0
	}
	return d
}
