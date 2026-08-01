package v1alpha1

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/nabrahma/driftwatch/pkg/check"
)

// The webhook is a thin adapter over pkg/check's validation, not a second
// implementation of it.
//
// Every rule in §10.2 is a property of a check specification rather than of
// Kubernetes, and pkg/check already enforces all of them because the CLI has to
// reject the same files without a cluster. Writing them again here would give
// two rule sets that eventually disagree, and which one an operator hit would
// depend on whether they ran `driftwatch watch -f` or `kubectl apply`. So this
// file converts, delegates, and translates the result into the field-path form
// the API server renders — and adds only the two things that genuinely have no
// meaning outside the API: the string-encoded decimals, and the immutability
// comparison against a stored object.
//
// One rule from the §10.2 table is deliberately absent: secret references are
// checked by the controller, not here, and §10.2 says so. A webhook that read
// other objects would reject a DriftCheck applied a second before its Secret in
// the same manifest, and would be wrong again the moment the Secret was
// rotated. The controller reports it as a condition instead, where it can
// recover on its own.

// +kubebuilder:webhook:path=/mutate-driftwatch-io-v1alpha1-driftcheck,mutating=true,failurePolicy=fail,sideEffects=None,groups=driftwatch.io,resources=driftchecks,verbs=create;update,versions=v1alpha1,name=mdriftcheck.kb.io,admissionReviewVersions=v1
// +kubebuilder:webhook:path=/validate-driftwatch-io-v1alpha1-driftcheck,mutating=false,failurePolicy=fail,sideEffects=None,groups=driftwatch.io,resources=driftchecks,verbs=create;update,versions=v1alpha1,name=vdriftcheck.kb.io,admissionReviewVersions=v1

// SetupWebhookWithManager registers the defaulting and validating webhooks.
func SetupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(&DriftCheck{}).
		WithDefaulter(&DriftCheckDefaulter{}).
		WithValidator(&DriftCheckValidator{}).
		Complete()
}

// DriftCheckDefaulter fills every optional field on admission.
//
// +kubebuilder:object:generate=false
type DriftCheckDefaulter struct{}

var _ webhook.CustomDefaulter = &DriftCheckDefaulter{}

// Default applies §10.1's defaults to an incoming DriftCheck.
func (d *DriftCheckDefaulter) Default(ctx context.Context, obj runtime.Object) error {
	dc, ok := obj.(*DriftCheck)
	if !ok {
		return fmt.Errorf("expected a DriftCheck, got %T", obj)
	}

	dc.Spec.Default()
	logf.FromContext(ctx).V(1).Info("defaulted DriftCheck",
		"name", dc.Name, "namespace", dc.Namespace)
	return nil
}

// DriftCheckValidator enforces §10.2 on admission.
//
// Excluded from deepcopy generation, like the defaulter above: the package
// marker generates DeepCopy for every type in it, and an admission handler with
// no state is not an API object. The methods would compile and never be called.
//
// +kubebuilder:object:generate=false
type DriftCheckValidator struct{}

var _ webhook.CustomValidator = &DriftCheckValidator{}

// ValidateCreate rejects a specification that could not run.
func (v *DriftCheckValidator) ValidateCreate(
	_ context.Context, obj runtime.Object,
) (admission.Warnings, error) {
	dc, ok := obj.(*DriftCheck)
	if !ok {
		return nil, fmt.Errorf("expected a DriftCheck, got %T", obj)
	}
	return dc.Validate(nil)
}

// ValidateUpdate additionally enforces the immutable fields.
func (v *DriftCheckValidator) ValidateUpdate(
	_ context.Context, oldObj, newObj runtime.Object,
) (admission.Warnings, error) {
	dc, ok := newObj.(*DriftCheck)
	if !ok {
		return nil, fmt.Errorf("expected a DriftCheck, got %T", newObj)
	}
	previous, ok := oldObj.(*DriftCheck)
	if !ok {
		return nil, fmt.Errorf("expected a DriftCheck, got %T", oldObj)
	}
	return dc.Validate(previous)
}

// ValidateDelete allows every deletion.
//
// Refusing one would leave an operator unable to remove a check whose
// configuration has become invalid, which is exactly when they most want to.
func (v *DriftCheckValidator) ValidateDelete(
	_ context.Context, _ runtime.Object,
) (admission.Warnings, error) {
	return nil, nil
}

// Validate checks one DriftCheck, optionally against the version it replaces.
//
// It is exported and takes plain objects so that every rule in §10.2 is
// testable without an admission request, an API server, or a manager.
func (dc *DriftCheck) Validate(previous *DriftCheck) (admission.Warnings, error) {
	spec := dc.Spec.ToCheckSpec(dc.Name, dc.Namespace)

	errs := decimalErrors(&dc.Spec)
	errs = append(errs, translate(spec.Validate())...)

	if previous != nil {
		old := previous.Spec.ToCheckSpec(previous.Name, previous.Namespace)
		errs = append(errs, translate(spec.ValidateUpdate(&old))...)
	}

	warnings := admission.Warnings(spec.Warnings())

	if len(errs) == 0 {
		return warnings, nil
	}
	return warnings, apierrors.NewInvalid(
		GroupVersion.WithKind("DriftCheck").GroupKind(), dc.Name, errs)
}

var specPath = field.NewPath("spec")

// decimalErrors reports the fields the CRD carries as strings and driftwatch
// reads as numbers.
//
// Kubernetes API convention avoids floats in schemas, so safetyFactor is a
// string and nothing before this point has checked that it is a number at all.
// Without this a value of "3.O" would convert to zero and then be reported as
// violating the ">= 1.0" bound, which sends the operator looking at a number
// they never wrote.
func decimalErrors(spec *DriftCheckSpec) field.ErrorList {
	var errs field.ErrorList

	decimals := []struct {
		path *field.Path
		raw  string
	}{
		{
			specPath.Child("policy", "settlementWindow", "safetyFactor"),
			spec.Policy.SettlementWindow.SafetyFactor,
		},
		{
			specPath.Child("alert", "divergentRatioThreshold"),
			spec.Alert.DivergentRatioThreshold,
		},
	}

	for _, d := range decimals {
		if _, err := check.ParseDecimal(d.raw); err != nil {
			errs = append(errs, field.Invalid(d.path, d.raw, err.Error()))
		}
	}
	return errs
}

// translate turns pkg/check's field errors into the API's.
//
// The message is carried across verbatim. It is the same sentence the CLI
// prints for the same mistake, which means searching for it finds one answer
// rather than two half-answers.
func translate(err error) field.ErrorList {
	if err == nil {
		return nil
	}

	var ve *check.ValidationError
	if !errors.As(err, &ve) {
		return field.ErrorList{field.InternalError(specPath, err)}
	}

	errs := make(field.ErrorList, 0, len(ve.Errors))
	for _, fe := range ve.Errors {
		errs = append(errs, field.Invalid(parsePath(fe.Field), "", fe.Message))
	}
	return errs
}

// parsePath turns pkg/check's dotted field name into an API field path.
//
// It has to handle both subscript forms, because pkg/check produces both:
// "source.zmq.endpoints[0]" is a slice index and `codec.opMapping["BLOCK_STORED"]`
// is a map key. Dropping the subscript would be the quiet kind of wrong — the
// error would name the whole opMapping rather than the one entry in it, on
// exactly the spec where the operator has typed a dozen of them.
func parsePath(dotted string) *field.Path {
	path := specPath

	for _, segment := range strings.Split(dotted, ".") {
		name, subscript, hasSubscript := strings.Cut(segment, "[")
		path = path.Child(name)

		if !hasSubscript {
			continue
		}

		subscript = strings.TrimSuffix(subscript, "]")
		if i, err := strconv.Atoi(subscript); err == nil {
			path = path.Index(i)
			continue
		}
		path = path.Key(subscript)
	}
	return path
}
