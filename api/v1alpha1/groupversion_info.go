// Package v1alpha1 contains the DriftCheck API types (§10).
//
// +kubebuilder:object:generate=true
// +groupName=driftwatch.io
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	// GroupVersion is the group and version this package registers.
	GroupVersion = schema.GroupVersion{Group: "driftwatch.io", Version: "v1alpha1"}

	// SchemeBuilder collects the types in this package for registration.
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}

	// AddToScheme adds this package's types to a runtime scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)

// Resource maps an unqualified resource name into this group, for the error
// types that want a GroupResource.
func Resource(resource string) schema.GroupResource {
	return GroupVersion.WithResource(resource).GroupResource()
}
