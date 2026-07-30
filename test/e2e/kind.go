// Package e2e holds the Kind-based end-to-end suite (PRD §14).
//
// The test files in this package are behind the e2e build tag so that they
// never run in the unit suite; the helpers below are not, so that the package
// still builds under ./... .
package e2e
