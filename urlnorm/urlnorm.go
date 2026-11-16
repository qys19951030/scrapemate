// Package urlnorm normalizes URLs so that links that identify the same
// resource collapse into a single canonical representation.
//
// Normalization ignores fragments, tracking parameters, host name case and
// trailing slashes, while preserving every query parameter that can affect the
// returned content.
package urlnorm

// Normalize returns the canonical form of rawURL.
//
// It applies the following rules:
//   - the scheme and host are lower-cased;
//   - the default port for the scheme is removed;
//   - the fragment is dropped;
//   - known tracking parameters are dropped, all other query parameters are
//     preserved and rendered in sorted order;
//   - a trailing slash on a non-root path is removed.
//
// An error is returned when rawURL cannot be parsed or does not contain an
// absolute http/https URL.
func Normalize(rawURL string) (string, error) {
	return normalize(rawURL, defaultTrackingParams)
}

// NormalizeWithTrackingParams works like Normalize but allows the caller to
// extend the set of query parameter names that are treated as tracking
// parameters. The supplied names are used in addition to the built-in set;
// parameter names are matched case-insensitively.
func NormalizeWithTrackingParams(rawURL string, trackingParams []string) (string, error) {
	merged := make([]string, 0, len(defaultTrackingParams)+len(trackingParams))
	merged = append(merged, defaultTrackingParams...)
	merged = append(merged, trackingParams...)

	return normalize(rawURL, merged)
}

// IsTrackingParam reports whether name is a known tracking query parameter.
// The comparison is case-insensitive.
func IsTrackingParam(name string) bool {
	return isTrackingParam(name, defaultTrackingParams)
}
