package urlnorm

import (
	"fmt"
	"net"
	"net/url"
	"sort"
	"strings"
)

// defaultTrackingParams is the built-in set of query parameter names that do
// not change the returned content and are therefore dropped during
// normalization. Names are matched case-insensitively.
var defaultTrackingParams = []string{
	"utm_source",
	"utm_medium",
	"utm_campaign",
	"utm_term",
	"utm_content",
	"utm_id",
	"utm_name",
	"utm_cid",
	"utm_reader",
	"gclid",
	"gbraid",
	"wbraid",
	"fbclid",
	"msclkid",
	"dclid",
	"yclid",
	"mc_cid",
	"mc_eid",
	"igshid",
	"ref",
	"referrer",
	"referral",
	"source",
	"spm",
	"_hsenc",
	"_hsmi",
	"hsctatracking",
	"vero_conv",
	"vero_id",
	"soc_src",
	"soc_trk",
	"twclid",
	"li_fat_id",
	"oly_anon_id",
	"oly_enc_id",
	"vgo_ee",
}

var defaultPorts = map[string]string{
	"http":  "80",
	"https": "443",
}

func normalize(rawURL string, trackingParams []string) (string, error) {
	if strings.TrimSpace(rawURL) == "" {
		return "", fmt.Errorf("urlnorm: empty url")
	}

	u, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("urlnorm: parse %q: %w", rawURL, err)
	}

	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("urlnorm: unsupported scheme %q in %q", u.Scheme, rawURL)
	}

	if u.Host == "" {
		return "", fmt.Errorf("urlnorm: missing host in %q", rawURL)
	}

	host := strings.ToLower(u.Hostname())

	if net.ParseIP(host) == nil {
		// host names are case-insensitive; an IP is already canonical
		host = strings.ToLower(host)
	}

	port := u.Port()
	if port != "" && defaultPorts[u.Scheme] == port {
		port = ""
	}

	var hostPort string
	if port != "" {
		hostPort = net.JoinHostPort(host, port)
	} else {
		hostPort = host
	}

	path := normalizePath(u.EscapedPath())

	query := normalizeQuery(u.RawQuery, trackingParams)

	var b strings.Builder

	b.WriteString(u.Scheme)
	b.WriteString("://")
	b.WriteString(hostPort)
	b.WriteString(path)

	if query != "" {
		b.WriteByte('?')
		b.WriteString(query)
	}

	return b.String(), nil
}

// normalizePath lower-cases nothing in the path (path case may be
// content-significant) but removes a single trailing slash on a non-root
// path.
func normalizePath(escapedPath string) string {
	if escapedPath == "" || escapedPath == "/" {
		return "/"
	}

	if strings.HasSuffix(escapedPath, "/") {
		return strings.TrimSuffix(escapedPath, "/")
	}

	return escapedPath
}

// normalizeQuery drops tracking parameters, keeps every content parameter and
// renders the surviving keys in sorted order with stable value ordering.
func normalizeQuery(rawQuery string, trackingParams []string) string {
	if rawQuery == "" {
		return ""
	}

	values, err := url.ParseQuery(rawQuery)
	if err != nil {
		// Unparseable query strings are preserved verbatim rather than
		// silently changing the request shape.
		return rawQuery
	}

	keys := make([]string, 0, len(values))

	for key := range values {
		if isTrackingParam(key, trackingParams) {
			continue
		}

		keys = append(keys, key)
	}

	sort.Strings(keys)

	parts := make([]string, 0, len(keys))

	for _, key := range keys {
		vals := values[key]
		sortedVals := append([]string(nil), vals...)
		sort.Strings(sortedVals)

		for _, v := range sortedVals {
			parts = append(parts, url.Values{key: []string{v}}.Encode())
		}
	}

	return strings.Join(parts, "&")
}

func isTrackingParam(name string, trackingParams []string) bool {
	for _, candidate := range trackingParams {
		if strings.EqualFold(name, candidate) {
			return true
		}
	}

	return false
}
