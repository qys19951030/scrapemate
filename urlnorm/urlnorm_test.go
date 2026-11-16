package urlnorm_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gosom/scrapemate/urlnorm"
)

func TestNormalize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "drops fragment",
			in:   "https://Example.COM/path#section",
			want: "https://example.com/path",
		},
		{
			name: "lowercases host but keeps path case",
			in:   "HTTPS://EXAMPLE.com/SomePath",
			want: "https://example.com/SomePath",
		},
		{
			name: "removes trailing slash on non-root path",
			in:   "https://example.com/path/",
			want: "https://example.com/path",
		},
		{
			name: "keeps root slash",
			in:   "https://example.com/",
			want: "https://example.com/",
		},
		{
			name: "strips default http port",
			in:   "http://example.com:80/path",
			want: "http://example.com/path",
		},
		{
			name: "strips default https port",
			in:   "https://example.com:443/path",
			want: "https://example.com/path",
		},
		{
			name: "keeps non default port",
			in:   "https://example.com:8443/path",
			want: "https://example.com:8443/path",
		},
		{
			name: "drops tracking params but keeps content params",
			in:   "https://example.com/search?q=golang&page=2&utm_source=newsletter&fbclid=abc",
			want: "https://example.com/search?page=2&q=golang",
		},
		{
			name: "tracking param matching is case insensitive",
			in:   "https://example.com/p?UTM_MEDIUM=email&id=7",
			want: "https://example.com/p?id=7",
		},
		{
			name: "sorts query params deterministically",
			in:   "https://example.com/p?b=2&a=1&c=3",
			want: "https://example.com/p?a=1&b=2&c=3",
		},
		{
			name: "repeated params keep all values sorted",
			in:   "https://example.com/p?tag=zebra&tag=apple",
			want: "https://example.com/p?tag=apple&tag=zebra",
		},
		{
			name: "equivalent urls collapse to same canonical form",
			in:   "https://Example.com:443/a/b/?utm_campaign=x#frag",
			want: "https://example.com/a/b",
		},
		{
			name: "ip host is accepted",
			in:   "http://127.0.0.1:8080/health",
			want: "http://127.0.0.1:8080/health",
		},
		{
			name: "query order and fragment do not matter",
			in:   "http://example.com/list?page=1&sort=asc#top",
			want: "http://example.com/list?page=1&sort=asc",
		},
	}

	for _, tc := range tests {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := urlnorm.Normalize(tc.in)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestNormalize_Equivalence(t *testing.T) {
	t.Parallel()

	variants := []string{
		"https://Shop.Example.com/products/123/?utm_source=google&utm_medium=cpc",
		"https://shop.example.com/products/123?utm_medium=cpc&utm_source=google",
		"HTTPS://SHOP.EXAMPLE.COM/products/123#reviews",
		"https://shop.example.com:443/products/123/",
	}

	canonical, err := urlnorm.Normalize(variants[0])
	require.NoError(t, err)

	for _, v := range variants[1:] {
		got, err := urlnorm.Normalize(v)
		require.NoError(t, err)
		require.Equal(t, canonical, got, "variant %q must normalize to %s", v, canonical)
	}
}

func TestNormalize_ContentParamsSurvive(t *testing.T) {
	t.Parallel()

	a, err := urlnorm.Normalize("https://example.com/p?id=1")
	require.NoError(t, err)

	b, err := urlnorm.Normalize("https://example.com/p?id=2")
	require.NoError(t, err)

	require.NotEqual(t, a, b, "content-affecting query params must be preserved")
	require.Contains(t, a, "id=1")
}

func TestNormalizeWithTrackingParams(t *testing.T) {
	t.Parallel()

	got, err := urlnorm.NormalizeWithTrackingParams(
		"https://example.com/p?session=xyz&q=go",
		[]string{"session"},
	)
	require.NoError(t, err)
	require.Equal(t, "https://example.com/p?q=go", got)

	// default tracking params still apply when a custom list is supplied
	got, err = urlnorm.NormalizeWithTrackingParams(
		"https://example.com/p?session=xyz&utm_source=mail",
		[]string{"session"},
	)
	require.NoError(t, err)
	require.Equal(t, "https://example.com/p", got)
}

func TestNormalize_Errors(t *testing.T) {
	t.Parallel()

	for _, in := range []string{"", " ", "ftp://example.com/x", "https:///missing-host"} {
		_, err := urlnorm.Normalize(in)
		require.Error(t, err, "input %q must be rejected", in)
	}
}

func TestIsTrackingParam(t *testing.T) {
	t.Parallel()

	require.True(t, urlnorm.IsTrackingParam("utm_source"))
	require.True(t, urlnorm.IsTrackingParam("FBClid"))
	require.False(t, urlnorm.IsTrackingParam("page"))
}
