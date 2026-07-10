package geodb

import (
	"net/netip"
	"reflect"
	"testing"

	"github.com/oschwald/maxminddb-golang/v2"
)

func TestParsePath(t *testing.T) {
	got, err := ParsePath("subdivisions.0.iso_code")
	if err != nil {
		t.Fatal(err)
	}
	want := []any{"subdivisions", 0, "iso_code"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v want %v", got, want)
	}
	for _, bad := range []string{"", "a..b"} {
		if _, err := ParsePath(bad); err == nil {
			t.Errorf("ParsePath(%q): expected error", bad)
		}
	}
}

func openCity(t *testing.T) *maxminddb.Reader {
	t.Helper()
	r, err := maxminddb.Open("../../testdata/GeoIP2-City-Test.mmdb")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close() })
	return r
}

func TestLookupPath(t *testing.T) {
	city := openCity(t)
	ip := netip.MustParseAddr("2.125.160.216")
	cases := []struct{ path, want string }{
		{"country.iso_code", "GB"},
		{"subdivisions.0.iso_code", "ENG"},
		{"city.names.en", "Boxford"},
		{"postal.code", "OX1"},
		{"location.time_zone", "Europe/London"},
		{"location.latitude", "51.75"},
		{"location.longitude", "-1.25"},
	}
	for _, c := range cases {
		path, err := ParsePath(c.path)
		if err != nil {
			t.Fatal(err)
		}
		got, found, err := LookupPath(city, ip, path)
		if err != nil || !found || got != c.want {
			t.Errorf("%s: got (%q, %v, %v), want (%q, true, nil)", c.path, got, found, err, c.want)
		}
	}
}

func TestLookupPathASN(t *testing.T) {
	r, err := maxminddb.Open("../../testdata/GeoLite2-ASN-Test.mmdb")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	ip := netip.MustParseAddr("1.128.0.0")
	path, _ := ParsePath("autonomous_system_number")
	got, found, err := LookupPath(r, ip, path)
	if err != nil || !found || got != "1221" {
		t.Errorf("asn: got (%q, %v, %v)", got, found, err)
	}
	path, _ = ParsePath("autonomous_system_organization")
	got, found, _ = LookupPath(r, ip, path)
	if !found || got != "Telstra Pty Ltd" {
		t.Errorf("org: got (%q, %v)", got, found)
	}
}

func TestLookupPathMisses(t *testing.T) {
	city := openCity(t)
	// IP not in the test database at all.
	path, _ := ParsePath("country.iso_code")
	if _, found, err := LookupPath(city, netip.MustParseAddr("203.0.113.5"), path); found || err != nil {
		t.Errorf("unknown ip: found=%v err=%v, want false, nil", found, err)
	}
	// Existing IP, path absent in the record.
	path, _ = ParsePath("traits.nope")
	if _, found, _ := LookupPath(city, netip.MustParseAddr("2.125.160.216"), path); found {
		t.Error("absent path: want found=false")
	}
	// Path pointing at a composite value must error, not emit garbage.
	path, _ = ParsePath("location")
	if _, _, err := LookupPath(city, netip.MustParseAddr("2.125.160.216"), path); err == nil {
		t.Error("composite value: want error")
	}
}
