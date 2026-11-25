package dns

import "testing"

func TestIsGeoSensitiveCDN(t *testing.T) {
	geoHosts := []string{
		"scontent.cdninstagram.com",
		"scontent.fxyz1-1.fna.fbcdn.net",
		"www.instagram.com",
		"rr1---sn-example.googlevideo.com",
		"i.ytimg.com",
		"ipv4-c001-example.1.oca.nflxvideo.net",
		"video-weaver.example.hls.ttvnw.net",
		"s2.glbimg.com",
		"http2.mlstatic.com",
		"br1.api.riotgames.com",
		"lol.secure.dyn.riotcdn.net",
	}
	for _, h := range geoHosts {
		if !IsGeoSensitiveCDN(h) {
			t.Errorf("expected %s to be geo-sensitive CDN", h)
		}
	}

	nonGeoHosts := []string{
		"github.com",
		"api.openai.com",
		"claude.ai",
		"ipinfo.io",
		"www.google.com",
	}
	for _, h := range nonGeoHosts {
		if IsGeoSensitiveCDN(h) {
			t.Errorf("expected %s NOT to be geo-sensitive CDN", h)
		}
	}
}
