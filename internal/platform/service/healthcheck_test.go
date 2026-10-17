package service

import (
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthcheck(t *testing.T) {
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ok.Close()
	_, port, _ := net.SplitHostPort(ok.Listener.Addr().String())
	if err := Healthcheck(":" + port); err != nil {
		t.Fatalf("healthy server reported unhealthy: %v", err)
	}

	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer bad.Close()
	_, port, _ = net.SplitHostPort(bad.Listener.Addr().String())
	if err := Healthcheck(":" + port); err == nil {
		t.Fatal("503 reported healthy")
	}
	if err := Healthcheck("not-an-addr"); err == nil {
		t.Fatal("bad address accepted")
	}
}
