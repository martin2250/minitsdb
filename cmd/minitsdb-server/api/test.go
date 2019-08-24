package api

import (
	"net/http"
)

type handleTest struct{}

func (h handleTest) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(200)
}
