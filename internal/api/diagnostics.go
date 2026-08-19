package api

import "net/http"

func (a *API) diagnostics(w http.ResponseWriter, r *http.Request) {
	report, err := a.delivery.Report(r.Context())
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, report)
}
