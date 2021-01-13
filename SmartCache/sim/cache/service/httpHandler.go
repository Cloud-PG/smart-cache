package service

import (
	"fmt"
	"net/http"

	"github.com/rs/zerolog/log"
)

func Version(buildstamp string, githash string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Printf("Received %s request for host %s from IP address %s and X-FORWARDED-FOR %s",
			r.Method, r.Host, r.RemoteAddr, r.Header.Get("X-FORWARDED-FOR"))

		resp := fmt.Sprintf("Build time:\t%s\nGit hash:\t%s\n", buildstamp, githash)
		_, errWrite := w.Write([]byte(resp))

		if errWrite != nil {
			log.Err(errWrite).Str("resp", resp).Msg("Cannot write a response")
		} else {
			log.Printf("Sent response %s", resp)
		}
	}
}
