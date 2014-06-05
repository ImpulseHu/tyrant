package main

import (
	"net/http"
	"os"

	log "github.com/ngaut/logging"
)

func main() {
	http.Handle("/", http.FileServer(http.Dir(os.Args[1])))
	log.Error(http.ListenAndServe(":80", nil))
}
