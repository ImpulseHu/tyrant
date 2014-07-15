package main

import (
	"flag"
	"net/http"

	log "github.com/ngaut/logging"
)

var (
	addr = flag.String("addr", ":80", "listen address")
	path = flag.String("path", "./", "root path")
)

func main() {
	flag.Parse()
	log.Debug(*path)
	http.Handle("/", http.FileServer(http.Dir(*path)))
	log.Error(http.ListenAndServe(*addr, nil))
}
