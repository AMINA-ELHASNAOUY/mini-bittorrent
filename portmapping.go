package main

import (
	"fmt"
	"sync"
	"time"

	"mini-bittorrent/portmap"
)

// portMapping is switched off by the tests, which must not talk to a real router.
var portMapping = true

// startPortMapping asks the router (NAT-PMP or UPnP) to forward port to us
// for TCP and, with udp, for UDP (uTP and the DHT). It runs in the
// background, so a slow or missing router never delays anything; the
// returned function removes the mappings again.
func startPortMapping(port int, udp, enabled, verbose bool) (stop func()) {
	if !portMapping || !enabled {
		return func() {}
	}
	protos := []string{"tcp"}
	if udp {
		protos = append(protos, "udp")
	}
	var mu sync.Mutex
	var session *portmap.Session
	stopped := false
	go func() {
		s, err := portmap.Map(portmap.Options{Port: port, Protocols: protos})
		if err != nil {
			if verbose {
				fmt.Printf("%s  no port mapping (%v)\n", clearLineSeq, err)
			}
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if stopped {
			s.Close()
			return
		}
		session = s
		fmt.Printf("%s→ %s\n", clearLineSeq, s)
	}()
	return func() {
		mu.Lock()
		stopped = true
		s := session
		mu.Unlock()
		if s == nil {
			return
		}
		done := make(chan struct{})
		go func() { s.Close(); close(done) }()
		select { // don't hang on exit if the router stopped answering
		case <-done:
		case <-time.After(3 * time.Second):
		}
	}
}
