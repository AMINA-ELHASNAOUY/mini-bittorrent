// Package portmap asks the home router to forward a port to us, so peers
// on the internet can connect even though we sit behind NAT. It speaks
// NAT-PMP (RFC 6886) and UPnP IGD, the two protocols home routers offer,
// keeps the mappings alive while running and removes them on Close.
package portmap

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"
)

// Options configure Map. The zero value of the test hooks means "find the
// real gateway".
type Options struct {
	Port      int      // our local port; the same external port is requested
	Protocols []string // "tcp", "udp"
	Lease     time.Duration

	NATPMPGateway string // "ip:port" of a NAT-PMP gateway (default: the default route, port 5351)
	SSDPTarget    string // where to send the UPnP search (default: the SSDP multicast group)
	Timeout       time.Duration
}

// Session is a set of active mappings.
type Session struct {
	Method     string         // "NAT-PMP" or "UPnP"
	ExternalIP net.IP         // the router's public address, if it told us
	External   map[string]int // protocol -> external port

	opts    Options
	pmp     *natpmp
	igd     *upnpIGD
	stop    chan struct{}
	done    chan struct{}
	closeMu sync.Once
}

// Map finds the router and maps the port for every protocol. NAT-PMP is
// tried first (one UDP round trip); UPnP discovery runs at the same time
// and is used if NAT-PMP isn't available.
func Map(o Options) (*Session, error) {
	if o.Lease == 0 {
		o.Lease = time.Hour
	}
	if o.Timeout == 0 {
		o.Timeout = 3 * time.Second
	}
	if o.SSDPTarget == "" {
		o.SSDPTarget = ssdpAddr
	}

	type found struct {
		igd *upnpIGD
		err error
	}
	upnpCh := make(chan found, 1)
	go func() {
		igd, err := discoverUPnP(o.SSDPTarget, o.Timeout)
		upnpCh <- found{igd, err}
	}()

	var errs []error
	gw := o.NATPMPGateway
	if gw == "" {
		if ip, err := DefaultGateway(); err == nil {
			gw = net.JoinHostPort(ip.String(), fmt.Sprint(natpmpPort))
		} else {
			errs = append(errs, err)
		}
	}
	if gw != "" {
		pmp := &natpmp{gateway: gw, tries: 3}
		s := &Session{Method: "NAT-PMP", opts: o, pmp: pmp, External: map[string]int{}}
		if err := s.mapAll(); err == nil {
			s.ExternalIP, _ = pmp.externalIP()
			s.start()
			return s, nil
		} else {
			errs = append(errs, err)
		}
	}

	f := <-upnpCh
	if f.err != nil {
		return nil, errors.Join(append(errs, f.err)...)
	}
	s := &Session{Method: "UPnP", opts: o, igd: f.igd, External: map[string]int{}}
	if err := s.mapAll(); err != nil {
		return nil, err
	}
	s.ExternalIP, _ = f.igd.externalIP()
	s.start()
	return s, nil
}

func (s *Session) mapAll() error {
	for _, proto := range s.opts.Protocols {
		if s.pmp != nil {
			ext, err := s.pmp.addMapping(proto, s.opts.Port, s.opts.Port, s.opts.Lease)
			if err != nil {
				return err
			}
			s.External[proto] = ext
		} else {
			if err := s.igd.addMapping(proto, s.opts.Port, s.opts.Port, s.opts.Lease); err != nil {
				return err
			}
			s.External[proto] = s.opts.Port
		}
	}
	return nil
}

// start renews the mappings at half their lease, as RFC 6886 suggests.
func (s *Session) start() {
	s.stop, s.done = make(chan struct{}), make(chan struct{})
	go func() {
		defer close(s.done)
		t := time.NewTicker(s.opts.Lease / 2)
		defer t.Stop()
		for {
			select {
			case <-s.stop:
				return
			case <-t.C:
				s.mapAll() // a failed renewal is retried next time
			}
		}
	}()
}

// Close stops renewing and removes the mappings from the router.
func (s *Session) Close() {
	s.closeMu.Do(func() {
		close(s.stop)
		<-s.done
		for proto, ext := range s.External {
			if s.pmp != nil {
				s.pmp.addMapping(proto, s.opts.Port, 0, 0) // lifetime 0 deletes
			} else {
				s.igd.deleteMapping(proto, ext)
			}
		}
	})
}

// String describes the result for the user.
func (s *Session) String() string {
	var parts []string
	for _, proto := range s.opts.Protocols {
		parts = append(parts, fmt.Sprintf("%s %d", strings.ToUpper(proto), s.External[proto]))
	}
	where := ""
	if s.ExternalIP != nil {
		where = " on " + s.ExternalIP.String()
	}
	return fmt.Sprintf("%s forwarded by the router (%s)%s", strings.Join(parts, ", "), s.Method, where)
}

// DefaultGateway returns the IPv4 default route's gateway.
func DefaultGateway() (net.IP, error) {
	switch runtime.GOOS {
	case "linux":
		f, err := os.Open("/proc/net/route")
		if err != nil {
			return nil, err
		}
		defer f.Close()
		return parseProcRoute(bufio.NewScanner(f))
	case "darwin", "freebsd", "openbsd", "netbsd", "dragonfly":
		out, err := exec.Command("route", "-n", "get", "default").Output()
		if err != nil {
			return nil, err
		}
		return parseRouteGet(string(out))
	}
	return nil, fmt.Errorf("finding the gateway isn't supported on %s", runtime.GOOS)
}

// parseProcRoute reads Linux's routing table: the default route has
// destination 00000000, and addresses are little-endian hex.
func parseProcRoute(sc *bufio.Scanner) (net.IP, error) {
	for sc.Scan() {
		f := strings.Fields(sc.Text())
		if len(f) < 3 || f[1] != "00000000" {
			continue
		}
		var v uint32
		if _, err := fmt.Sscanf(f[2], "%x", &v); err != nil {
			continue
		}
		ip := make(net.IP, 4)
		binary.LittleEndian.PutUint32(ip, v)
		if !ip.IsUnspecified() {
			return ip, nil
		}
	}
	return nil, errors.New("no default route")
}

// parseRouteGet reads `route -n get default` output on macOS and the BSDs.
func parseRouteGet(out string) (net.IP, error) {
	for _, line := range strings.Split(out, "\n") {
		k, v, ok := strings.Cut(strings.TrimSpace(line), ":")
		if ok && k == "gateway" {
			if ip := net.ParseIP(strings.TrimSpace(v)); ip != nil {
				return ip, nil
			}
		}
	}
	return nil, errors.New("no default gateway")
}
