package portmap

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeNATPMP is a gateway answering NAT-PMP on localhost.
type fakeNATPMP struct {
	pc       net.PacketConn
	mu       sync.Mutex
	mappings map[string]int // "tcp:6881" -> lifetime seconds
	dropN    int            // drop this many requests first (packet loss)
}

func startNATPMP(t *testing.T) *fakeNATPMP {
	pc, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeNATPMP{pc: pc, mappings: map[string]int{}}
	t.Cleanup(func() { pc.Close() })
	go func() {
		buf := make([]byte, 64)
		for {
			n, from, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			f.mu.Lock()
			if f.dropN > 0 {
				f.dropN--
				f.mu.Unlock()
				continue
			}
			f.mu.Unlock()
			if n < 2 || buf[0] != 0 {
				continue
			}
			switch op := buf[1]; op {
			case 0:
				pc.WriteTo([]byte{0, 128, 0, 0, 0, 0, 0, 1, 203, 0, 113, 7}, from)
			case 1, 2:
				internal := binary.BigEndian.Uint16(buf[4:])
				external := binary.BigEndian.Uint16(buf[6:])
				life := binary.BigEndian.Uint32(buf[8:])
				proto := map[byte]string{1: "udp", 2: "tcp"}[op]
				f.mu.Lock()
				key := fmt.Sprintf("%s:%d", proto, internal)
				if life == 0 {
					delete(f.mappings, key)
					external = 0
				} else {
					f.mappings[key] = int(life)
				}
				f.mu.Unlock()
				resp := make([]byte, 16)
				resp[1] = 128 + op
				binary.BigEndian.PutUint16(resp[8:], internal)
				binary.BigEndian.PutUint16(resp[10:], external)
				binary.BigEndian.PutUint32(resp[12:], life)
				pc.WriteTo(resp, from)
			}
		}
	}()
	return f
}

func (f *fakeNATPMP) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.mappings)
}

// closedUDP returns an address where nothing answers.
func closedUDP(t *testing.T) string {
	pc, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := pc.LocalAddr().String()
	pc.Close()
	return addr
}

func TestNATPMPMapsAndRemoves(t *testing.T) {
	gw := startNATPMP(t)
	gw.mu.Lock()
	gw.dropN = 1 // the first request is lost: the client must resend
	gw.mu.Unlock()
	s, err := Map(Options{Port: 6881, Protocols: []string{"tcp", "udp"},
		NATPMPGateway: gw.pc.LocalAddr().String(), SSDPTarget: closedUDP(t), Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if s.Method != "NAT-PMP" || s.External["tcp"] != 6881 || s.External["udp"] != 6881 {
		t.Fatalf("got %+v", s)
	}
	if s.ExternalIP.String() != "203.0.113.7" {
		t.Fatalf("external IP %v", s.ExternalIP)
	}
	if gw.count() != 2 {
		t.Fatalf("gateway has %d mappings, want 2", gw.count())
	}
	s.Close()
	if gw.count() != 0 {
		t.Fatal("mappings left on the gateway after Close")
	}
}

func TestNATPMPRenewsBeforeTheLeaseRunsOut(t *testing.T) {
	gw := startNATPMP(t)
	s, err := Map(Options{Port: 7000, Protocols: []string{"tcp"}, Lease: 200 * time.Millisecond,
		NATPMPGateway: gw.pc.LocalAddr().String(), SSDPTarget: closedUDP(t), Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	gw.mu.Lock()
	delete(gw.mappings, "tcp:7000") // as if the lease had expired
	gw.mu.Unlock()
	time.Sleep(300 * time.Millisecond)
	if gw.count() != 1 {
		t.Fatal("mapping was not renewed")
	}
}

// fakeIGD is a UPnP router: an SSDP responder plus an HTTP server with the
// device description and the WANIPConnection control endpoint.
type fakeIGD struct {
	ssdp     net.PacketConn
	http     *httptest.Server
	mu       sync.Mutex
	mappings map[string]string // "TCP:6881" -> internal client
	actions  []string
	permOnly bool // reject leases with error 725, like some routers
}

const description = `<?xml version="1.0"?>
<root xmlns="urn:schemas-upnp-org:device-1-0">
 <device><deviceType>urn:schemas-upnp-org:device:InternetGatewayDevice:1</deviceType>
  <deviceList><device><deviceType>urn:schemas-upnp-org:device:WANDevice:1</deviceType>
   <deviceList><device><deviceType>urn:schemas-upnp-org:device:WANConnectionDevice:1</deviceType>
    <serviceList><service>
     <serviceType>urn:schemas-upnp-org:service:WANIPConnection:1</serviceType>
     <controlURL>/ctl/IPConn</controlURL>
    </service></serviceList>
   </device></deviceList>
  </device></deviceList>
 </device>
</root>`

func startIGD(t *testing.T) *fakeIGD {
	f := &fakeIGD{mappings: map[string]string{}}
	mux := http.NewServeMux()
	mux.HandleFunc("/desc.xml", func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, description) })
	mux.HandleFunc("/ctl/IPConn", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		action := r.Header.Get("SOAPAction")
		f.mu.Lock()
		defer f.mu.Unlock()
		f.actions = append(f.actions, action)
		field := func(name string) string { return xmlField(body, name) }
		fail := func(code int, desc string) {
			w.WriteHeader(500)
			fmt.Fprintf(w, `<s:Envelope><s:Body><s:Fault><detail><UPnPError><errorCode>%d</errorCode><errorDescription>%s</errorDescription></UPnPError></detail></s:Fault></s:Body></s:Envelope>`, code, desc)
		}
		switch {
		case strings.HasSuffix(action, `#AddPortMapping"`):
			if f.permOnly && field("NewLeaseDuration") != "0" {
				fail(725, "OnlyPermanentLeasesSupported")
				return
			}
			f.mappings[field("NewProtocol")+":"+field("NewExternalPort")] = field("NewInternalClient")
			io.WriteString(w, `<s:Envelope><s:Body><u:AddPortMappingResponse/></s:Body></s:Envelope>`)
		case strings.HasSuffix(action, `#DeletePortMapping"`):
			delete(f.mappings, field("NewProtocol")+":"+field("NewExternalPort"))
			io.WriteString(w, `<s:Envelope><s:Body><u:DeletePortMappingResponse/></s:Body></s:Envelope>`)
		case strings.HasSuffix(action, `#GetExternalIPAddress"`):
			io.WriteString(w, `<s:Envelope><s:Body><u:GetExternalIPAddressResponse><NewExternalIPAddress>198.51.100.9</NewExternalIPAddress></u:GetExternalIPAddressResponse></s:Body></s:Envelope>`)
		default:
			fail(401, "Invalid Action")
		}
	})
	f.http = httptest.NewServer(mux)
	t.Cleanup(f.http.Close)

	pc, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f.ssdp = pc
	t.Cleanup(func() { pc.Close() })
	go func() {
		buf := make([]byte, 2048)
		for {
			n, from, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			req, err := http.ReadRequest(bufio.NewReader(strings.NewReader(string(buf[:n]))))
			if err != nil || req.Method != "M-SEARCH" {
				continue
			}
			pc.WriteTo([]byte("HTTP/1.1 200 OK\r\nST: "+req.Header.Get("St")+"\r\nLOCATION: "+f.http.URL+"/desc.xml\r\n\r\n"), from)
		}
	}()
	return f
}

func TestUPnPWhenThereIsNoNATPMP(t *testing.T) {
	igd := startIGD(t)
	s, err := Map(Options{Port: 6881, Protocols: []string{"tcp", "udp"},
		NATPMPGateway: closedUDP(t), SSDPTarget: igd.ssdp.LocalAddr().String(), Timeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if s.Method != "UPnP" || s.ExternalIP.String() != "198.51.100.9" {
		t.Fatalf("got %+v", s)
	}
	igd.mu.Lock()
	client := igd.mappings["TCP:6881"]
	n := len(igd.mappings)
	igd.mu.Unlock()
	if n != 2 || client != "127.0.0.1" {
		t.Fatalf("router mappings: %d, TCP forwarded to %q", n, client)
	}
	s.Close()
	igd.mu.Lock()
	defer igd.mu.Unlock()
	if len(igd.mappings) != 0 {
		t.Fatal("mappings left on the router after Close")
	}
}

func TestUPnPRetriesWithPermanentLease(t *testing.T) {
	igd := startIGD(t)
	igd.permOnly = true
	s, err := Map(Options{Port: 51413, Protocols: []string{"tcp"},
		NATPMPGateway: closedUDP(t), SSDPTarget: igd.ssdp.LocalAddr().String(), Timeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	igd.mu.Lock()
	defer igd.mu.Unlock()
	if _, ok := igd.mappings["TCP:51413"]; !ok {
		t.Fatal("mapping not created after error 725")
	}
}

func TestNoRouterFailsFast(t *testing.T) {
	start := time.Now()
	_, err := Map(Options{Port: 6881, Protocols: []string{"tcp"},
		NATPMPGateway: closedUDP(t), SSDPTarget: closedUDP(t), Timeout: 500 * time.Millisecond})
	if err == nil {
		t.Fatal("mapped a port with no router")
	}
	if time.Since(start) > 3*time.Second {
		t.Fatalf("took %s to give up", time.Since(start))
	}
}

func TestParseGateways(t *testing.T) {
	proc := "Iface\tDestination\tGateway \tFlags\n" +
		"eth0\t0000FEA9\t00000000\t0001\n" +
		"eth0\t00000000\t0101A8C0\t0003\n"
	ip, err := parseProcRoute(bufio.NewScanner(strings.NewReader(proc)))
	if err != nil || ip.String() != "192.168.1.1" {
		t.Fatalf("linux: %v, %v", ip, err)
	}
	mac := "   route to: default\ndestination: default\n       mask: default\n    gateway: 10.0.0.138\n  interface: en0\n"
	ip, err = parseRouteGet(mac)
	if err != nil || ip.String() != "10.0.0.138" {
		t.Fatalf("macOS: %v, %v", ip, err)
	}
}
