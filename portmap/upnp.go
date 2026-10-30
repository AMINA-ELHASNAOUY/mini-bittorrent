package portmap

import (
	"bufio"
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// UPnP IGD: find the router with SSDP (a multicast HTTP-over-UDP search),
// read its device description, then ask its WANIPConnection (or
// WANPPPConnection) service for port mappings with SOAP calls.

const ssdpAddr = "239.255.255.250:1900"

var igdSearchTargets = []string{
	"urn:schemas-upnp-org:device:InternetGatewayDevice:1",
	"urn:schemas-upnp-org:device:InternetGatewayDevice:2",
	"urn:schemas-upnp-org:service:WANIPConnection:1",
}

type upnpIGD struct {
	controlURL  string
	serviceType string
	localIP     string // our LAN address, as the router sees it
	client      *http.Client
}

// discoverUPnP finds an internet gateway device. target is normally the
// SSDP multicast address; tests point it at a local fake.
func discoverUPnP(target string, timeout time.Duration) (*upnpIGD, error) {
	pc, err := net.ListenPacket("udp4", ":0")
	if err != nil {
		return nil, err
	}
	defer pc.Close()
	dst, err := net.ResolveUDPAddr("udp4", target)
	if err != nil {
		return nil, err
	}
	for _, st := range igdSearchTargets {
		msg := "M-SEARCH * HTTP/1.1\r\nHOST: " + ssdpAddr + "\r\nST: " + st +
			"\r\nMAN: \"ssdp:discover\"\r\nMX: 2\r\n\r\n"
		pc.WriteTo([]byte(msg), dst)
	}

	client := &http.Client{Timeout: 3 * time.Second}
	deadline := time.Now().Add(timeout)
	buf := make([]byte, 2048)
	tried := map[string]bool{}
	var lastErr error = errors.New("UPnP: no gateway answered")
	for time.Now().Before(deadline) {
		pc.SetReadDeadline(deadline)
		n, _, err := pc.ReadFrom(buf)
		if err != nil {
			break
		}
		resp, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(buf[:n])), nil)
		if err != nil {
			continue
		}
		loc := resp.Header.Get("Location")
		if loc == "" || tried[loc] {
			continue
		}
		tried[loc] = true
		igd, err := describe(client, loc)
		if err != nil {
			lastErr = err
			continue
		}
		return igd, nil
	}
	return nil, lastErr
}

type upnpService struct {
	ServiceType string `xml:"serviceType"`
	ControlURL  string `xml:"controlURL"`
}

type upnpDevice struct {
	Services []upnpService `xml:"serviceList>service"`
	Devices  []upnpDevice  `xml:"deviceList>device"`
}

type upnpRoot struct {
	URLBase string     `xml:"URLBase"`
	Device  upnpDevice `xml:"device"`
}

// findService walks the (nested) device tree for a WAN connection service.
func (d *upnpDevice) findService() *upnpService {
	for i, s := range d.Services {
		if strings.Contains(s.ServiceType, "WANIPConnection") || strings.Contains(s.ServiceType, "WANPPPConnection") {
			return &d.Services[i]
		}
	}
	for i := range d.Devices {
		if s := d.Devices[i].findService(); s != nil {
			return s
		}
	}
	return nil
}

// describe reads a device description and finds where to send SOAP calls.
func describe(client *http.Client, location string) (*upnpIGD, error) {
	resp, err := client.Get(location)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var root upnpRoot
	if err := xml.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&root); err != nil {
		return nil, fmt.Errorf("UPnP: device description: %w", err)
	}
	svc := root.Device.findService()
	if svc == nil {
		return nil, errors.New("UPnP: device has no WAN connection service")
	}
	base, err := url.Parse(location)
	if err != nil {
		return nil, err
	}
	if root.URLBase != "" {
		if b, err := url.Parse(root.URLBase); err == nil {
			base = b
		}
	}
	ctl, err := base.Parse(svc.ControlURL)
	if err != nil {
		return nil, err
	}
	// Our LAN address is whatever local address reaches the router.
	c, err := net.Dial("udp4", ctl.Host)
	if err != nil {
		return nil, err
	}
	local := c.LocalAddr().(*net.UDPAddr).IP.String()
	c.Close()
	return &upnpIGD{controlURL: ctl.String(), serviceType: svc.ServiceType, localIP: local, client: client}, nil
}

// soapError is the UPnP error a router returned.
type soapError struct {
	Code        int
	Description string
}

func (e *soapError) Error() string { return fmt.Sprintf("UPnP error %d: %s", e.Code, e.Description) }

// call performs one SOAP action and returns the response body.
func (g *upnpIGD) call(action string, args [][2]string) ([]byte, error) {
	var body strings.Builder
	body.WriteString(`<?xml version="1.0"?>` +
		`<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/"><s:Body>`)
	fmt.Fprintf(&body, `<u:%s xmlns:u="%s">`, action, g.serviceType)
	for _, a := range args {
		body.WriteString("<" + a[0] + ">")
		xml.EscapeText(&body, []byte(a[1]))
		body.WriteString("</" + a[0] + ">")
	}
	fmt.Fprintf(&body, `</u:%s></s:Body></s:Envelope>`, action)

	req, err := http.NewRequest("POST", g.controlURL, strings.NewReader(body.String()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", `text/xml; charset="utf-8"`)
	req.Header.Set("SOAPAction", `"`+g.serviceType+"#"+action+`"`)
	resp, err := g.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		e := &soapError{Description: resp.Status}
		if code := xmlField(out, "errorCode"); code != "" {
			e.Code, _ = strconv.Atoi(code)
		}
		if d := xmlField(out, "errorDescription"); d != "" {
			e.Description = d
		}
		return nil, e
	}
	return out, nil
}

// xmlField returns the text of the first element with this local name.
func xmlField(doc []byte, name string) string {
	d := xml.NewDecoder(bytes.NewReader(doc))
	for {
		tok, err := d.Token()
		if err != nil {
			return ""
		}
		if se, ok := tok.(xml.StartElement); ok && se.Name.Local == name {
			var v string
			if d.DecodeElement(&v, &se) == nil {
				return strings.TrimSpace(v)
			}
			return ""
		}
	}
}

func (g *upnpIGD) externalIP() (net.IP, error) {
	out, err := g.call("GetExternalIPAddress", nil)
	if err != nil {
		return nil, err
	}
	ip := net.ParseIP(xmlField(out, "NewExternalIPAddress"))
	if ip == nil {
		return nil, errors.New("UPnP: no external address in the answer")
	}
	return ip, nil
}

// addMapping forwards external -> our internal port. Routers that only
// accept permanent mappings (error 725) get a lease of 0.
func (g *upnpIGD) addMapping(proto string, internal, external int, lease time.Duration) error {
	args := func(lease time.Duration) [][2]string {
		return [][2]string{
			{"NewRemoteHost", ""},
			{"NewExternalPort", strconv.Itoa(external)},
			{"NewProtocol", strings.ToUpper(proto)},
			{"NewInternalPort", strconv.Itoa(internal)},
			{"NewInternalClient", g.localIP},
			{"NewEnabled", "1"},
			{"NewPortMappingDescription", "mini-bittorrent"},
			{"NewLeaseDuration", strconv.Itoa(int(lease / time.Second))},
		}
	}
	_, err := g.call("AddPortMapping", args(lease))
	var se *soapError
	if errors.As(err, &se) && se.Code == 725 {
		_, err = g.call("AddPortMapping", args(0))
	}
	return err
}

func (g *upnpIGD) deleteMapping(proto string, external int) error {
	_, err := g.call("DeletePortMapping", [][2]string{
		{"NewRemoteHost", ""},
		{"NewExternalPort", strconv.Itoa(external)},
		{"NewProtocol", strings.ToUpper(proto)},
	})
	return err
}
