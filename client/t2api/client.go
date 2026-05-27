package t2api

import (
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"time"

	utls "github.com/refraction-networking/utls"
)

var (
	SharedClient    *http.Client
	certFingerprint string
)

func GetCertFingerprint() string {
	return certFingerprint
}

func ValidateCert(conn net.Conn, expectedFingerprint string) error {
	tlsConn := tls.Client(conn, &tls.Config{
		ServerName: T2Host,
	})
	if err := tlsConn.Handshake(); err != nil {
		tlsConn.Close()
		return fmt.Errorf("ошибка TLS handshake: %w", err)
	}
	defer tlsConn.Close()

	certs := tlsConn.ConnectionState().PeerCertificates
	if len(certs) == 0 {
		return fmt.Errorf("no certificates received")
	}

	fp := sha256.Sum256(certs[0].Raw)
	fingerprint := hex.EncodeToString(fp[:8])
	if fingerprint != expectedFingerprint {
		return fmt.Errorf("certificate mismatch: expected %s, got %s", expectedFingerprint, fingerprint)
	}

	return nil
}

const (
	AppVersion    = "mytele2-app/6.39.0"
	OkHttpVersion = "okhttp/4.12.0"
	T2Host        = "yar.t2.ru"
	T2FullHost    = "yar.t2.ru:443"
)

func setTele2Headers(req *http.Request, apiVersion string) {
	req.Header.Set("Tele2-User-Agent", AppVersion)
	req.Header.Set("User-Agent", OkHttpVersion)
	if apiVersion != "" {
		req.Header.Set("X-API-Version", apiVersion)
	}
}

func FetchCertificateFingerprint() error {
	conn, err := tls.Dial("tcp", T2FullHost, nil)
	if err != nil {
		return fmt.Errorf("failed to connect to T2: %w", err)
	}
	defer conn.Close()

	certs := conn.ConnectionState().PeerCertificates
	if len(certs) == 0 {
		return fmt.Errorf("no certificates received from T2")
	}

	fp := sha256.Sum256(certs[0].Raw)
	certFingerprint = hex.EncodeToString(fp[:8])
	return nil
}

func init() {
	dialTLS := func(network, addr string) (net.Conn, error) {
		conn, err := net.DialTimeout(network, addr, 10*time.Second)
		if err != nil {
			return nil, err
		}

		config := &utls.Config{
			ServerName: T2Host,
			NextProtos: []string{"http/1.1"},
		}

		uConn := utls.UClient(conn, config, utls.HelloAndroid_11_OkHttp)

		if err := uConn.Handshake(); err != nil {
			uConn.Close()
			return nil, err
		}

		if certFingerprint != "" {
			cert := uConn.ConnectionState().PeerCertificates[0]
			fp := sha256.Sum256(cert.Raw)
			fingerprint := hex.EncodeToString(fp[:8])
			if fingerprint != certFingerprint {
				uConn.Close()
				return nil, fmt.Errorf("certificate mismatch: possible MITM attack")
			}
		}

		return uConn, nil
	}

	SharedClient = &http.Client{
		Timeout: 20 * time.Second,
		Transport: &http.Transport{
			DialTLS:            dialTLS,
			MaxIdleConns:       10,
			IdleConnTimeout:    90 * time.Second,
			TLSNextProto:       make(map[string]func(authority string, c *tls.Conn) http.RoundTripper),
		},
	}
}
