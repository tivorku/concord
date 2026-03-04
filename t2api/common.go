package t2api
import (
    "net/http"
    "time"
    "net"
    utls "github.com/refraction-networking/utls"
    "crypto/tls"
)
var (
    yes_reset = map[string]bool{"yes": true, "ye": true, "y": true}
	no = map[string]bool{"no": true, "n": true}
	no_reset = map[string]bool{"": true, "no": true, "n": true}
	yes = map[string]bool{"": true, "yes": true, "ye": true, "y": true}
	SharedClient *http.Client
)
const (
    AppVersion    = "mytele2-app/6.35.0"
    OkHttpVersion = "okhttp/4.0.12"
    T2Host        = "yar.t2.ru"
    T2FullHost    = "yar.t2.ru:443"
)
func init() {
	// создаем кастомный DialTLS для имитации реального устройства (Android/Chrome)
	dialTLS := func(network, addr string) (net.Conn, error) {
		conn, err := net.DialTimeout(network, addr, 10*time.Second)
		if err != nil {
			return nil, err
		}

		// создаем UClient, который мимикрирует под Chrome или Android.
		// Cloudflare доверяет этим отпечаткам гораздо больше, чем стандартному Go.
		config := &utls.Config{
			ServerName: T2Host,
			NextProtos: []string{"http/1.1"}, 
		}
		
		uConn := utls.UClient(conn, config, utls.HelloAndroid_11_OkHttp) // мимикрируем под Android 11
		
		if err := uConn.Handshake(); err != nil {
			uConn.Close()
			return nil, err
		}
		return uConn, nil
	}

	SharedClient = &http.Client{
		Timeout: 20 * time.Second,
		Transport: &http.Transport{
			DialTLS:             dialTLS,
			MaxIdleConns:        10,
			IdleConnTimeout:     90 * time.Second,
			// принудительно отключаем HTTP/2, так как в Go он палится на отпечатках
			TLSNextProto:        make(map[string]func(authority string, c *tls.Conn) http.RoundTripper),
			ForceAttemptHTTP2:   false,
			DisableCompression: false,
		},
	}
}