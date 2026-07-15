package ojhttp

import (
	"net"
	"net/http"
	"time"
)

const DefaultTimeout = 30 * time.Second

// Client 带超时的全局 OJ HTTP 客户端，避免 http.Get/DefaultClient 无超时挂死。
var Client = &http.Client{
	Timeout: DefaultTimeout,
	Transport: &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          64,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: 25 * time.Second,
	},
}

// NewWithJar 返回带 CookieJar 与超时的客户端（洛谷/QOJ 登录场景）。
func NewWithJar(jar http.CookieJar) *http.Client {
	return &http.Client{
		Timeout: DefaultTimeout,
		Jar:     jar,
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          32,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			ResponseHeaderTimeout: 25 * time.Second,
		},
	}
}

func Get(url string) (*http.Response, error) {
	return Client.Get(url)
}

func Do(req *http.Request) (*http.Response, error) {
	return Client.Do(req)
}
