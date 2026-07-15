package clientip

import (
	"context"
	"net"
	"net/http"
	"strings"

	"github.com/go-kratos/kratos/v2/transport"
	khttp "github.com/go-kratos/kratos/v2/transport/http"
)

// FromRequest 解析客户端真实 IP。
// 优先 Cloudflare / 反代头；X-Forwarded-For 取最左侧公网/非私有候选；最后 RemoteAddr。
// 在 Client → CF/Nginx → Gateway → User 链路上，只要边缘正确设置头即可拿到真实 IP。
func FromRequest(r *http.Request) string {
	if r == nil {
		return ""
	}
	if ip := cleanIP(r.Header.Get("CF-Connecting-IP")); ip != "" {
		return ip
	}
	if ip := cleanIP(r.Header.Get("True-Client-IP")); ip != "" {
		return ip
	}
	if ip := cleanIP(r.Header.Get("X-Real-IP")); ip != "" {
		return ip
	}
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		// 最左通常是原始客户端（由可信边缘追加）
		for _, p := range parts {
			if ip := cleanIP(p); ip != "" {
				return ip
			}
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		return cleanIP(host)
	}
	return cleanIP(r.RemoteAddr)
}

// FromContext 从 Kratos HTTP 上下文取真实 IP
func FromContext(ctx context.Context) string {
	if tr, ok := transport.FromServerContext(ctx); ok {
		if ht, ok := tr.(khttp.Transporter); ok {
			return FromRequest(ht.Request())
		}
		if h := tr.RequestHeader(); h != nil {
			if ip := cleanIP(h.Get("CF-Connecting-IP")); ip != "" {
				return ip
			}
			if ip := cleanIP(h.Get("True-Client-IP")); ip != "" {
				return ip
			}
			if ip := cleanIP(h.Get("X-Real-IP")); ip != "" {
				return ip
			}
			if xff := h.Get("X-Forwarded-For"); xff != "" {
				for _, p := range strings.Split(xff, ",") {
					if ip := cleanIP(p); ip != "" {
						return ip
					}
				}
			}
		}
	}
	return ""
}

func cleanIP(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	// 可能带端口
	if host, _, err := net.SplitHostPort(s); err == nil {
		s = host
	}
	s = strings.Trim(s, "[]")
	ip := net.ParseIP(s)
	if ip == nil {
		return ""
	}
	return ip.String()
}
