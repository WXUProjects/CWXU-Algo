package health

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	khttp "github.com/go-kratos/kratos/v2/transport/http"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

// Checker 可选依赖检查。
type Checker struct {
	DB  *gorm.DB
	RDB *redis.Client
}

type statusBody struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks,omitempty"`
}

// Register 在 Kratos HTTP Server 上注册 /healthz 与 /readyz。
func Register(srv *khttp.Server, c Checker) {
	srv.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, statusBody{Status: "ok"})
	})
	srv.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		checks := map[string]string{}
		ok := true
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		if c.DB != nil {
			sqlDB, err := c.DB.DB()
			if err != nil {
				checks["db"] = err.Error()
				ok = false
			} else if err := sqlDB.PingContext(ctx); err != nil {
				checks["db"] = err.Error()
				ok = false
			} else {
				checks["db"] = "ok"
			}
		}
		if c.RDB != nil {
			if err := c.RDB.Ping(ctx).Err(); err != nil {
				checks["redis"] = err.Error()
				ok = false
			} else {
				checks["redis"] = "ok"
			}
		}
		code := http.StatusOK
		st := "ok"
		if !ok {
			code = http.StatusServiceUnavailable
			st = "not_ready"
		}
		writeJSON(w, code, statusBody{Status: st, Checks: checks})
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// StopGroup 管理可取消的后台循环。
type StopGroup struct {
	mu     sync.Mutex
	stopCh chan struct{}
	once   sync.Once
}

func NewStopGroup() *StopGroup {
	return &StopGroup{stopCh: make(chan struct{})}
}

func (g *StopGroup) Stop() {
	g.once.Do(func() { close(g.stopCh) })
}

func (g *StopGroup) Done() <-chan struct{} {
	return g.stopCh
}
