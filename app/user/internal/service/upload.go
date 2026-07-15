package service

import (
	"crypto/rand"
	"cwxu-algo/app/common/utils/auth"
	"encoding/hex"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	khttp "github.com/go-kratos/kratos/v2/transport/http"
)

const (
	maxUploadBytes    = 3 << 20 // 3MB
	staticURLPrefix   = "/api/user/static"
	staticRoutePrefix = "/v1/user/static"
)

func UploadDir() string {
	if d := os.Getenv("CWXU_UPLOAD_DIR"); d != "" {
		return d
	}
	return "./data/uploads"
}

func ensureUploadDir() error {
	return os.MkdirAll(UploadDir(), 0o755)
}

func randomName(ext string) string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return time.Now().Format("20060102") + "_" + hex.EncodeToString(b) + ext
}

func extFromContentType(ct, filename string) string {
	ext := strings.ToLower(filepath.Ext(filename))
	switch ext {
	case ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".ico":
		return ext
	}
	if exts, _ := mime.ExtensionsByType(ct); len(exts) > 0 {
		return exts[0]
	}
	return ".bin"
}

func allowedImage(ct string) bool {
	ct = strings.ToLower(strings.TrimSpace(strings.Split(ct, ";")[0]))
	switch ct {
	case "image/jpeg", "image/png", "image/gif", "image/webp", "image/svg+xml",
		"image/x-icon", "image/vnd.microsoft.icon":
		return true
	default:
		return false
	}
}

// RegisterUploadRoutes 注册 multipart 上传与静态文件
func RegisterUploadRoutes(srv *khttp.Server) {
	_ = ensureUploadDir()
	r := srv.Route("/")

	r.POST("/v1/user/upload", func(ctx khttp.Context) error {
		pd := auth.GetCurrentUser(ctx)
		if pd == nil || pd.UserID == 0 {
			return ctx.JSON(http.StatusUnauthorized, map[string]interface{}{
				"code": 1, "message": "请先登录",
			})
		}

		req := ctx.Request()
		if err := req.ParseMultipartForm(maxUploadBytes); err != nil {
			return ctx.JSON(http.StatusBadRequest, map[string]interface{}{
				"code": 1, "message": "解析表单失败或文件过大(≤3MB)",
			})
		}
		file, hdr, err := req.FormFile("file")
		if err != nil {
			return ctx.JSON(http.StatusBadRequest, map[string]interface{}{
				"code": 1, "message": "缺少 file 字段",
			})
		}
		defer file.Close()

		// 读入内存嗅探类型（限制大小）
		data, err := io.ReadAll(io.LimitReader(file, maxUploadBytes+1))
		if err != nil {
			return ctx.JSON(http.StatusBadRequest, map[string]interface{}{
				"code": 1, "message": "读取文件失败",
			})
		}
		if int64(len(data)) > maxUploadBytes {
			return ctx.JSON(http.StatusBadRequest, map[string]interface{}{
				"code": 1, "message": "文件过大，最大 3MB",
			})
		}
		ct := hdr.Header.Get("Content-Type")
		if ct == "" || ct == "application/octet-stream" {
			ct = http.DetectContentType(data)
		}
		if !allowedImage(ct) {
			return ctx.JSON(http.StatusBadRequest, map[string]interface{}{
				"code": 1, "message": "仅支持图片: jpg/png/gif/webp/svg/ico",
			})
		}

		purpose := strings.TrimSpace(req.FormValue("purpose"))
		switch purpose {
		case "avatar", "site", "bulletin", "misc":
		default:
			purpose = "misc"
		}

		ext := extFromContentType(ct, hdr.Filename)
		relDir := filepath.Join(purpose, fmt.Sprintf("%d", pd.UserID))
		absDir := filepath.Join(UploadDir(), relDir)
		if err := os.MkdirAll(absDir, 0o755); err != nil {
			return ctx.JSON(http.StatusInternalServerError, map[string]interface{}{
				"code": 1, "message": "创建目录失败",
			})
		}
		name := randomName(ext)
		absPath := filepath.Join(absDir, name)
		if err := os.WriteFile(absPath, data, 0o644); err != nil {
			return ctx.JSON(http.StatusInternalServerError, map[string]interface{}{
				"code": 1, "message": "保存失败",
			})
		}

		urlPath := staticURLPrefix + "/" + filepath.ToSlash(filepath.Join(relDir, name))
		return ctx.JSON(http.StatusOK, map[string]interface{}{
			"code":    0,
			"message": "success",
			"url":     urlPath,
		})
	})

	fs := http.StripPrefix(staticRoutePrefix+"/", http.FileServer(http.Dir(UploadDir())))
	srv.HandlePrefix(staticRoutePrefix+"/", fs)
	fsAPI := http.StripPrefix(staticURLPrefix+"/", http.FileServer(http.Dir(UploadDir())))
	srv.HandlePrefix(staticURLPrefix+"/", fsAPI)
}
