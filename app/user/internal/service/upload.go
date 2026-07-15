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

// 常见图片后缀：边缘 CDN/nginx 常对这类扩展名做静态加速，不反代到后端，导致 404。
// 对外 URL 不使用这些后缀；磁盘上仍可保留旧文件名以便兼容。
var imageExts = []string{".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".ico", ".bin"}

func UploadDir() string {
	if d := os.Getenv("CWXU_UPLOAD_DIR"); d != "" {
		return d
	}
	return "./data/uploads"
}

func ensureUploadDir() error {
	return os.MkdirAll(UploadDir(), 0o755)
}

func randomName() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return time.Now().Format("20060102") + "_" + hex.EncodeToString(b)
}

func extFromContentType(ct, filename string) string {
	ext := strings.ToLower(filepath.Ext(filename))
	switch ext {
	case ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".ico":
		return ext
	}
	if exts, _ := mime.ExtensionsByType(ct); len(exts) > 0 {
		return strings.ToLower(exts[0])
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

func contentTypeFromExt(ext string) string {
	switch strings.ToLower(ext) {
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".png":
		return "image/png"
	case ".gif":
		return "image/gif"
	case ".webp":
		return "image/webp"
	case ".svg":
		return "image/svg+xml"
	case ".ico":
		return "image/x-icon"
	default:
		return ""
	}
}

// resolveUploadFile 在上传目录内安全解析相对路径；无扩展名时尝试常见图片后缀（兼容旧文件）。
func resolveUploadFile(rel string) (abs string, ext string, err error) {
	rel = filepath.ToSlash(rel)
	rel = strings.TrimPrefix(rel, "/")
	if rel == "" || strings.Contains(rel, "..") {
		return "", "", os.ErrNotExist
	}
	base := UploadDir()
	try := func(p string) (string, string, error) {
		p = filepath.Clean(p)
		absPath := filepath.Join(base, p)
		// 必须仍在 UploadDir 下
		relCheck, e := filepath.Rel(base, absPath)
		if e != nil || strings.HasPrefix(relCheck, "..") {
			return "", "", os.ErrNotExist
		}
		st, e := os.Stat(absPath)
		if e != nil || st.IsDir() {
			return "", "", os.ErrNotExist
		}
		return absPath, strings.ToLower(filepath.Ext(absPath)), nil
	}

	if abs, ext, e := try(rel); e == nil {
		return abs, ext, nil
	}
	// 无后缀 URL → 磁盘上可能是 xxx.png（历史上传）
	if filepath.Ext(rel) == "" {
		for _, e := range imageExts {
			if abs, ext, err := try(rel + e); err == nil {
				return abs, ext, nil
			}
		}
	}
	// 带图片后缀的请求若能到达后端也直接试
	return "", "", os.ErrNotExist
}

func serveUploadFile(w http.ResponseWriter, r *http.Request, prefix string) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	rel := strings.TrimPrefix(r.URL.Path, prefix)
	rel = strings.TrimPrefix(rel, "/")
	if rel == "" || strings.HasSuffix(r.URL.Path, "/") {
		// 禁止目录浏览
		http.NotFound(w, r)
		return
	}

	abs, ext, err := resolveUploadFile(rel)
	if err != nil {
		http.NotFound(w, r)
		return
	}

	f, err := os.Open(abs)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		http.NotFound(w, r)
		return
	}

	// 读前 512 字节嗅探类型
	head := make([]byte, 512)
	n, _ := f.Read(head)
	ct := contentTypeFromExt(ext)
	if ct == "" {
		ct = http.DetectContentType(head[:n])
	}
	if ct == "" || ct == "application/octet-stream" {
		if t := contentTypeFromExt(ext); t != "" {
			ct = t
		} else {
			ct = "application/octet-stream"
		}
	}

	// 重新定位后交给 ServeContent
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		http.Error(w, "read error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", ct)
	w.Header().Set("Cache-Control", "public, max-age=604800")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	http.ServeContent(w, r, filepath.Base(abs), st.ModTime(), f)
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

		// 磁盘仍带真实后缀便于运维识别；对外 URL 去掉后缀，避免 CDN 按扩展名劫持
		ext := extFromContentType(ct, hdr.Filename)
		nameNoExt := randomName()
		diskName := nameNoExt + ext
		relDir := filepath.Join(purpose, fmt.Sprintf("%d", pd.UserID))
		absDir := filepath.Join(UploadDir(), relDir)
		if err := os.MkdirAll(absDir, 0o755); err != nil {
			return ctx.JSON(http.StatusInternalServerError, map[string]interface{}{
				"code": 1, "message": "创建目录失败",
			})
		}
		absPath := filepath.Join(absDir, diskName)
		if err := os.WriteFile(absPath, data, 0o644); err != nil {
			return ctx.JSON(http.StatusInternalServerError, map[string]interface{}{
				"code": 1, "message": "保存失败",
			})
		}

		// 对外：无图片扩展名
		urlPath := staticURLPrefix + "/" + filepath.ToSlash(filepath.Join(relDir, nameNoExt))
		return ctx.JSON(http.StatusOK, map[string]interface{}{
			"code":    0,
			"message": "success",
			"url":     urlPath,
		})
	})

	// 自定义静态服务：正确 Content-Type、禁止目录列表、兼容无后缀 URL
	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		path := req.URL.Path
		switch {
		case strings.HasPrefix(path, staticURLPrefix+"/"):
			serveUploadFile(w, req, staticURLPrefix+"/")
		case strings.HasPrefix(path, staticRoutePrefix+"/"):
			serveUploadFile(w, req, staticRoutePrefix+"/")
		case path == staticURLPrefix || path == staticRoutePrefix ||
			path == staticURLPrefix+"/" || path == staticRoutePrefix+"/":
			http.NotFound(w, req)
		default:
			http.NotFound(w, req)
		}
	})
	srv.HandlePrefix(staticRoutePrefix+"/", handler)
	srv.HandlePrefix(staticURLPrefix+"/", handler)
}
