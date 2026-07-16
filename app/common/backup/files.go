package backup

import (
	"io"
	"os"
	"path/filepath"
	"strings"
)

// UploadDir mirrors user service upload path resolution.
func UploadDir() string {
	if d := os.Getenv("CWXU_UPLOAD_DIR"); d != "" {
		return d
	}
	return "./data/uploads"
}

// BackupDir is where completed zip archives and job workspaces live.
func BackupDir() string {
	if d := os.Getenv("CWXU_BACKUP_DIR"); d != "" {
		return d
	}
	return "./data/backups"
}

// CopyUploadTree copies all files under src into dest, preserving relative paths.
// Returns number of files copied. Missing src is OK (0 files).
func CopyUploadTree(src, dest string) (int, error) {
	st, err := os.Stat(src)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	if !st.IsDir() {
		return 0, nil
	}
	n := 0
	err = filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		// skip hidden junk
		if strings.HasPrefix(filepath.Base(rel), ".") {
			return nil
		}
		target := filepath.Join(dest, rel)
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		if err := copyFile(path, target); err != nil {
			return err
		}
		n++
		return nil
	})
	return n, err
}

// ReplaceUploadTree removes dest contents (if any) then copies src → dest.
func ReplaceUploadTree(src, dest string) error {
	if err := os.MkdirAll(dest, 0o755); err != nil {
		return err
	}
	// remove children of dest only (keep the directory mount point)
	entries, err := os.ReadDir(dest)
	if err != nil {
		return err
	}
	for _, e := range entries {
		_ = os.RemoveAll(filepath.Join(dest, e.Name()))
	}
	_, err = CopyUploadTree(src, dest)
	return err
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, in)
	closeErr := out.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}
