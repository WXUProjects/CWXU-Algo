package backup

import (
	"archive/zip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func zipWrite(srcDir, zipPath string) error {
	if err := os.MkdirAll(filepath.Dir(zipPath), 0o755); err != nil {
		return err
	}
	tmp := zipPath + ".partial"
	_ = os.Remove(tmp)
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	zw := zip.NewWriter(f)
	errWalk := filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		w, err := zw.Create(rel)
		if err != nil {
			return err
		}
		rf, err := os.Open(path)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(w, rf)
		_ = rf.Close()
		return copyErr
	})
	closeErr := zw.Close()
	fileErr := f.Close()
	if errWalk != nil {
		_ = os.Remove(tmp)
		return errWalk
	}
	if closeErr != nil {
		_ = os.Remove(tmp)
		return closeErr
	}
	if fileErr != nil {
		_ = os.Remove(tmp)
		return fileErr
	}
	return os.Rename(tmp, zipPath)
}

func zipRead(zipPath, destDir string) error {
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return err
	}
	defer r.Close()
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		return err
	}
	for _, f := range r.File {
		if err := extractZipFile(f, destDir); err != nil {
			return err
		}
	}
	return nil
}

func extractZipFile(f *zip.File, destDir string) error {
	name := filepath.ToSlash(f.Name)
	if name == "" || strings.HasPrefix(name, "/") || strings.Contains(name, "..") {
		return fmt.Errorf("非法 zip 路径: %s", f.Name)
	}
	target := filepath.Join(destDir, filepath.FromSlash(name))
	// ensure still under destDir
	rel, err := filepath.Rel(destDir, target)
	if err != nil || strings.HasPrefix(rel, "..") {
		return fmt.Errorf("非法 zip 路径: %s", f.Name)
	}
	if f.FileInfo().IsDir() {
		return os.MkdirAll(target, 0o755)
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	rc, err := f.Open()
	if err != nil {
		return err
	}
	defer rc.Close()
	out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, f.Mode())
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, rc)
	closeErr := out.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}
