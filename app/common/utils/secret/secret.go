package secret

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
)

const prefix = "enc:v1:"

var (
	configuredKeyMu sync.RWMutex
	configuredKey   string
)

// ConfigureKey sets the config.yaml value. The environment variable remains
// an optional higher-priority override for container secret injection.
func ConfigureKey(value string) error {
	if env := strings.TrimSpace(os.Getenv("CWXU_CONFIG_ENCRYPTION_KEY")); env != "" {
		value = env
	}
	value = strings.TrimSpace(value)
	if len(value) < 32 {
		return errors.New("server.config_encryption_key must contain at least 32 characters")
	}
	configuredKeyMu.Lock()
	configuredKey = value
	configuredKeyMu.Unlock()
	return nil
}

func key() ([]byte, error) {
	raw := strings.TrimSpace(os.Getenv("CWXU_CONFIG_ENCRYPTION_KEY"))
	if raw == "" {
		configuredKeyMu.RLock()
		raw = configuredKey
		configuredKeyMu.RUnlock()
	}
	if len(raw) < 32 {
		return nil, errors.New("server.config_encryption_key must contain at least 32 characters")
	}
	sum := sha256.Sum256([]byte(raw))
	return sum[:], nil
}

func Configured() bool {
	_, err := key()
	return err == nil
}

// Fingerprint returns a short stable id of the active encryption key (hex of
// first 8 bytes of SHA256). Used by site backup to ensure import uses the same key.
func Fingerprint() string {
	raw := strings.TrimSpace(os.Getenv("CWXU_CONFIG_ENCRYPTION_KEY"))
	if raw == "" {
		configuredKeyMu.RLock()
		raw = configuredKey
		configuredKeyMu.RUnlock()
	}
	if raw == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(raw))
	return fmt.Sprintf("%x", sum[:8])
}

// Encrypt encrypts an application secret with AES-GCM. Already encrypted values
// are returned unchanged so retries and whole-form saves are idempotent.
func Encrypt(plain string) (string, error) {
	if plain == "" || strings.HasPrefix(plain, prefix) {
		return plain, nil
	}
	k, err := key()
	if err != nil {
		return "", err
	}
	block, err := aes.NewCipher(k)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}
	ciphertext := gcm.Seal(nil, nonce, []byte(plain), nil)
	payload := append(nonce, ciphertext...)
	return prefix + base64.RawStdEncoding.EncodeToString(payload), nil
}

// Decrypt accepts legacy plaintext to support a rolling migration. The user
// service rewrites those values encrypted when the encryption key is configured.
func Decrypt(value string) (string, error) {
	if value == "" || !strings.HasPrefix(value, prefix) {
		return value, nil
	}
	k, err := key()
	if err != nil {
		return "", err
	}
	payload, err := base64.RawStdEncoding.DecodeString(strings.TrimPrefix(value, prefix))
	if err != nil {
		return "", fmt.Errorf("decode encrypted secret: %w", err)
	}
	block, err := aes.NewCipher(k)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	if len(payload) < gcm.NonceSize() {
		return "", errors.New("encrypted secret is truncated")
	}
	plain, err := gcm.Open(nil, payload[:gcm.NonceSize()], payload[gcm.NonceSize():], nil)
	if err != nil {
		return "", errors.New("encrypted secret authentication failed")
	}
	return string(plain), nil
}
