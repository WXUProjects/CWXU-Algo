package secret

import (
	"strings"
	"testing"
)

func TestEncryptDecrypt(t *testing.T) {
	t.Setenv("CWXU_CONFIG_ENCRYPTION_KEY", strings.Repeat("k", 32))
	encrypted, err := Encrypt("sensitive-value")
	if err != nil {
		t.Fatal(err)
	}
	if encrypted == "sensitive-value" || !strings.HasPrefix(encrypted, prefix) {
		t.Fatalf("secret was not encrypted: %q", encrypted)
	}
	plain, err := Decrypt(encrypted)
	if err != nil {
		t.Fatal(err)
	}
	if plain != "sensitive-value" {
		t.Fatalf("round trip mismatch: %q", plain)
	}
}

func TestEncryptedValueRejectsWrongKey(t *testing.T) {
	t.Setenv("CWXU_CONFIG_ENCRYPTION_KEY", strings.Repeat("a", 32))
	encrypted, err := Encrypt("secret")
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("CWXU_CONFIG_ENCRYPTION_KEY", strings.Repeat("b", 32))
	if _, err := Decrypt(encrypted); err == nil {
		t.Fatal("expected authenticated decryption to fail")
	}
}
