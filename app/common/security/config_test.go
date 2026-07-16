package security

import (
	"strings"
	"testing"

	"cwxu-algo/app/common/conf"
	_const "cwxu-algo/app/common/const"
	secretutil "cwxu-algo/app/common/utils/secret"
)

func TestConfigureFromServerConfig(t *testing.T) {
	t.Setenv("CWXU_JWT_SECRET", "")
	t.Setenv("CWXU_CONFIG_ENCRYPTION_KEY", "")
	jwtValue := strings.Repeat("j", 32)
	encryptionValue := strings.Repeat("e", 32)
	if err := Configure(&conf.Server{
		JwtSecret: jwtValue, ConfigEncryptionKey: encryptionValue,
	}); err != nil {
		t.Fatal(err)
	}
	if _const.JWTSecret() != jwtValue {
		t.Fatal("JWT config value was not installed")
	}
	encrypted, err := secretutil.Encrypt("value")
	if err != nil || encrypted == "value" {
		t.Fatalf("encryption config value was not installed: %v", err)
	}
}
