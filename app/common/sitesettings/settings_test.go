package sitesettings

import (
	"context"
	"testing"
)

func TestRuntimeWorthCaching(t *testing.T) {
	if (&Runtime{SiteTitle: "GoAlgo"}).worthCaching() {
		t.Fatal("title-only should not be cached")
	}
	if !(&Runtime{SMTPHost: "smtp.example.com"}).worthCaching() {
		t.Fatal("SMTP host should be cacheable")
	}
	if !(&Runtime{AiAnalyzeEndpoint: "http://x"}).worthCaching() {
		t.Fatal("AI endpoint should be cacheable")
	}
	if !(&Runtime{AgentModel: "gpt"}).worthCaching() {
		t.Fatal("agent model should be cacheable")
	}
}

func TestHasSMTP(t *testing.T) {
	if (*Runtime)(nil).HasSMTP() {
		t.Fatal("nil should not have SMTP")
	}
	if (&Runtime{}).HasSMTP() {
		t.Fatal("empty should not have SMTP")
	}
	if !(&Runtime{SMTPHost: "smtp.example.com"}).HasSMTP() {
		t.Fatal("host set should have SMTP")
	}
}

func TestLoadNilDBNoPanic(t *testing.T) {
	// Redis miss + db=nil（core_data 路径）：返回空 Runtime，不得 panic
	rt := Load(context.Background(), nil, nil)
	if rt == nil {
		t.Fatal("expected non-nil runtime")
	}
	if rt.HasSMTP() {
		t.Fatal("expected no SMTP without redis/db")
	}
	if rt.SiteTitle != "GoAlgo" {
		t.Fatalf("site title = %q", rt.SiteTitle)
	}
}

func TestPublishRedisNilSafe(t *testing.T) {
	if err := PublishRedis(context.Background(), nil, &Runtime{SMTPHost: "x"}); err != nil {
		t.Fatal(err)
	}
	if err := PublishRedis(context.Background(), nil, nil); err != nil {
		t.Fatal(err)
	}
}
