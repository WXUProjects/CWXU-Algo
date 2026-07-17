package blogaccess

import (
	"reflect"
	"testing"
)

func TestNormalizeVisibility(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"", VisibilityPublic},
		{" PUBLIC ", VisibilityPublic},
		{"private", VisibilityPrivate},
		{"unlisted", VisibilityPrivate},
		{"hidden", VisibilityPrivate},
		{"password", VisibilityPassword},
	}
	for _, c := range cases {
		if got := NormalizeVisibility(c.in); got != c.want {
			t.Errorf("NormalizeVisibility(%q)=%q want %q", c.in, got, c.want)
		}
	}
}

func TestEvaluate_Public(t *testing.T) {
	a := ArticleAccess{Visibility: VisibilityPublic, OwnerID: 10}
	d := Evaluate(a, 0, false)
	if !d.CanSeeMeta || !d.CanSeeBody || d.RequiresPassword {
		t.Fatalf("public anonymous: %+v", d)
	}
	d2 := Evaluate(a, 99, false)
	if !d2.CanSeeBody {
		t.Fatalf("public other user: %+v", d2)
	}
}

func TestEvaluate_Private(t *testing.T) {
	a := ArticleAccess{Visibility: VisibilityPrivate, OwnerID: 10}
	// stranger
	d := Evaluate(a, 99, false)
	if d.CanSeeMeta || d.CanSeeBody {
		t.Fatalf("private stranger should see nothing: %+v", d)
	}
	if d.Reason != "private" {
		t.Fatalf("reason=%q", d.Reason)
	}
	// anonymous
	d0 := Evaluate(a, 0, false)
	if d0.CanSeeBody {
		t.Fatalf("private anon: %+v", d0)
	}
	// owner
	own := Evaluate(a, 10, false)
	if !own.CanSeeMeta || !own.CanSeeBody {
		t.Fatalf("owner must see private: %+v", own)
	}
	// password flag does not open private
	pw := Evaluate(a, 99, true)
	if pw.CanSeeBody {
		t.Fatalf("passwordOK must not open private: %+v", pw)
	}
}

func TestEvaluate_Password(t *testing.T) {
	a := ArticleAccess{Visibility: VisibilityPassword, OwnerID: 10, HasPassword: true}
	// teaser without password
	d := Evaluate(a, 5, false)
	if !d.CanSeeMeta || d.CanSeeBody || !d.RequiresPassword {
		t.Fatalf("password locked: %+v", d)
	}
	if d.Reason != "password_required" {
		t.Fatalf("reason=%q", d.Reason)
	}
	// unlocked
	ok := Evaluate(a, 5, true)
	if !ok.CanSeeBody || ok.RequiresPassword {
		t.Fatalf("password unlocked: %+v", ok)
	}
	// owner without password still full access
	own := Evaluate(a, 10, false)
	if !own.CanSeeBody || own.RequiresPassword {
		t.Fatalf("owner: %+v", own)
	}
}

func TestCanManage(t *testing.T) {
	if CanManage(1, 0, false) {
		t.Fatal("anon")
	}
	if !CanManage(1, 1, false) {
		t.Fatal("owner")
	}
	if CanManage(1, 2, false) {
		t.Fatal("other")
	}
	if !CanManage(1, 2, true) {
		t.Fatal("site admin")
	}
}

func TestExpandSyncOrgIDs_PrivateImpliesPublic(t *testing.T) {
	isSystem := func(id uint) bool { return id == 1 }
	// only private orgs
	got := ExpandSyncOrgIDs([]uint{3, 5}, 1, isSystem)
	want := []uint{1, 3, 5}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
	// already has public
	got2 := ExpandSyncOrgIDs([]uint{3, 1}, 1, isSystem)
	if !reflect.DeepEqual(got2, []uint{3, 1}) {
		t.Fatalf("got2 %v", got2)
	}
	// only public — no expansion needed
	got3 := ExpandSyncOrgIDs([]uint{1}, 1, isSystem)
	if !reflect.DeepEqual(got3, []uint{1}) {
		t.Fatalf("got3 %v", got3)
	}
	// empty
	if ExpandSyncOrgIDs(nil, 1, isSystem) != nil {
		t.Fatal("nil selected")
	}
	// dedupe zeros
	got4 := ExpandSyncOrgIDs([]uint{0, 3, 3, 0}, 1, isSystem)
	if !reflect.DeepEqual(got4, []uint{1, 3}) {
		t.Fatalf("got4 %v", got4)
	}
}

func TestThemeEnabled(t *testing.T) {
	if ThemeEnabled(false, nil) {
		t.Fatal("default off")
	}
	if !ThemeEnabled(true, nil) {
		t.Fatal("global all")
	}
	f := false
	if ThemeEnabled(true, &f) {
		t.Fatal("per-user false overrides global")
	}
	tr := true
	if !ThemeEnabled(false, &tr) {
		t.Fatal("per-user true overrides global off")
	}
}

func TestValidVisibility(t *testing.T) {
	if !ValidVisibility("public") || !ValidVisibility("private") || !ValidVisibility("password") {
		t.Fatal("expected valid")
	}
	if ValidVisibility("nope") {
		t.Fatal("invalid")
	}
}
