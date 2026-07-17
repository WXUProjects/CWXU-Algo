package notify

import (
	"reflect"
	"testing"
)

func TestExtractMentions(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"hello world", nil},
		{"@alice", []string{"alice"}},
		{"hi @Bob and @alice, also @bob again", []string{"Bob", "alice"}},
		{"email a@b.com not mention; real @user_01 ok", []string{"user_01"}},
		{"@ab @a @verylongusernamethatexceeds32charsxx", []string{"ab"}},
		{"题解里 @coach_zhang 请看", []string{"coach_zhang"}},
	}
	for _, c := range cases {
		got := ExtractMentions(c.in)
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("ExtractMentions(%q)=%v want %v", c.in, got, c.want)
		}
	}
}
