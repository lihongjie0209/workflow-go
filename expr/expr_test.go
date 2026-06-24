package expr

import (
	"testing"
)

func TestNewCondition(t *testing.T) {
	tests := []struct {
		name    string
		expr    string
		wantErr bool
	}{
		{"simple comparison", "${amount > 100}", false},
		{"without wrapper", "amount > 100", false},
		{"empty string", "", true},
		{"whitespace only", "   ", true},
		{"logical expression", "${approved && total > 0}", false},
		{"string comparison", `user.role == "admin"`, false},
		{"arithmetic", "${price * quantity > 1000}", false},
		{"ternary", `score > 60 ? "pass" : "fail"`, false},
		{"invalid syntax", "${>>>}", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := NewCondition(tt.expr)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewCondition() error = %v, wantErr = %v", err, tt.wantErr)
				return
			}
			if err == nil && c == nil {
				t.Error("NewCondition() returned nil condition without error")
			}
		})
	}
}

func TestCondition_Evaluate(t *testing.T) {
	vars := map[string]any{
		"amount":   250,
		"total":    5,
		"approved": true,
		"name":     "test",
		"user":     map[string]any{"role": "admin"},
		"score":    85,
	}

	tests := []struct {
		name    string
		expr    string
		want    bool
		wantErr bool
	}{
		{"true comparison", "${amount > 100}", true, false},
		{"false comparison", "${amount < 100}", false, false},
		{"equals true", `${approved == true}`, true, false},
		{"equals false", `${approved == false}`, false, false},
		{"and both true", "${approved && amount > 100}", true, false},
		{"and one false", "${approved && amount < 100}", false, false},
		{"or both false", "${amount < 100 || total > 10}", false, false},
		{"or one true", "${amount < 100 || total == 5}", true, false},
		{"string equality", `${name == "test"}`, true, false},
		{"string inequality", `${name == "other"}`, false, false},
		{"nested map access", `${user.role == "admin"}`, true, false},
		{"nested map false", `${user.role == "guest"}`, false, false},
		{"greater or equal", "${score >= 85}", true, false},
		{"less than", "${score < 60}", false, false},
		{"not operator", "${!approved}", false, false},
		{"nil check", "${name != nil}", true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := NewCondition(tt.expr)
			if err != nil {
				t.Fatalf("NewCondition() error = %v", err)
			}
			got, err := c.Evaluate(vars)
			if (err != nil) != tt.wantErr {
				t.Errorf("Evaluate() error = %v, wantErr = %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("Evaluate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCondition_Evaluate_NilVars(t *testing.T) {
	c := MustNewCondition("true")
	got, err := c.Evaluate(nil)
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	if got != true {
		t.Errorf("Evaluate() with nil vars = %v, want %v", got, true)
	}
}

func TestCondition_Evaluate_NonBoolResult(t *testing.T) {
	c := MustNewCondition("1 + 1")
	_, err := c.Evaluate(nil)
	if err == nil {
		t.Error("expected error for non-boolean result")
	}
}

func TestMustNewCondition_Panics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("MustNewCondition should panic on invalid expression")
		}
	}()
	MustNewCondition("")
}
