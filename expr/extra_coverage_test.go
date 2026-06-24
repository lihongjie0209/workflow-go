package expr

import (
	"testing"
)

func TestCondition_EvaluateNumeric(t *testing.T) {
	vars := map[string]any{"x": 42}

	tests := []struct {
		name    string
		expr    string
		want    float64
		wantErr bool
	}{
		{"int literal", "5", 5, false},
		{"float literal", "3.14", 3.14, false},
		{"variable", "x", 42, false},
		{"arithmetic", "10 + 20", 30, false},
		{"bool (non-numeric)", "true", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := NewCondition(tt.expr)
			if err != nil { t.Fatalf("NewCondition: %v", err) }
			got, err := c.EvaluateNumeric(vars)
			if (err != nil) != tt.wantErr { t.Errorf("EvaluateNumeric error=%v, wantErr=%v", err, tt.wantErr) }
			if !tt.wantErr && got != tt.want { t.Errorf("got %v, want %v", got, tt.want) }
		})
	}
}

func TestCondition_String(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"${amount > 100}", "amount > 100"},
		{"true", "true"},
		{"x + y", "x + y"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			c, err := NewCondition(tt.input)
			if err != nil { t.Fatalf("NewCondition: %v", err) }
			if c.String() != tt.want { t.Errorf("String()=%q, want %q", c.String(), tt.want) }
		})
	}
}
