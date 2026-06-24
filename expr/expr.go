// Package expr provides condition expression evaluation for the workflow engine.
// It wraps the expr-lang/expr library (github.com/expr-lang/expr) to evaluate
// boolean conditions against process variables.
//
// Expression format follows the common ${expression} pattern:
//
//	${amount > 100}
//	${user.role == "admin"}
//	${approved == true || retryCount < 3}
package expr

import (
	"fmt"
	"strings"

	exprlang "github.com/expr-lang/expr"
)

// Condition represents a parsed boolean expression.
// The raw expression string is stored and compiled on each Evaluate() call;
// the expr-lang library maintains its own internal cache for repeated
// compilations of the same input.
type Condition struct {
	raw string
}

// NewCondition parses and validates a condition expression string.
// The expression can optionally be wrapped in ${} (common BPMN syntax).
// Returns an error if the expression is empty or syntactically invalid.
func NewCondition(exprStr string) (*Condition, error) {
	e := strings.TrimSpace(exprStr)
	if e == "" {
		return nil, fmt.Errorf("expr: condition expression is empty")
	}
	if strings.HasPrefix(e, "${") && strings.HasSuffix(e, "}") {
		e = e[2 : len(e)-1]
	}

	// Validate by attempting compilation.
	if _, err := exprlang.Compile(e); err != nil {
		return nil, fmt.Errorf("expr: compile error for %q: %w", exprStr, err)
	}

	return &Condition{raw: e}, nil
}

// Evaluate evaluates the condition against the given variable map.
// Returns the boolean result. Runtime errors (e.g. missing variables,
// type mismatches) are returned as errors rather than silently treated as false.
func (c *Condition) Evaluate(variables map[string]any) (bool, error) {
	env := variables
	if env == nil {
		env = map[string]any{}
	}

	program, err := exprlang.Compile(c.raw)
	if err != nil {
		return false, fmt.Errorf("expr: compile error: %w", err)
	}

	result, err := exprlang.Run(program, env)
	if err != nil {
		return false, fmt.Errorf("expr: eval error: %w", err)
	}

	b, ok := result.(bool)
	if !ok {
		return false, fmt.Errorf("expr: non-boolean result %T(%v)", result, result)
	}
	return b, nil
}

// String returns the raw expression string.
func (c *Condition) String() string {
	return c.raw
}

// EvaluateNumeric evaluates the expression and returns a numeric result.
// Returns 0 and an error if the result is not numeric.
func (c *Condition) EvaluateNumeric(variables map[string]any) (float64, error) {
	env := variables
	if env == nil {
		env = map[string]any{}
	}

	program, err := exprlang.Compile(c.raw)
	if err != nil {
		return 0, fmt.Errorf("expr: compile error: %w", err)
	}

	result, err := exprlang.Run(program, env)
	if err != nil {
		return 0, fmt.Errorf("expr: eval error: %w", err)
	}

	switch v := result.(type) {
	case float64:
		return v, nil
	case int:
		return float64(v), nil
	case int64:
		return float64(v), nil
	default:
		return 0, fmt.Errorf("expr: non-numeric result %T(%v)", result, result)
	}
}

// MustNewCondition is like NewCondition but panics on error.
// Use in tests or when the expression is known to be valid.
func MustNewCondition(exprStr string) *Condition {
	c, err := NewCondition(exprStr)
	if err != nil {
		panic(fmt.Sprintf("expr: MustNewCondition(%q): %v", exprStr, err))
	}
	return c
}
