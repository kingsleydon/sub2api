package handler

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestHandlersUseUnifiedBillingEligibilityCheck(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate test file")
	}
	handlerDir := filepath.Dir(currentFile)
	entries, err := os.ReadDir(handlerDir)
	if err != nil {
		t.Fatal(err)
	}

	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") || name == "billing_check.go" {
			continue
		}

		path := filepath.Join(handlerDir, name)
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}

		ast.Inspect(file, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "CheckBillingEligibility" {
				return true
			}
			pos := fset.Position(selector.Pos())
			t.Errorf("%s:%d calls CheckBillingEligibility directly; use checkBillingEligibility so Clawdi external billing is preserved", name, pos.Line)
			return true
		})
	}
}
