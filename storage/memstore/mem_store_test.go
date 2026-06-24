package memstore

import (
	"testing"

	"github.com/lihongjie/workflow-go/storage/storagetest"
)

func TestStore(t *testing.T) {
	s := New()
	t.Cleanup(func() { s.Close() })
	storagetest.RunStoreTestSuite(t, s)
}
