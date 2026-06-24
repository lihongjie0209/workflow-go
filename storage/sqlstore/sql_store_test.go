package sqlstore

import (
	"testing"

	"github.com/lihongjie/workflow-go/storage/storagetest"
)

func TestStore(t *testing.T) {
	s := New(WithMemory())
	t.Cleanup(func() { s.Close() })
	storagetest.RunStoreTestSuite(t, s)
}

func TestStoreWithFile(t *testing.T) {
	s := New(WithDBPath("file:test_workflow.db?cache=shared&mode=memory"))
	t.Cleanup(func() { s.Close() })
	storagetest.RunStoreTestSuite(t, s)
}
