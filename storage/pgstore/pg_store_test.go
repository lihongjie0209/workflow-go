package pgstore

import (
	"context"
	"testing"

	"github.com/lihongjie/workflow-go/storage/storagetest"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestPgStore(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping postgres test in short mode")
	}

	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("workflow"),
		postgres.WithUsername("workflow"),
		postgres.WithPassword("workflow"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2),
		),
	)
	if err != nil {
		t.Skipf("Docker/Postgres container not available: %v", err)
	}
	t.Cleanup(func() {
		pgContainer.Terminate(ctx)
	})

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}

	s := New(WithConnString(connStr))
	t.Cleanup(func() { s.Close() })

	storagetest.RunStoreTestSuite(t, s)
}
