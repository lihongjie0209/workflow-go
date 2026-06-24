package mysqlstore

import (
	"context"
	"fmt"
	"testing"

	"github.com/lihongjie/workflow-go/storage/storagetest"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestMysqlStore(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping mysql test in short mode")
	}

	ctx := context.Background()

	mysqlContainer, err := mysql.Run(ctx,
		"mysql:8.0",
		mysql.WithDatabase("workflow"),
		mysql.WithUsername("workflow"),
		mysql.WithPassword("workflow"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("port: 3306  MySQL Community Server - GPL").
				WithOccurrence(1),
		),
	)
	if err != nil {
		t.Skipf("Docker/MySQL container not available: %v", err)
	}
	t.Cleanup(func() {
		mysqlContainer.Terminate(ctx)
	})

	connStr, err := mysqlContainer.ConnectionString(ctx, "parseTime=true", "charset=utf8mb4")
	if err != nil {
		t.Fatal(err)
	}

	s := New(WithConnString(connStr))
	t.Cleanup(func() { s.Close() })

	storagetest.RunStoreTestSuite(t, s)
}

// TestMysqlStoreWithCustomPort demonstrates connection with a port override.
func TestMysqlStoreWithCustomPort(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping mysql test in short mode")
	}
	t.Run("default", func(t *testing.T) {
		ctx := context.Background()
		c, err := mysql.Run(ctx,
			"mysql:8.0",
			mysql.WithDatabase("testdb"),
			mysql.WithUsername("test"),
			mysql.WithPassword("test"),
			testcontainers.WithWaitStrategy(
				wait.ForLog("port: 3306  MySQL Community Server - GPL").
					WithOccurrence(1),
			),
		)
		if err != nil {
			t.Skipf("Docker/MySQL not available: %v", err)
		}
		defer c.Terminate(ctx)

		connStr, err := c.ConnectionString(ctx, "parseTime=true")
		if err != nil {
			t.Fatal(err)
		}
		// Verify a simple ping
		store := New(WithConnString(connStr))
		defer store.Close()
		if err := store.db.PingContext(ctx); err != nil {
			t.Fatalf("ping failed: %v", err)
		}
		// Run one subtest
		t.Run("Ping", func(t *testing.T) {
			fmt.Println("MySQL ping OK")
		})
	})
}
