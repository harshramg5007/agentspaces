package migrations

import "testing"

func TestParseMigrationSQL_DefaultsToTransactional(t *testing.T) {
	sql, transactional := parseMigrationSQL("CREATE TABLE example(id INT);\n")
	if !transactional {
		t.Fatalf("expected migration to default to transactional")
	}
	if sql != "CREATE TABLE example(id INT);\n" {
		t.Fatalf("unexpected SQL body: %q", sql)
	}
}

func TestParseMigrationSQL_StripsNonTransactionalDirective(t *testing.T) {
	raw := "-- migrate:nontransactional\nCREATE INDEX CONCURRENTLY idx_example ON example(id);\n"
	sql, transactional := parseMigrationSQL(raw)
	if transactional {
		t.Fatalf("expected migration to be marked non-transactional")
	}
	if sql != "CREATE INDEX CONCURRENTLY idx_example ON example(id);\n" {
		t.Fatalf("unexpected SQL body: %q", sql)
	}
}

func TestCountTopLevelSQLStatements(t *testing.T) {
	t.Run("counts a single statement with dollar quotes", func(t *testing.T) {
		count, err := countTopLevelSQLStatements("DO $$ BEGIN RAISE NOTICE 'ok'; END $$;\n")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if count != 1 {
			t.Fatalf("expected 1 statement, got %d", count)
		}
	})

	t.Run("counts multiple top level statements", func(t *testing.T) {
		count, err := countTopLevelSQLStatements("CREATE INDEX idx_a ON a(id);\nDROP INDEX idx_b;\n")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if count != 2 {
			t.Fatalf("expected 2 statements, got %d", count)
		}
	})
}

func TestValidateSpecRejectsMultiStatementNonTransactionalPostgresMigration(t *testing.T) {
	err := validateSpec(Spec{
		Backend:       BackendPostgres,
		File:          "postgres/9999_bad.up.sql",
		SQL:           "CREATE INDEX CONCURRENTLY idx_a ON a(id);\nDROP INDEX CONCURRENTLY idx_b;\n",
		Transactional: false,
	})
	if err == nil {
		t.Fatal("expected validation error")
	}
}
