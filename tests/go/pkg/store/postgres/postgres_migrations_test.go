package postgres

import "testing"

func TestMigrationChecksumAccepted(t *testing.T) {
	t.Run("accepts current checksum", func(t *testing.T) {
		if !migrationChecksumAccepted(6, "current", "current") {
			t.Fatal("expected current checksum to be accepted")
		}
	})

	t.Run("accepts legacy rewritten migration checksum", func(t *testing.T) {
		if !migrationChecksumAccepted(
			6,
			"5776530a74c7b9f65d49b207bd1555c6f89927245dd1e3f2651b6ef77f38646c",
			"new-checksum",
		) {
			t.Fatal("expected legacy checksum to be accepted")
		}
	})

	t.Run("rejects unknown checksum", func(t *testing.T) {
		if migrationChecksumAccepted(6, "unknown", "new-checksum") {
			t.Fatal("expected unknown checksum to be rejected")
		}
	})

	t.Run("accepts legacy version 7 checksum", func(t *testing.T) {
		if !migrationChecksumAccepted(
			7,
			"d01aa52ff1458434284c74fc4c8ba6cb78f9484acfc4f65fab74587944b29aa8",
			"new-checksum",
		) {
			t.Fatal("expected legacy version 7 checksum to be accepted")
		}
	})
}
