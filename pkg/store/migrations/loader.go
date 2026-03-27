package migrations

import (
	"crypto/sha256"
	"embed"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

const (
	DirectionUp   = "up"
	DirectionDown = "down"

	BackendPostgres = "postgres"
	BackendSQLite   = "sqlite"
)

// SchemaTargetVersion is the schema version targeted by the current binary (N).
const SchemaTargetVersion int64 = 17

// SchemaMinCompatVersion is the minimum schema version accepted by the current binary (N-1).
const SchemaMinCompatVersion int64 = SchemaTargetVersion - 1

//go:embed postgres/*.sql sqlite/*.sql
var migrationFiles embed.FS

// Spec describes one migration file.
type Spec struct {
	Backend       string `json:"backend"`
	Version       int64  `json:"version"`
	Name          string `json:"name"`
	Direction     string `json:"direction"`
	File          string `json:"file"`
	SQL           string `json:"-"`
	Checksum      string `json:"checksum"`
	Transactional bool   `json:"transactional"`
}

// Load returns ordered migration specs for one backend and direction.
func Load(backend string, direction string) ([]Spec, error) {
	backend = strings.ToLower(strings.TrimSpace(backend))
	direction = strings.ToLower(strings.TrimSpace(direction))
	if backend != BackendPostgres && backend != BackendSQLite {
		return nil, fmt.Errorf("unsupported migration backend %q", backend)
	}
	if direction != DirectionUp && direction != DirectionDown {
		return nil, fmt.Errorf("unsupported migration direction %q", direction)
	}

	pattern := fmt.Sprintf("%s/*.%s.sql", backend, direction)
	files, err := fs.Glob(migrationFiles, pattern)
	if err != nil {
		return nil, fmt.Errorf("glob migrations: %w", err)
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no migration files found for backend=%s direction=%s", backend, direction)
	}

	specs := make([]Spec, 0, len(files))
	for _, file := range files {
		base := path.Base(file)
		parts := strings.Split(base, ".")
		if len(parts) != 3 {
			return nil, fmt.Errorf("invalid migration filename %q", base)
		}
		stem := parts[0]
		stemParts := strings.SplitN(stem, "_", 2)
		if len(stemParts) != 2 {
			return nil, fmt.Errorf("invalid migration filename %q", base)
		}
		version, err := strconv.ParseInt(stemParts[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse migration version from %q: %w", base, err)
		}
		name := stemParts[1]
		raw, err := fs.ReadFile(migrationFiles, file)
		if err != nil {
			return nil, fmt.Errorf("read migration %q: %w", file, err)
		}
		sqlText, transactional := parseMigrationSQL(string(raw))
		checksumBytes := sha256.Sum256(raw)
		specs = append(specs, Spec{
			Backend:       backend,
			Version:       version,
			Name:          name,
			Direction:     direction,
			File:          file,
			SQL:           sqlText,
			Checksum:      fmt.Sprintf("%x", checksumBytes[:]),
			Transactional: transactional,
		})
	}

	for _, spec := range specs {
		if err := validateSpec(spec); err != nil {
			return nil, err
		}
	}

	sort.Slice(specs, func(i, j int) bool {
		if direction == DirectionDown {
			if specs[i].Version == specs[j].Version {
				return specs[i].Name < specs[j].Name
			}
			return specs[i].Version > specs[j].Version
		}
		if specs[i].Version == specs[j].Version {
			return specs[i].Name < specs[j].Name
		}
		return specs[i].Version < specs[j].Version
	})

	return specs, nil
}

func parseMigrationSQL(raw string) (string, bool) {
	transactional := true
	lines := strings.Split(raw, "\n")
	filtered := make([]string, 0, len(lines))
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "-- migrate:nontransactional" {
			transactional = false
			continue
		}
		filtered = append(filtered, line)
	}
	return strings.Join(filtered, "\n"), transactional
}

func validateSpec(spec Spec) error {
	if spec.Backend != BackendPostgres || spec.Transactional {
		return nil
	}
	count, err := countTopLevelSQLStatements(spec.SQL)
	if err != nil {
		return fmt.Errorf("validate migration %q: %w", spec.File, err)
	}
	if count != 1 {
		return fmt.Errorf(
			"validate migration %q: non-transactional postgres migrations must contain exactly 1 top-level statement, found %d",
			spec.File,
			count,
		)
	}
	return nil
}

func countTopLevelSQLStatements(sqlText string) (int, error) {
	count := 0
	hasContent := false
	blockCommentDepth := 0
	lineComment := false
	dollarQuote := ""

	for i := 0; i < len(sqlText); {
		if lineComment {
			if sqlText[i] == '\n' {
				lineComment = false
			}
			i++
			continue
		}
		if blockCommentDepth > 0 {
			switch {
			case strings.HasPrefix(sqlText[i:], "/*"):
				blockCommentDepth++
				i += 2
			case strings.HasPrefix(sqlText[i:], "*/"):
				blockCommentDepth--
				i += 2
			default:
				i++
			}
			continue
		}
		if dollarQuote != "" {
			if strings.HasPrefix(sqlText[i:], dollarQuote) {
				i += len(dollarQuote)
				dollarQuote = ""
				continue
			}
			i++
			continue
		}
		switch {
		case strings.HasPrefix(sqlText[i:], "--"):
			lineComment = true
			i += 2
		case strings.HasPrefix(sqlText[i:], "/*"):
			blockCommentDepth = 1
			i += 2
		case sqlText[i] == '\'':
			hasContent = true
			next, err := scanQuotedString(sqlText, i, '\'')
			if err != nil {
				return 0, err
			}
			i = next
		case sqlText[i] == '"':
			hasContent = true
			next, err := scanQuotedString(sqlText, i, '"')
			if err != nil {
				return 0, err
			}
			i = next
		case sqlText[i] == '$':
			tag, ok := matchDollarQuote(sqlText[i:])
			if ok {
				hasContent = true
				dollarQuote = tag
				i += len(tag)
				continue
			}
			hasContent = true
			i++
		case sqlText[i] == ';':
			if hasContent {
				count++
				hasContent = false
			}
			i++
		default:
			if !unicode.IsSpace(rune(sqlText[i])) {
				hasContent = true
			}
			i++
		}
	}

	if lineComment || blockCommentDepth > 0 || dollarQuote != "" {
		return 0, fmt.Errorf("unterminated SQL construct")
	}
	if hasContent {
		count++
	}
	return count, nil
}

func scanQuotedString(sqlText string, start int, quote byte) (int, error) {
	i := start + 1
	for i < len(sqlText) {
		if sqlText[i] != quote {
			i++
			continue
		}
		if i+1 < len(sqlText) && sqlText[i+1] == quote {
			i += 2
			continue
		}
		return i + 1, nil
	}
	return 0, fmt.Errorf("unterminated quoted string")
}

func matchDollarQuote(sqlText string) (string, bool) {
	if !strings.HasPrefix(sqlText, "$") {
		return "", false
	}
	end := strings.IndexByte(sqlText[1:], '$')
	if end < 0 {
		return "", false
	}
	end++
	tag := sqlText[:end+1]
	for _, ch := range tag[1 : len(tag)-1] {
		if !unicode.IsLetter(ch) && !unicode.IsDigit(ch) && ch != '_' {
			return "", false
		}
	}
	return tag, true
}
