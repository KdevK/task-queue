package config

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadDotenv_EmptyFile(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, ".env")
	envFile, err := os.Create(filePath)
	if err != nil {
		t.Fatalf("failed to create .env file: %v", err)
	}
	defer func() {
		_ = envFile.Close()
	}()

	loadErr := loadDotenv(filePath)
	if loadErr != nil {
		t.Fatalf("failed to load .env file: %v", loadErr)
	}
}

func TestLoadDotenv_NonExistentPath(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, ".env")

	loadErr := loadDotenv(filePath)
	if loadErr != nil {
		t.Fatalf("failed to load .env file: %v", loadErr)
	}
}

func TestLoadDotenv_Table(t *testing.T) {
	const key = "DOTENV_TEST"

	tests := []struct {
		name       string
		fullEnvStr string
		expected   string
	}{
		{
			name:       "empty string",
			fullEnvStr: "",
			expected:   "",
		},
		{
			name:       "key=value",
			fullEnvStr: fmt.Sprintf("%v=VALUE", key),
			expected:   "VALUE",
		},
		{
			name:       "whitespaces around =",
			fullEnvStr: fmt.Sprintf("%v = VALUE", key),
			expected:   "VALUE",
		},
		{
			name:       "comment after value with whitespace",
			fullEnvStr: fmt.Sprintf("%v=VALUE # TEST", key),
			expected:   "VALUE",
		},
		{
			name:       "comment after value with tab",
			fullEnvStr: fmt.Sprintf("%v=VALUE\t# TEST", key),
			expected:   "VALUE",
		},
		{
			name:       "only comment",
			fullEnvStr: "# comment",
			expected:   "",
		},
		{
			name:       "no = between key and value",
			fullEnvStr: fmt.Sprintf("%v VALUE", key),
			expected:   "",
		},
		{
			name:       "single quotes",
			fullEnvStr: fmt.Sprintf(`%v='VALUE'`, key),
			expected:   "VALUE",
		},
		{
			name:       "double quotes",
			fullEnvStr: fmt.Sprintf(`%v="VALUE"`, key),
			expected:   "VALUE",
		},
		{
			name:       "# inside of quotes",
			fullEnvStr: fmt.Sprintf(`%v="VALUE #HASH"`, key),
			expected:   "VALUE #HASH",
		},
		{
			name:       "# outside of quotes",
			fullEnvStr: fmt.Sprintf(`%v="VALUE" #HASH`, key),
			expected:   "VALUE",
		},
		{
			name:       "# inside of value without preceding whitespace",
			fullEnvStr: fmt.Sprintf("%v=HASH#HASH", key),
			expected:   "HASH#HASH",
		},
		{
			name:       "key's value is double quote",
			fullEnvStr: fmt.Sprintf(`%v="`, key),
			expected:   "\"",
		},
		{
			name:       "key's value is single quote",
			fullEnvStr: fmt.Sprintf(`%v='`, key),
			expected:   "'",
		},
		{
			name:       "mismatched quotes not stripped",
			fullEnvStr: fmt.Sprintf(`%v='VALUE"`, key),
			expected:   `'VALUE"`,
		},
	}

	unsetErr := os.Unsetenv(key)
	if unsetErr != nil {
		t.Fatalf("failed to unset env value for test key: %v", unsetErr)
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				unsetErr = os.Unsetenv(key)
				if unsetErr != nil {
					t.Fatalf("failed to unset env value for test key: %v", unsetErr)
				}
			}()

			dir := t.TempDir()
			filePath := filepath.Join(dir, ".env")
			envFile, err := os.Create(filePath)
			if err != nil {
				t.Fatalf("failed to create .env file: %v", err)
			}
			defer func() {
				_ = envFile.Close()
			}()

			_, writeErr := envFile.Write([]byte(tc.fullEnvStr))
			if writeErr != nil {
				t.Fatalf("failed to write data to .env file: %v", writeErr)
			}

			loadErr := loadDotenv(filePath)
			if loadErr != nil {
				t.Fatalf("failed to load .env file: %v", loadErr)
			}

			got := os.Getenv(key)
			if got != tc.expected {
				t.Fatalf("expected %v, got %v", tc.expected, got)
			}
		})
	}
}

func TestLoadDotenv_ExistingEnvNotOverridden(t *testing.T) {
	const key = "DOTENV_TEST"
	const value = "VALUE"
	defer func() {
		unsetErr := os.Unsetenv(key)
		if unsetErr != nil {
			t.Fatalf("failed to unset env value for test key: %v", unsetErr)
		}
	}()

	setErr := os.Setenv(key, value)
	if setErr != nil {
		t.Fatalf("failed to set env value for test key: %v", setErr)
	}

	dir := t.TempDir()
	filePath := filepath.Join(dir, ".env")
	envFile, err := os.Create(filePath)
	if err != nil {
		t.Fatalf("failed to create .env file: %v", err)
	}
	defer func() {
		_ = envFile.Close()
	}()

	strToWrite := fmt.Sprintf("%v=NEW_VALUE", key)
	_, writeErr := envFile.Write([]byte(strToWrite))
	if writeErr != nil {
		t.Fatalf("failed to write data to .env file: %v", writeErr)
	}

	loadErr := loadDotenv(filePath)
	if loadErr != nil {
		t.Fatalf("failed to load .env file: %v", loadErr)
	}

	got := os.Getenv(key)
	if got != value {
		t.Fatalf("expected %v, got %v", value, got)
	}
}
