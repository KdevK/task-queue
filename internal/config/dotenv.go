package config

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func loadDotenv(path string) error {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("dotenv: failed to open %s, %w", path, err)
	}
	defer func() {
		_ = file.Close()
	}()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		key, value, found := strings.Cut(line, "=")
		if !found {
			continue
		}

		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		value = strings.Trim(value, `'"`)

		if _, keyExists := os.LookupEnv(key); keyExists {
			continue
		}

		if setErr := os.Setenv(key, value); setErr != nil {
			return fmt.Errorf("dotenv: failed to set %s: %w", key, setErr)
		}
	}

	return scanner.Err()
}
