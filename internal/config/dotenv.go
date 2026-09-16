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

		// removes hash symbol # that is used for comments
		// e.g. KEY=VALUE # comment -> KEY=VALUE
		// but hash symbol used in values is NOT removed
		// e.g. PASSWORD=Q1E#E2Q! doesn't change
		if !(strings.HasPrefix(value, `'`) && strings.HasSuffix(value, `'`)) &&
			!(strings.HasPrefix(value, `"`) && strings.HasSuffix(value, `"`)) {
			for i := 0; i < len(value); i++ {
				if value[i] == '#' {
					if i > 0 && (value[i-1] == ' ' || value[i-1] == '\t') {
						value = value[:i]
						break
					}
				}
			}
			value = strings.TrimSpace(value)
		}

		// removes double quotes
		// e.g. KEY="VALUE" -> KEY=VALUE
		if len(value) >= 2 &&
			((strings.HasPrefix(value, `'`) && strings.HasSuffix(value, `'`)) ||
				(strings.HasPrefix(value, `"`) && strings.HasSuffix(value, `"`))) {
			value = value[1 : len(value)-1]
		}

		if _, keyExists := os.LookupEnv(key); keyExists {
			continue
		}

		if setErr := os.Setenv(key, value); setErr != nil {
			return fmt.Errorf("dotenv: failed to set %s: %w", key, setErr)
		}
	}

	return scanner.Err()
}
