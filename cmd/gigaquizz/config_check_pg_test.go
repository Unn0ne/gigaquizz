package main

import "testing"

func TestDryConfigurationRejectsInvalidPostgresSettings(t *testing.T) {
	for name, overrides := range map[string]map[string]string{
		"schema":     {"GIGAQUIZZ_SCHEMA": "bad-name"},
		"connection": {"DATABASE_URL": "postgres://%"},
		"quorum":     {"DURABILITY_REQUIRED_STANDBYS": "1", "DURABILITY_STANDBY_NAMES": ""},
	} {
		t.Run(name, func(t *testing.T) {
			env, data := startupFixture(t, overrides)
			if _, err := runStartup("-env", env, "-check-config"); err == nil {
				t.Fatal("invalid PG configuration accepted")
			}
			requireUntouchedStore(t, data)
		})
	}
}

func TestDryConfigurationCannotRunMigrations(t *testing.T) {
	env, data := startupFixture(t, nil)
	if _, err := runStartup("-env", env, "-check-config", "-migrate-only"); err == nil {
		t.Fatal("dry configuration allowed migrations")
	}
	requireUntouchedStore(t, data)
}
