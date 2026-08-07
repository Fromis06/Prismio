package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

// AccountsFile defines the SHARED account registry for the entire system. This is
// distinct from OverrideConfig, which contains per-user operational settings.
// This file must be separate because it needs to be read BEFORE a user logs in
// to perform authentication; it cannot reside in a per-user config file.
type AccountsFile struct {
	// Key: SHA-256 hashed API key. Value: The corresponding username.
	HashedAPIKeys map[string]string `yaml:"hashed_api_keys"`
}

// LoadAccounts reads the account registry from disk.
func LoadAccounts(path string) (*AccountsFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var af AccountsFile
	if err := yaml.Unmarshal(data, &af); err != nil {
		return nil, err
	}
	if af.HashedAPIKeys == nil {
		af.HashedAPIKeys = make(map[string]string)
	}
	return &af, nil
}

// SaveAccounts saves the account registry to disk using a temporary file and rename
// mechanism to prevent file corruption if the process is terminated mid-write.
func SaveAccounts(path string, af *AccountsFile) error {
	if af.HashedAPIKeys == nil {
		af.HashedAPIKeys = make(map[string]string)
	}
	data, err := yaml.Marshal(af)
	if err != nil {
		return err
	}
	tempPath := path + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return err
	}
	return os.Rename(tempPath, path)
}
