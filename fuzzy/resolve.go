package fuzzy

import (
	"os"
	"path/filepath"
)

// resolveDirectory returns a full directory path based on the supplied dir path
// if the supplied dir path is absolute (i.e. it starts with / ) then it is
// returned as is, if it's a relative path, then it is assumed to be relative
// to the executable, and that is computed and returned.
//
// if create is true, then the directory path will be created if it doesn't
// already exist
//
// if create is false, then it's upto the caller to ensure it exists and/or
// create it as needed [this won't verify that it exists]
func resolveDirectory(dir string, create bool) (string, error) {
	var resolved string
	if filepath.IsAbs(dir) {
		resolved = dir
	} else {
		execdir, err := filepath.Abs(filepath.Dir(os.Args[0]))
		if err != nil {
			return "", err
		}
		resolved = filepath.Join(execdir, dir)
	}
	if create {
		if _, err := os.Stat(resolved); os.IsNotExist(err) {
			if err := os.MkdirAll(resolved, 0744); err != nil {
				return "", err
			}
		}
	}
	return resolved, nil
}
