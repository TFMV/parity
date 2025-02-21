package version

import (
	"testing"
)

func TestVersion(t *testing.T) {
	version := Version
	if version == "" {
		t.Errorf("Version is empty")
	}
}

func TestBuildDate(t *testing.T) {
	buildDate := BuildDate
	if buildDate == "" {
		t.Errorf("BuildDate is empty")
	}
}
