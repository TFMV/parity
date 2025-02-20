package version

import "time"

// Version and BuildDate are populated at build time using -ldflags.
var Version = "dev"
var BuildDate = "unknown"

// GetVersion returns the current application version.
func GetVersion() string {
	return Version
}

// GetBuildDate returns the build date.
func GetBuildDate() string {
	return BuildDate
}

// BumpVersion is NOT typically used in Go. Versioning should be managed via Git tags and build metadata.
func BumpVersion() {
	Version = Version + ".1"
}

// BumpBuildDate updates the build date, though this is typically set at build time.
func BumpBuildDate() {
	BuildDate = time.Now().Format("2006-01-02")
}
