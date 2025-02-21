package version

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
