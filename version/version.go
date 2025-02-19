package version

import "time"

var Version = "0.1"
var BuildDate = "2025-02-20"

func GetVersion() string {
	return Version
}

func GetBuildDate() string {
	return BuildDate
}

func BumpVersion() {
	Version = Version + ".1"
}

func BumpBuildDate() {
	BuildDate = time.Now().Format("2006-01-02")
}
