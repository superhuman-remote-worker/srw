// Package buildinfo holds what the release build stamps into the binary.
//
// A nested Go module cannot take its version from the repository's vX.Y.Z
// tags, so the release build injects both values with -ldflags -X.
package buildinfo

// Version is the platform release this binary was built for, without a
// leading "v". A build from a checkout says "dev".
var Version = "dev"

// Commit is the source revision the binary was built from.
var Commit = "unknown"
