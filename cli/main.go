// Command srw is the command-line client of Superhuman Remote Worker.
package main

import (
	"os"

	"github.com/superhuman-remote-worker/srw/cli/internal/cmd"
)

func main() {
	os.Exit(cmd.Main())
}
