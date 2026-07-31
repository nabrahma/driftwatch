package cli

import (
	"encoding/json"
	"runtime"

	"github.com/spf13/cobra"
)

// versionInfo is the shape `--output json` emits.
type versionInfo struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildDate string `json:"buildDate"`
	GoVersion string `json:"goVersion"`
	Platform  string `json:"platform"`
}

func newVersionCommand(env *Env, g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version, commit, build date and Go version",
		Example: trim(`
  driftwatch version
  driftwatch version -o json | jq -r .commit`),
		Args: cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			return runVersion(env, g)
		},
	}
}

func runVersion(env *Env, g *globalFlags) error {
	info := versionInfo{
		Version:   orUnknown(env.Version),
		Commit:    orUnknown(env.Commit),
		BuildDate: orUnknown(env.Date),
		GoVersion: runtime.Version(),
		Platform:  runtime.GOOS + "/" + runtime.GOARCH,
	}

	if g.output == OutputJSON {
		doc, err := json.MarshalIndent(info, "", "  ")
		if err != nil {
			return exitWith(ExitFatal, err)
		}
		return exitWith(ExitFatal, writeJSON(env, doc))
	}

	env.printf("driftwatch %s\n", info.Version)
	env.printf("  commit     %s\n", info.Commit)
	env.printf("  built      %s\n", info.BuildDate)
	env.printf("  go         %s\n", info.GoVersion)
	env.printf("  platform   %s\n", info.Platform)
	return nil
}

// orUnknown keeps the output honest about a build that did not go through the
// release pipeline, rather than printing an empty field.
func orUnknown(s string) string {
	if s == "" {
		return "unknown"
	}
	return s
}
