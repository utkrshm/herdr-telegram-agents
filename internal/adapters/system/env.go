// Package system is the adapter for the process environment: the HERDR_*
// variables Herdr passes to plugin commands, process spawning and signals.
// It is one of the two packages allowed to read environment variables.
package system

import (
	"fmt"
	"os"
	"path/filepath"
)

// DefaultPluginID is used when Herdr does not pass HERDR_PLUGIN_ID (older
// versions, or a developer running the binary by hand inside a pane).
const DefaultPluginID = "permgps.telegram-agents"

// PluginEnv is the plugin's view of the environment Herdr provides.
type PluginEnv struct {
	// PluginID is the manifest id, needed to open the plugin's own panes.
	PluginID string
	// Root is the plugin checkout; never used for state.
	Root string
	// ConfigDir holds config.json.
	ConfigDir string
	// StateDir holds mapping.json, the pid file and the logs.
	StateDir string
	// SocketPath is the Herdr NDJSON socket (or named pipe on Windows).
	SocketPath string
	// BinPath is the herdr CLI used for one-shot commands.
	BinPath string
	// LogLevel is the raw LOG_LEVEL value, empty when unset.
	LogLevel string
	// Path is the process PATH, forwarded to plugin panes so that the
	// manifest's relative command resolves (see herdr.CLI.OpenPane).
	Path string
	// InsideHerdr is true when HERDR_ENV=1.
	InsideHerdr bool
}

// Lookup mirrors os.LookupEnv so tests can inject an environment.
type Lookup func(key string) (string, bool)

// ReadEnv reads the real process environment.
func ReadEnv() (PluginEnv, error) {
	return ReadEnvFrom(os.LookupEnv, os.UserHomeDir)
}

// ReadEnvFrom builds a PluginEnv from the given lookup. It fails when the
// config or state directory is missing because every daemon and action
// needs both; use SocketPath for commands that only talk to the socket.
func ReadEnvFrom(lookup Lookup, home func() (string, error)) (PluginEnv, error) {
	get := func(key string) string {
		v, _ := lookup(key)
		return v
	}
	env := PluginEnv{
		PluginID:    get("HERDR_PLUGIN_ID"),
		Root:        get("HERDR_PLUGIN_ROOT"),
		ConfigDir:   get("HERDR_PLUGIN_CONFIG_DIR"),
		StateDir:    get("HERDR_PLUGIN_STATE_DIR"),
		BinPath:     get("HERDR_BIN_PATH"),
		LogLevel:    get("LOG_LEVEL"),
		Path:        get("PATH"),
		InsideHerdr: get("HERDR_ENV") == "1",
	}
	if env.PluginID == "" {
		env.PluginID = DefaultPluginID
	}
	if env.BinPath == "" {
		env.BinPath = "herdr"
	}
	if env.ConfigDir == "" {
		return env, fmt.Errorf("HERDR_PLUGIN_CONFIG_DIR is not set: run this command from Herdr")
	}
	if env.StateDir == "" {
		return env, fmt.Errorf("HERDR_PLUGIN_STATE_DIR is not set: run this command from Herdr")
	}
	sock, err := SocketPath(lookup, home)
	if err != nil {
		return env, err
	}
	env.SocketPath = sock
	return env, nil
}

// SocketPath resolves the Herdr socket the way Herdr documents it:
// HERDR_SOCKET_PATH, else ~/.config/herdr/herdr.sock.
func SocketPath(lookup Lookup, home func() (string, error)) (string, error) {
	if p, ok := lookup("HERDR_SOCKET_PATH"); ok && p != "" {
		return p, nil
	}
	dir, err := home()
	if err != nil {
		return "", fmt.Errorf("HERDR_SOCKET_PATH unset and home dir unknown: %w", err)
	}
	return filepath.Join(dir, ".config", "herdr", "herdr.sock"), nil
}

// InsideHerdr reports whether the real environment has HERDR_ENV=1.
func InsideHerdr() bool {
	return os.Getenv("HERDR_ENV") == "1"
}

// DefaultSocketPath resolves the Herdr socket from the real environment.
// It is the socket-only counterpart of ReadEnv for commands that do not
// need the plugin directories.
func DefaultSocketPath() (string, error) {
	return SocketPath(os.LookupEnv, os.UserHomeDir)
}

// ReadAgentCommandEnv resolves the narrow environment needed by local agent
// commands. Herdr panes expose HERDR_PANE_ID but do not necessarily inherit
// plugin-specific config/state variables, so the state path falls back to
// Herdr's XDG state layout (and an explicit variable always wins).
func ReadAgentCommandEnv() (PluginEnv, error) {
	return ReadAgentCommandEnvFrom(os.LookupEnv, os.UserHomeDir)
}

func ReadAgentCommandEnvFrom(lookup Lookup, home func() (string, error)) (PluginEnv, error) {
	get := func(key string) string { value, _ := lookup(key); return value }
	env := PluginEnv{PluginID: get("HERDR_PLUGIN_ID"), StateDir: get("HERDR_PLUGIN_STATE_DIR"),
		ConfigDir: get("HERDR_PLUGIN_CONFIG_DIR"), BinPath: get("HERDR_BIN_PATH"),
		Root: get("HERDR_PLUGIN_ROOT"), Path: get("PATH"), InsideHerdr: get("HERDR_ENV") == "1"}
	if env.PluginID == "" {
		env.PluginID = DefaultPluginID
	}
	if env.PluginID != DefaultPluginID {
		return env, fmt.Errorf("agent upload commands are only available for plugin %q", DefaultPluginID)
	}
	if env.BinPath == "" {
		env.BinPath = "herdr"
	}
	if env.StateDir == "" {
		base := get("XDG_STATE_HOME")
		if base == "" {
			var err error
			base, err = home()
			if err != nil {
				return env, fmt.Errorf("resolve Herdr state directory: %w", err)
			}
			base = filepath.Join(base, ".local", "state")
		}
		env.StateDir = filepath.Join(base, "herdr", "plugins", env.PluginID)
	}
	stateDir, err := filepath.Abs(env.StateDir)
	if err != nil {
		return env, fmt.Errorf("resolve Herdr plugin state path: %w", err)
	}
	env.StateDir = stateDir
	socket, err := SocketPath(lookup, home)
	if err != nil {
		return env, err
	}
	env.SocketPath = socket
	return env, nil
}
