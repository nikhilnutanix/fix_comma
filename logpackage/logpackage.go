package logpackage

import (
	"bufio"
	"flag"
	"os"
	"strings"
	"time"

	"github.com/golang/glog"
	util "fix_comma/util"
)

// Init initialises logging level for the application
func Init() {
	// Set time for logging in Utc
	os.Setenv(util.TimeZone, util.Utc)
	time.Local = time.UTC
	// Load the initial configuration
	loadConfig()
}

// to be done when there is outside change in config file
func loadConfig() {
	fullPath := strings.Join([]string{util.CategoryConfigFilePath, util.LogConfigFile}, util.SlashSeparator)
	file, err := os.Open(fullPath)
	if err != nil {
		glog.Errorf("Failed to read %s file: %v", fullPath, err)
		loadDefaultConfig()
		return
	}
	defer func(file *os.File) {
		err = file.Close()
		if err != nil {
			glog.Errorf("error closing %s: %v", fullPath, err)
		}
	}(file)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		if err = flag.Set(key, value); err != nil {
			glog.Errorf("Failed to set flag %s: %v", key, err)
		} else {
			glog.Infof("%s: %v", key, value)
		}
	}
	flag.Parse()
	glog.Info("read config.properties file")
}

// default config
// loaded when application cannot read the config file
func loadDefaultConfig() {
	// Set flags for glog
	if err := flag.Set("log_dir", util.LogDir); err != nil {
		glog.Errorf("Failed to set flag log_dir: %v", err)
	}
	if err := flag.Set("logtostderr", "false"); err != nil {
		glog.Errorf("Failed to set flag logtostderr: %v", err)
	}
	if err := flag.Set("alsologtostderr", "true"); err != nil {
		glog.Errorf("Failed to set flag alsologtostderr: %v", err)
	}
	if err := flag.Set("v", "0"); err != nil {
		glog.Errorf("Failed to set flag v: %v", err)
	}
	flag.Parse()
	glog.Info("Default config loaded")
}
