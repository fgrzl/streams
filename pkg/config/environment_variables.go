package config

import (
	"os"
	"strconv"
)

// Env variable keys
const (
	// Time server options
	WOOLF_TIME_SERVER = "WOOLF_TIME_SERVER"

	// Service options
	WOOLF_ENABLE_BACKGROUND_MERGE   = "WOOLF_ENABLE_BACKGROUND_MERGE"
	WOOLF_ENABLE_BACKGROUND_PRUNE   = "WOOLF_ENABLE_BACKGROUND_PRUNE"
	WOOLF_ENABLE_BACKGROUND_REBUILD = "WOOLF_ENABLE_BACKGROUND_REBUILD"

	// File system store variables
	WOOLF_FS_PATH = "WOOLF_FS_PATH"

	// S3 store variables
	WOOLF_S3_BUCKET          = "WOOLF_S3_BUCKET"
	WOOLF_S3_CREDENTIAL_TYPE = "WOOLF_S3_CREDENTIAL_TYPE"
	WOOLF_S3_CLIENT_ID       = "WOOLF_S3_CLIENT_ID"
	WOOLF_S3_CLIENT_SECRET   = "WOOLF_S3_CLIENT_SECRET"

	// Azure blob storage store variables
	WOOLF_AZURE_ENDPOINT        = "WOOLF_AZURE_ENDPOINT"
	WOOLF_AZURE_CONTAINER       = "WOOLF_AZURE_CONTAINER"
	WOOLF_AZURE_CREDENTIAL_TYPE = "WOOLF_AZURE_CREDENTIAL_TYPE"
	WOOLF_AZURE_ACCOUNT_NAME    = "WOOLF_AZURE_ACCOUNT_NAME"
	WOOLF_AZURE_ACCOUNT_KEY     = "WOOLF_AZURE_ACCOUNT_KEY"

	// Google Cloud Storage store variables
	WOOLF_GOOGLE_CONTAINER       = "WOOLF_GOOGLE_CONTAINER"
	WOOLF_GOOGLE_CREDENTIAL_TYPE = "WOOLF_GOOGLE_CREDENTIAL_TYPE"
	WOOLF_GOOGLE_CLIENT_ID       = "WOOLF_GOOGLE_CLIENT_ID"
	WOOLF_GOOGLE_CLIENT_SECRET   = "WOOLF_GOOGLE_CLIENT_SECRET"
)

// Time Server
func GetTimeServer() string {
	return os.Getenv(WOOLF_TIME_SERVER)
}

func SetTimeServer(name string) {
	os.Setenv(WOOLF_TIME_SERVER, name)
}

// File System Store
func GetFileSystemPath() string {
	return os.Getenv(WOOLF_FS_PATH)
}

func SetFileSystemPath(path string) {
	os.Setenv(WOOLF_FS_PATH, path)
}

// Background Service Options
func GetEnableBackgroundMerge() bool {
	val, _ := strconv.ParseBool(os.Getenv(WOOLF_ENABLE_BACKGROUND_MERGE))
	return val
}

func SetEnableBackgroundMerge(enabled bool) {
	os.Setenv(WOOLF_ENABLE_BACKGROUND_MERGE, strconv.FormatBool(enabled))
}

func GetEnableBackgroundPrune() bool {
	val, _ := strconv.ParseBool(os.Getenv(WOOLF_ENABLE_BACKGROUND_PRUNE))
	return val
}

func SetEnableBackgroundPrune(enabled bool) {
	os.Setenv(WOOLF_ENABLE_BACKGROUND_PRUNE, strconv.FormatBool(enabled))
}

func GetEnableBackgroundRebuild() bool {
	val, _ := strconv.ParseBool(os.Getenv(WOOLF_ENABLE_BACKGROUND_REBUILD))
	return val
}

func SetEnableBackgroundRebuild(enabled bool) {
	os.Setenv(WOOLF_ENABLE_BACKGROUND_REBUILD, strconv.FormatBool(enabled))
}
