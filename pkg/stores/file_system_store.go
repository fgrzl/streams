package stores

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/fgrzl/streams/pkg/config"
	"github.com/fgrzl/streams/pkg/enumerators"
	"github.com/fgrzl/streams/pkg/models"
	"github.com/fgrzl/streams/pkg/serializers"

	"google.golang.org/protobuf/proto"
)

const FILE_SYSTEM = "FILE_SYSTEM"

func init() {
	RegisterStore(FILE_SYSTEM, NewFileSystemStore)
}

// FileSystemStreamStore represents a store for file-based streams
type FileSystemStreamStore struct {
	path string
}

// NewFileSystemStore creates a new instance of FileSystemPartitionStore
func NewFileSystemStore() StreamStore {
	return &FileSystemStreamStore{path: config.GetFileSystemPath()}
}

func (fs *FileSystemStreamStore) CreateTier(ctx context.Context, args *models.CreateTierArgs) error {
	tierPath := fs.getTierDirectoryPath(args.Tenant, args.Space, args.Partition, args.Tier)
	return os.MkdirAll(tierPath, 0755)
}

func (fs *FileSystemStreamStore) GetSpaces(ctx context.Context, args *models.GetSpacesArgs) enumerators.Enumerator[string] {
	path := fs.getTenantDirectoryPath(args.Tenant)
	buf, err := os.ReadDir(path)
	if err != nil {
		return enumerators.Error[string](err)
	}

	// force sort order
	sort.Slice(buf, func(i, j int) bool {
		return buf[i].Name() < buf[j].Name()
	})

	return enumerators.Map(
		enumerators.Slice(buf),
		func(d os.DirEntry) (string, error) {
			return d.Name(), nil
		})
}

func (fs *FileSystemStreamStore) GetPartitions(ctx context.Context, args *models.GetPartitionsArgs) enumerators.Enumerator[string] {
	path := fs.getSpaceDirectoryPath(args.Tenant, args.Space)
	buf, err := os.ReadDir(path)
	if err != nil {
		return enumerators.Error[string](err)
	}

	// force sort order
	sort.Slice(buf, func(i, j int) bool {
		return buf[i].Name() < buf[j].Name()
	})

	return enumerators.Map(
		enumerators.Slice(buf),
		func(d os.DirEntry) (string, error) {
			return d.Name(), nil
		})
}

func (fs *FileSystemStreamStore) GetPages(ctx context.Context, args *models.GetPagesArgs) enumerators.Enumerator[int32] {
	// Get the directory path for the tier
	dir := fs.getTierDirectoryPath(args.Tenant, args.Space, args.Partition, args.Tier)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return enumerators.Error[int32](err)
	}
	var slice []int32
	for _, entry := range entries {

		//Skip directories
		if entry.IsDir() {
			continue
		}

		// Split the filename by the period (.)
		parts := strings.Split(entry.Name(), ".")
		if len(parts) != 2 {
			// Skip files that do not have exactly 2 parts (e.g., "00001.pg")
			continue
		}

		// Validate that the second part is "pg"
		if parts[1] != "pg" {
			// Skip files that do not have "pg" as the extension
			continue
		}

		// Parse the first part to an int32
		number, err := strconv.ParseInt(parts[0], 10, 32)
		if err != nil {
			// Skip files where the first part cannot be converted to int32
			continue
		}
		slice = append(slice, int32(number))
	}

	// force sort order
	sort.Slice(slice, func(i, j int) bool { return i < j })

	return enumerators.Slice(slice)
}

// DeletePartition deletes the entire stream directory
func (fs *FileSystemStreamStore) DeleteSpace(ctx context.Context, args *models.DeleteSpaceArgs) error {

	path := fs.getSpaceDirectoryPath(args.Tenant, args.Space)
	err := os.RemoveAll(path)
	if err != nil {
		return fmt.Errorf("failed to remove stream directory: %w", err)
	}
	return nil
}

// DeletePartition deletes the entire stream directory
func (fs *FileSystemStreamStore) DeletePartition(ctx context.Context, args *models.DeletePartitionArgs) error {

	path := fs.getPartitionDirectoryPath(args.Tenant, args.Space, args.Partition)
	err := os.RemoveAll(path)
	if err != nil {
		return fmt.Errorf("failed to remove stream directory: %w", err)
	}
	return nil
}

// WriteManifest writes the manifest for a stream
func (fs *FileSystemStreamStore) WriteManifest(ctx context.Context, args *models.WriteManifestArgs) (models.ConcurrencyTag, error) {

	tierPath := fs.getTierDirectoryPath(args.Tenant, args.Space, args.Partition, args.Tier)
	fileName := fs.getManifestFilePath(tierPath)

	var isNew bool
	fileInfo, err := os.Stat(fileName)
	if err != nil {
		if os.IsNotExist(err) {
			isNew = true
		} else {
			return nil, err
		}
	}

	if !isNew {
		if fileInfo.ModTime() != args.Tag {
			return nil, models.NewConcurrencyError(fileInfo.Name())
		}
	}

	tmpFile, err := os.CreateTemp(tierPath, ".manifest_*")
	if err != nil {
		return nil, err
	}

	data, err := proto.Marshal(args.Manifest)
	if err != nil {
		tmpFile.Close()
		return nil, err
	}

	_, err = tmpFile.Write(data)
	if err != nil {
		tmpFile.Close()
		return nil, err
	}
	tmpFile.Close()

	if err := os.Rename(tmpFile.Name(), fileName); err != nil {
		return nil, err
	}

	fileInfo, err = os.Stat(fileName)
	if err != nil {
		return nil, err
	}

	concurrencyTag := models.ConcurrencyTag(fileInfo.ModTime())

	return concurrencyTag, nil
}

// ReadManifest gets the manifest for a stream
func (fs *FileSystemStreamStore) ReadManifest(ctx context.Context, args *models.ReadManifestArgs) (*models.ManifestWrapper, error) {
	// Initialize the return value
	wrapper := &models.ManifestWrapper{}

	// Get the file path
	tierPath := fs.getTierDirectoryPath(args.Tenant, args.Space, args.Partition, args.Tier)
	fileName := fs.getManifestFilePath(tierPath)

	// Use os.Stat to check if the file exists and get file info in one go
	fileInfo, err := os.Stat(fileName)
	if err != nil {
		// Return early if the file does not exist
		if os.IsNotExist(err) {
			return wrapper, nil
		}
		// For other errors, return the error
		return wrapper, err
	}

	// Open the file
	file, err := os.Open(fileName)
	if err != nil {
		// Return if opening the file fails
		return wrapper, err
	}
	defer file.Close() // Ensure file is closed after reading

	// Read the decompressed data
	data, err := io.ReadAll(file)
	if err != nil {
		return wrapper, err
	}

	// Deserialize the file content into the manifest object
	var manifest models.Manifest
	if err := proto.Unmarshal(data, &manifest); err != nil {
		// Return if unmarshalling fails
		return wrapper, err
	}

	// Return the manifest wrapped with concurrency tag based on the file's last modification time
	wrapper.Manifest = &manifest
	wrapper.Tag = models.ConcurrencyTag(fileInfo.ModTime())
	return wrapper, nil
}

// WriteRecords writes records to a page file
func (fs *FileSystemStreamStore) WritePage(ctx context.Context, args *models.WritePageArgs, entries enumerators.Enumerator[*models.Entry]) (*models.Page, error) {
	// Determine the file paths
	tierPath := fs.getTierDirectoryPath(args.Tenant, args.Space, args.Partition, args.Tier)
	fileName := fs.getPageFilePath(tierPath, args.Number)

	// Create a temporary file
	tmpFile, err := os.CreateTemp(tierPath, "*.tmp")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}

	defer func() {
		if tmpFile != nil {
			tmpFile.Close()
			_ = os.Remove(tmpFile.Name()) // Ensure temp file is cleaned up in case of errors
		}
	}()

	// Serialize the channel to the temp file
	page, err := serializers.Write(entries, tmpFile)
	if err != nil {
		return nil, err
	}

	// Handle page empty
	if page.Count == 0 {
		return nil, nil
	}

	// Handle minimum page size
	if page.Size < args.MinPageSize {
		return nil, nil
	}

	// Close the temp file before renaming
	if err := tmpFile.Close(); err != nil {
		return nil, fmt.Errorf("failed to close temp file: %w", err)
	}

	// Check for concurrency issues
	if _, err := os.Stat(fileName); err == nil {
		return nil, models.NewConcurrencyError(fileName)
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	// Rename the temporary file to the final file name
	if err := os.Rename(tmpFile.Name(), fileName); err != nil {
		return nil, fmt.Errorf("failed to rename temp file: %w", err)
	}

	tmpFile = nil

	page.Number = args.Number

	return page, nil
}

func (fs *FileSystemStreamStore) ReadPage(ctx context.Context, args *models.ReadPageArgs) enumerators.Enumerator[*models.Entry] {

	tierPath := fs.getTierDirectoryPath(args.Tenant, args.Space, args.Partition, args.Tier)
	filePath := fs.getPageFilePath(tierPath, args.Number)

	file, err := os.Open(filePath)
	if err != nil {
		return enumerators.Error[*models.Entry](err)
	}
	return serializers.NewPageReader(file, args.Position)
}

// DeletePage deletes a page file
func (fs *FileSystemStreamStore) DeletePage(ctx context.Context, args *models.DeletePageArgs) error {
	tierPath := fs.getTierDirectoryPath(args.Tenant, args.Space, args.Partition, args.Tier)
	filePath := fs.getPageFilePath(tierPath, args.Number)
	err := os.Remove(filePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("failed to remove file: %w", err)
	}

	return nil
}

// Helper function to get the path for a page file
func (fs *FileSystemStreamStore) getPageFilePath(tierPath string, number int32) string {
	return filepath.Join(tierPath, fmt.Sprintf("%020d.pg", number))
}

// Helper function to get the path for the manifest file
func (fs *FileSystemStreamStore) getManifestFilePath(tierPath string) string {
	return filepath.Join(tierPath, ".manifest")
}

func (fs *FileSystemStreamStore) getTierDirectoryPath(tenant string, space string, partition string, tier int32) string {
	path := filepath.Join(fs.path, tenant, space, partition, fmt.Sprintf("%d", tier))
	return path
}

func (fs *FileSystemStreamStore) getTenantDirectoryPath(tenant string) string {
	path := filepath.Join(fs.path, tenant)
	return path
}

func (fs *FileSystemStreamStore) getSpaceDirectoryPath(tenant string, space string) string {
	path := filepath.Join(fs.path, tenant, space)
	return path
}

func (fs *FileSystemStreamStore) getPartitionDirectoryPath(tenant string, space string, partition string) string {
	path := filepath.Join(fs.path, tenant, space, partition)
	return path
}
