package stores

// import (
// 	"context"
// 	"fmt"
// 	"time"

// 	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
// 	"github.com/fgrzl/enumerators"
// 	"github.com/fgrzl/streams/pkg/config"
// 	"github.com/fgrzl/streams/pkg/models"
// )

const AZURE = "AZURE"

// func init() {
// 	RegisterStore(AZURE, NewAzureStore)
// }

// func NewAzureStore() StreamStore {
// 	return &AzureStreamStore{}
// }

// type AzureStreamStore struct {
// 	serviceClient *azblob.ServiceClient
// 	containerName string
// }

// func NewAzureStoreWithConfig() (*AzureStreamStore, error) {

// 	// todo : use env variables to get auth method, account name, container name etc

// 	// TODO: replace <storage-account-name> with your actual storage account name
// 	url := config.GetAzureEndpoint()
// 	credType := config.GetAzureCredentialType()

// 	var credential azidentity.TokenCredential
// 	var err error
// 	switch credType {
// 	case "account_key":

// 	default:
// 		credential, err := azidentity.NewDefaultAzureCredential(nil)
// 	}

// 	client, err := azblob.NewServiceClient(url, credential, nil)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to create service client: %w", err)
// 	}

// 	return &AzureStreamStore{
// 		serviceClient: client,
// 		containerName: containerName,
// 	}, nil
// }

// func (s *AzureStreamStore) getContainerClient() (*azblob.ContainerClient, error) {
// 	containerClient := s.serviceClient.NewContainerClient(s.containerName)
// 	return containerClient, nil
// }

// func (s *AzureStreamStore) CreateTier(ctx context.Context, args *models.CreateTierArgs) error {
// 	containerClient, err := s.getContainerClient()
// 	if err != nil {
// 		return err
// 	}

// 	_, err = containerClient.Create(ctx, nil)
// 	if err != nil {
// 		return fmt.Errorf("failed to create container: %w", err)
// 	}

// 	return nil
// }

// func (s *AzureStreamStore) DeletePage(ctx context.Context, args *models.DeletePageArgs) error {
// 	containerClient, err := s.getContainerClient()
// 	if err != nil {
// 		return err
// 	}

// 	blobClient := containerClient.NewBlobClient(args.PageID)
// 	_, err = blobClient.Delete(ctx, nil)
// 	if err != nil {
// 		return fmt.Errorf("failed to delete page: %w", err)
// 	}

// 	return nil
// }

// func (s *AzureStreamStore) DeletePartition(ctx context.Context, args *models.DeletePartitionArgs) error {
// 	containerClient, err := s.getContainerClient()
// 	if err != nil {
// 		return err
// 	}

// 	_, err = containerClient.Delete(ctx, nil)
// 	if err != nil {
// 		return fmt.Errorf("failed to delete partition: %w", err)
// 	}

// 	return nil
// }

// func (s *AzureStreamStore) DeleteSpace(ctx context.Context, args *models.DeleteSpaceArgs) error {
// 	containerClient, err := s.getContainerClient()
// 	if err != nil {
// 		return err
// 	}

// 	_, err = containerClient.Delete(ctx, nil)
// 	if err != nil {
// 		return fmt.Errorf("failed to delete space: %w", err)
// 	}

// 	return nil
// }

// func (s *AzureStreamStore) GetPages(ctx context.Context, args *models.GetPagesArgs) enumerators.Enumerator[int32] {
// 	// Implement listing blobs for pages
// 	panic("unimplemented")
// }

// func (s *AzureStreamStore) GetPartitions(ctx context.Context, args *models.GetPartitionsArgs) enumerators.Enumerator[string] {
// 	// Implement listing blobs for partitions
// 	panic("unimplemented")
// }

// func (s *AzureStreamStore) GetSpaces(ctx context.Context, args *models.GetSpacesArgs) enumerators.Enumerator[string] {
// 	// Implement listing containers for spaces
// 	panic("unimplemented")
// }

// func (s *AzureStreamStore) ReadManifest(ctx context.Context, args *models.ReadManifestArgs) (*models.ManifestWrapper, error) {
// 	containerClient, err := s.getContainerClient()
// 	if err != nil {
// 		return nil, err
// 	}

// 	blobClient := containerClient.NewBlobClient(args.ManifestID)
// 	downloadResponse, err := blobClient.Download(ctx, nil)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to download manifest: %w", err)
// 	}

// 	manifest := &models.ManifestWrapper{}
// 	// Process downloaded content
// 	_ = downloadResponse // Placeholder: Deserialize response

// 	return manifest, nil
// }

// func (s *AzureStreamStore) ReadPage(ctx context.Context, args *models.ReadPageArgs) enumerators.Enumerator[*models.Entry] {
// 	// Implement reading blob contents
// 	panic("unimplemented")
// }

// func (s *AzureStreamStore) WriteManifest(ctx context.Context, args *models.WriteManifestArgs) (models.ConcurrencyTag, error) {
// 	containerClient, err := s.getContainerClient()
// 	if err != nil {
// 		return "", err
// 	}

// 	blobClient := containerClient.NewBlockBlobClient(args.ManifestID)
// 	_, err = blobClient.Upload(ctx, nil, nil)
// 	if err != nil {
// 		return "", fmt.Errorf("failed to write manifest: %w", err)
// 	}

// 	return models.ConcurrencyTag(time.Now().String()), nil
// }

// func (s *AzureStreamStore) WritePage(ctx context.Context, args *models.WritePageArgs, entries enumerators.Enumerator[*models.Entry]) (*models.Page, error) {
// 	containerClient, err := s.getContainerClient()
// 	if err != nil {
// 		return nil, err
// 	}

// 	blobClient := containerClient.NewBlockBlobClient(args.PageID)
// 	_, err = blobClient.Upload(ctx, nil, nil)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to write page: %w", err)
// 	}

// 	return &models.Page{}, nil
// }

// func (s *AzureStreamStore) Scavenge(ctx context.Context) error {
// 	// Implement cleanup logic
// 	panic("unimplemented")
// }
