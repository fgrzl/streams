package repositories

import (
	"context"
	"fmt"
	"log"

	"github.com/fgrzl/woolf/pkg/models"
	"github.com/fgrzl/woolf/pkg/stores"
	"github.com/fgrzl/woolf/pkg/util"

	"google.golang.org/protobuf/proto"
)

// The manifest repository allows us to syncronize all reads/writes to the manifest with no locks
type ManifestRepository interface {
	util.Disposable
	Create() error
	GetManifest() (*models.Manifest, error)
	AddPage(page *models.Page)
	RemovePage(page *models.Page)
}

type manifestRepository struct {
	util.Disposable
	key          ManifestKey
	manifest     *models.Manifest
	tag          models.ConcurrencyTag
	store        stores.StreamStore
	backgroundCh chan any
	dispose      context.CancelFunc
}

type createManifest struct {
	ResponseCh chan error
}

type getManifest struct {
	ResponseCh chan *models.Manifest
}

type addPage struct {
	Tag  models.ConcurrencyTag
	Page *models.Page
}

type removePage struct {
	Tag  models.ConcurrencyTag
	Page *models.Page
}

type ManifestKey struct {
	Tenant    string
	Space     string
	Partition string
	Tier      int32
}

func NewManifestRepository(key ManifestKey, store stores.StreamStore) ManifestRepository {
	ctx, cancel := context.WithCancel(context.Background())
	m := &manifestRepository{
		key:          key,
		store:        store,
		backgroundCh: make(chan any, 10),
		dispose:      cancel,
	}
	go m.backgroundConsumer(ctx)
	return m
}

func (m *manifestRepository) Dispose() {
	m.dispose()
}
func (m *manifestRepository) Create() error {
	responseCh := make(chan error, 1)
	m.backgroundCh <- &createManifest{ResponseCh: responseCh}

	// Wait for the response
	return <-responseCh
}

// GetManifest sends a request to read the manifest
func (m *manifestRepository) GetManifest() (*models.Manifest, error) {
	ch := make(chan *models.Manifest, 1)
	m.backgroundCh <- &getManifest{ResponseCh: ch}

	// Wait for the response
	return <-ch, nil
}

func (m *manifestRepository) AddPage(page *models.Page) {
	m.backgroundCh <- &addPage{Page: page}
}

func (m *manifestRepository) RemovePage(page *models.Page) {
	m.backgroundCh <- &removePage{Page: page}
}

// brokers access to the manifest
func (m *manifestRepository) backgroundConsumer(ctx context.Context) {
	defer func() {
		fmt.Println("manifest repository background consumer stopped.")
	}()

	for {
		select {
		case <-ctx.Done():
			// Context canceled, exit the goroutine
			return

		case msg, ok := <-m.backgroundCh:
			if !ok {
				// Channel closed, exit the goroutine
				return
			}

			switch obj := msg.(type) {
			case *createManifest:
				defer close(obj.ResponseCh)
				args := models.ReadManifestArgs{
					Tenant:    m.key.Tenant,
					Space:     m.key.Space,
					Partition: m.key.Partition,
					Tier:      m.key.Tier}

				wrapper, err := m.store.ReadManifest(ctx, args)
				if err != nil {
					obj.ResponseCh <- err
					continue
				}

				if wrapper != nil && wrapper.Manifest == nil {
					_, err := m.store.WriteManifest(ctx, models.WriteManifestArgs{
						Tenant:    m.key.Tenant,
						Space:     m.key.Space,
						Partition: m.key.Partition,
						Tier:      m.key.Tier,
						Manifest:  &models.Manifest{LastPage: models.EmptyPage},
					})
					if err != nil {
						obj.ResponseCh <- err
						continue
					}
				}
				obj.ResponseCh <- nil

			case *getManifest:
				defer close(obj.ResponseCh)
				if m.manifest == nil || m.tag == nil {
					args := models.ReadManifestArgs{
						Tenant:    m.key.Tenant,
						Space:     m.key.Space,
						Partition: m.key.Partition,
						Tier:      m.key.Tier}

					wrapper, err := m.store.ReadManifest(ctx, args)
					if err != nil {
						log.Printf("could not load manifest for stream: %v, tier: %v", args.Partition, args.Tier)
					}
					m.manifest = wrapper.Manifest
					m.tag = wrapper.Tag
				}
				copy := proto.Clone(m.manifest).(*models.Manifest)
				obj.ResponseCh <- copy

			case *addPage:
				m.manifest.Pages = append(m.manifest.Pages, obj.Page)
				m.manifest.LastPage = obj.Page

				args := models.WriteManifestArgs{
					Tenant:    m.key.Tenant,
					Space:     m.key.Space,
					Partition: m.key.Partition,
					Tier:      m.key.Tier,
					Tag:       m.tag,
				}

				tag, err := m.store.WriteManifest(ctx, args)
				if err != nil {
					// todo : we need to fix the manifest

				}
				m.tag = tag

			case *removePage:
				filtered := m.manifest.Pages[:0]
				for _, page := range m.manifest.Pages {
					if page.Number != obj.Page.Number {
						filtered = append(filtered, page)
					}
				}
				m.manifest.Pages = filtered

				args := models.WriteManifestArgs{
					Tenant:    m.key.Tenant,
					Space:     m.key.Space,
					Partition: m.key.Partition,
					Tier:      m.key.Tier}

				tag, err := m.store.WriteManifest(ctx, args)
				if err != nil {
					// todo : we need to fix the manifest
				}
				m.tag = tag
			}
		}
	}
}
