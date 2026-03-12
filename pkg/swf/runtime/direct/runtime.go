package direct

import (
	"fmt"
	"time"

	strataclient "github.com/colony-2/strata-go/pkg/client"
	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/colony-2/swf-go/pkg/swf/impl"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// Runtime builds SWF engines backed by the current in-process strata + pgwf implementation.
type Runtime struct {
	db           *gorm.DB
	strataClient *strataclient.Client
}

func New(db *gorm.DB, strataClient *strataclient.Client) *Runtime {
	return &Runtime{
		db:           db,
		strataClient: strataClient,
	}
}

func NewFromConfig(postgresDSN, strataBaseURL, strataAPIKey string) (*Runtime, error) {
	if postgresDSN == "" {
		return nil, fmt.Errorf("postgres DSN is required")
	}
	if strataBaseURL == "" {
		return nil, fmt.Errorf("strata base URL is required")
	}
	if strataAPIKey == "" {
		return nil, fmt.Errorf("strata API key is required")
	}

	db, err := gorm.Open(postgres.Open(postgresDSN), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}
	strataClient, err := strataclient.New(strataclient.Config{
		BaseURL: strataBaseURL,
		APIKey:  strataAPIKey,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create strata client: %w", err)
	}
	return New(db, strataClient), nil
}

func (r *Runtime) BuildEngine(workers []swf.WorkSet, opts swf.RuntimeBuildOptions) (swf.SWFEngine, error) {
	if r == nil {
		return nil, fmt.Errorf("runtime is required")
	}
	if r.db == nil {
		return nil, fmt.Errorf("db is required")
	}
	if r.strataClient == nil {
		return nil, fmt.Errorf("strata client is required")
	}

	engine, err := impl.NewEngine(r.db, r.strataClient, workers, opts.Logger)
	if err != nil {
		return nil, err
	}
	type awaitConfigurator interface {
		SetAwaitThreshold(dur time.Duration)
	}
	if cfg, ok := engine.(awaitConfigurator); ok && opts.AwaitRecycleThreshold > 0 {
		cfg.SetAwaitThreshold(opts.AwaitRecycleThreshold)
	}
	return engine, nil
}
