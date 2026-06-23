package direct

import (
	directimpl "github.com/colony-2/jobdb/pkg/jobdb/runtime/direct/internal/directimpl"
	strataclient "github.com/colony-2/strata-go/pkg/client"
	"gorm.io/gorm"
)

type Runtime = directimpl.Runtime

func New(db *gorm.DB, strataClient *strataclient.Client) *Runtime {
	return directimpl.New(db, strataClient)
}

func NewFromConfig(postgresDSN, strataBaseURL, strataAPIKey string) (*Runtime, error) {
	return directimpl.NewFromConfig(postgresDSN, strataBaseURL, strataAPIKey)
}
