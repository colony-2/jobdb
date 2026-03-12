package impl

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	"github.com/colony-2/pgwf-go/pkg/pgwf"
	strataclient "github.com/colony-2/strata-go/pkg/client"
	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/colony-2/swf-go/pkg/swf/runtime/direct/testsupport"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type EmbeddedEngine struct {
	swf.SWFEngine
	stopPG         func()
	strataShutdown func()
}

func StartEmbeddedPostgres() (string, func(), error) {
	return testsupport.StartEmbeddedPostgres()
}

func InstallPGWF(ctx context.Context, db *sql.DB) error {
	return testsupport.InstallPGWF(ctx, db)
}

func StartEmbeddedEngine(ctx context.Context, job swf.JobWorker, tasks ...swf.TaskWorker) (*EmbeddedEngine, error) {
	dsn, stopPG, err := testsupport.StartEmbeddedPostgres()
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		stopPG()
		return nil, err
	}
	cleanup := func() {
		_ = db.Close()
		stopPG()
	}

	setupCtx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	if err := testsupport.InstallPGWF(setupCtx, db); err != nil {
		cleanup()
		return nil, err
	}
	s, err := testsupport.StartEmbeddedStrata()
	if err != nil {
		cleanup()
		return nil, err
	}

	gdb, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		s.Shutdown()
		cleanup()
		return nil, err
	}
	strataClient, err := strataclient.New(strataclient.Config{
		BaseURL: s.BaseURL,
		APIKey:  s.APIKey,
	})
	if err != nil {
		s.Shutdown()
		cleanup()
		return nil, err
	}

	var worksets []swf.WorkSet
	if job != nil {
		ws, err := swf.AsWorkSet(job, tasks...)
		if err != nil {
			s.Shutdown()
			cleanup()
			return nil, err
		}
		worksets = append(worksets, *ws)
	}
	engine, err := NewEngine(gdb, strataClient, worksets, slog.Default())
	if err != nil {
		s.Shutdown()
		cleanup()
		return nil, err
	}
	type awaitConfigurator interface {
		SetAwaitThreshold(time.Duration)
	}
	if cfg, ok := engine.(awaitConfigurator); ok {
		cfg.SetAwaitThreshold(5 * time.Second)
	}
	return &EmbeddedEngine{
		SWFEngine:      engine,
		stopPG:         cleanup,
		strataShutdown: s.Shutdown,
	}, nil
}

func newRunnerForTest(engine *swfEngineImpl, lease *pgwf.Lease, ws *swf.WorkSet, ctx context.Context) *runner {
	if engine == nil {
		return &runner{}
	}
	var cap pgwf.Capability
	if lease != nil {
		cap = lease.NextNeed()
	}
	leaseAdapter := newPgwfLeaseAdapter(lease, engine.udb)
	backend := &defaultRunnerBackend{
		engine:     engine,
		lease:      leaseAdapter,
		pgwfLease:  lease,
		capability: cap,
	}
	r := &runner{
		engine:       engine,
		worker:       ws,
		storyCounter: 1,
		backend:      backend,
		lease:        leaseAdapter,
		logger:       engine.logger,
		jobPolicy:    normalizeRunPolicy(swf.RunPolicy{}),
		capability:   cap,
		ctx:          ctx,
		workerId:     engine.workerId,
		observer:     noopReplayObserver{},
	}
	if lease != nil {
		r.jobId = lease.JobID()
		r.tenantId = string(lease.TenantID())
	}
	return r
}

func (e *EmbeddedEngine) Shutdown() {
	e.stopPG()
	e.strataShutdown()
}
