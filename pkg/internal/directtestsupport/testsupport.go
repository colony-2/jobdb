package directtestsupport

import (
	"fmt"
	"net"
	"os"
	"path/filepath"

	"github.com/fergusstrange/embedded-postgres"
)

func StartEmbeddedPostgres() (string, func(), error) {
	pgPort, err := freeTCPPort()
	if err != nil {
		return "", nil, err
	}
	tmpDir, err := os.MkdirTemp("", "jobdb-embedded-*")
	if err != nil {
		return "", nil, fmt.Errorf("temp dir: %w", err)
	}
	runtimePath := filepath.Join(tmpDir, "runtime")
	dataPath := filepath.Join(tmpDir, "data")
	_ = os.MkdirAll(runtimePath, 0o755)
	_ = os.MkdirAll(dataPath, 0o755)

	postgres := embeddedpostgres.NewDatabase(
		embeddedpostgres.DefaultConfig().
			Port(pgPort).
			RuntimePath(runtimePath).
			DataPath(dataPath),
	)
	if err := postgres.Start(); err != nil {
		return "", nil, err
	}
	stop := func() {
		_ = postgres.Stop()
		_ = os.RemoveAll(tmpDir)
	}
	dsn := fmt.Sprintf("postgres://postgres:postgres@localhost:%d/postgres?sslmode=disable", pgPort)
	return dsn, stop, nil
}

func freeTCPPort() (uint32, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("reserve postgres port: %w", err)
	}
	defer listener.Close()

	addr, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		return 0, fmt.Errorf("reserve postgres port: unexpected addr type %T", listener.Addr())
	}
	return uint32(addr.Port), nil
}
