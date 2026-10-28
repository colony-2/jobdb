package migration

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func (c *converter) manifestPath() string {
	path, _ := filepath.Abs(c.opts.Database)
	return filepath.Join(c.opts.BlobDir, fmt.Sprintf(".jobdb-chapter-migration-%x.json", sha256.Sum256([]byte(path))))
}

// The journal contains only migration-owned blob names. Recovery checks committed
// references before unlinking anything, including after an uncertain COMMIT.
func (c *converter) recoverBlobs() error {
	raw, err := os.ReadFile(c.manifestPath())
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var names []string
	if err = json.Unmarshal(raw, &names); err != nil {
		return err
	}
	for _, name := range names {
		if filepath.Base(name) != name || !strings.HasPrefix(name, "migration-") {
			return fmt.Errorf("invalid migration blob manifest")
		}
		var count int
		if err = c.conn.QueryRowContext(c.ctx, "SELECT count(*) FROM jobdb_chapter_artifacts WHERE blob_path=?", name).Scan(&count); err != nil {
			return err
		}
		if count == 0 {
			if err = os.Remove(filepath.Join(c.opts.BlobDir, name)); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	}
	return os.Remove(c.manifestPath())
}
func (c *converter) journalBlob(name string) error {
	names := make([]string, 0, len(c.created)+1)
	for _, path := range c.created {
		names = append(names, filepath.Base(path))
	}
	names = append(names, name)
	raw, err := json.Marshal(names)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(c.opts.BlobDir, ".migration-manifest-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if _, err = f.Write(raw); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = os.Rename(tmp, c.manifestPath()); err != nil {
		return err
	}
	dir, err := os.Open(c.opts.BlobDir)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
