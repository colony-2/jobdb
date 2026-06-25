package story

import "fmt"

// Key uniquely identifies a chapter story inside a tenant anthology.
type Key struct {
	AnthologyID string
	StoryID     string
}

func (k Key) Validate() error {
	if k.AnthologyID == "" {
		return fmt.Errorf("story: anthology ID is required")
	}
	if k.StoryID == "" {
		return fmt.Errorf("story: story ID is required")
	}
	return nil
}
