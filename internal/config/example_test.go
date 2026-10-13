package config

import (
	"path/filepath"
	"reflect"
	"testing"
)

// The shipped config.example.yaml (also the Docker image default) uses the
// chains.<name> shape: it must load without deprecation notices and match
// the legacy-shape example it replaced.
func TestExampleConfigCurrentShape(t *testing.T) {
	newCfg, err := Load(filepath.Join("..", "..", "config.example.yaml"), env(nil))
	if err != nil {
		t.Fatal(err)
	}
	if len(newCfg.Deprecated) != 0 {
		t.Fatalf("example config uses deprecated keys: %v", newCfg.Deprecated)
	}
	oldCfg, err := Load(filepath.Join("testdata", "legacy-example.yaml"), env(nil))
	if err != nil {
		t.Fatal(err)
	}
	oldCfg.Deprecated = nil
	if !reflect.DeepEqual(newCfg, oldCfg) {
		t.Fatalf("new example differs from legacy:\nnew %+v\nold %+v", newCfg, oldCfg)
	}
}

func TestExampleConfigChainsEnvOverride(t *testing.T) {
	c, err := Load(filepath.Join("..", "..", "config.example.yaml"), env(map[string]string{"CHAINS": "tron,ton,eth"}))
	if err != nil {
		t.Fatal(err)
	}
	if got := c.Chains; !reflect.DeepEqual(got, []string{"tron", "ton", "eth"}) {
		t.Fatalf("CHAINS env: %v", got)
	}
}
