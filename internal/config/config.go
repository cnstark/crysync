package config

import (
	"fmt"
	"os"

	"crysync/internal/prune"

	"gopkg.in/yaml.v3"
)

const DefaultChunkSize = 4194304

type Config struct {
	Listen  string         `yaml:"listen"`
	Auth    AuthConfig     `yaml:"auth"`
	Modules []ModuleConfig `yaml:"modules"`
}

type AuthConfig struct {
	Users map[string]string `yaml:"users"`
}

type BackendConfig struct {
	Type     string `yaml:"type"`
	Path     string `yaml:"path"`
	URL      string `yaml:"url"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type PruneConfig struct {
	KeepLast    int    `yaml:"keep_last"`
	KeepDaily   int    `yaml:"keep_daily"`
	KeepWeekly  int    `yaml:"keep_weekly"`
	KeepMonthly int    `yaml:"keep_monthly"`
	Schedule    string `yaml:"schedule"` // 每日执行时刻 "HH:MM"，缺省 "03:00"
}

// Policy 返回保留策略（restic 语义，internal/prune）。
func (p *PruneConfig) Policy() prune.Policy {
	return prune.Policy{
		KeepLast:    p.KeepLast,
		KeepDaily:   p.KeepDaily,
		KeepWeekly:  p.KeepWeekly,
		KeepMonthly: p.KeepMonthly,
	}
}

type ModuleConfig struct {
	Name      string        `yaml:"name"`
	Path      string        `yaml:"path"`
	ReadOnly  bool          `yaml:"read_only"`
	Backend   BackendConfig `yaml:"backend"`
	Keyfile   string        `yaml:"keyfile"`
	Meta      string        `yaml:"meta"`
	ChunkSize int           `yaml:"chunk_size"`
	Prune     *PruneConfig  `yaml:"prune"`
}

func (m *ModuleConfig) ChunkSizeBytes() int {
	if m.ChunkSize <= 0 {
		return DefaultChunkSize
	}
	return m.ChunkSize
}

func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取配置: %w", err)
	}
	var c Config
	if err := yaml.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("解析配置: %w", err)
	}
	if err := c.validateStructure(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *Config) Validate() error {
	if c.Listen == "" {
		return fmt.Errorf("listen 不能为空")
	}
	if err := c.validateStructure(); err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, m := range c.Modules {
		if seen[m.Name] {
			return fmt.Errorf("模块名重复: %s", m.Name)
		}
		seen[m.Name] = true
	}
	return nil
}

// validateStructure 校验配置的基本结构完整性（模块存在性及各模块必需字段）。
// Load 使用它保证返回结构合法；重复模块名、listen 等完整性校验由 Validate 完成。
func (c *Config) validateStructure() error {
	if len(c.Modules) == 0 {
		return fmt.Errorf("至少需要一个模块")
	}
	for _, m := range c.Modules {
		if m.Name == "" {
			return fmt.Errorf("模块名不能为空")
		}
		if m.Keyfile == "" || m.Meta == "" {
			return fmt.Errorf("模块 %s 缺少 keyfile 或 meta", m.Name)
		}
		if m.Backend.Type == "" {
			return fmt.Errorf("模块 %s 缺少 backend.type", m.Name)
		}
	}
	return nil
}
