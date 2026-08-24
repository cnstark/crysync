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
	Log     LogConfig      `yaml:"log"`
	Modules []ModuleConfig `yaml:"modules"`
}

// LogConfig 持久化日志配置：file 为空 = 仅 stderr（现状，向后兼容）；
// max_files 显式 0 = 不轮转（用户自管 logrotate），缺省 5。
type LogConfig struct {
	File      string `yaml:"file"`
	Level     string `yaml:"level"`       // debug/info/warn/error，缺省 info
	MaxSizeMB *int   `yaml:"max_size_mb"` // nil = 缺省 16
	MaxFiles  *int   `yaml:"max_files"`   // nil = 缺省 5；0 = 不轮转
}

// SizeLimitMB 返回单文件软上限 MiB（nil → 缺省 16）。
func (l *LogConfig) SizeLimitMB() int {
	if l.MaxSizeMB == nil {
		return 16
	}
	return *l.MaxSizeMB
}

// RetainFiles 返回轮转保留数（nil → 缺省 5；0 → 不轮转）。
func (l *LogConfig) RetainFiles() int {
	if l.MaxFiles == nil {
		return 5
	}
	return *l.MaxFiles
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
	// 环境变量展开（yaml 解析前，全字段生效）：支持 ${VAR} 与 $VAR 两种引用。
	// 约定：引用写在 YAML 引号内（如 "${VAR}"），env 值含 #、: 等特殊字符时安全；
	// 未定义变量展开为空串（$ 为特殊字符，配置字面值含 $ 时注意会被尝试展开）。
	expanded := os.ExpandEnv(string(b))
	var c Config
	if err := yaml.Unmarshal([]byte(expanded), &c); err != nil {
		return nil, fmt.Errorf("解析配置: %w", err)
	}
	c.applyLogDefaults()
	if err := c.validateStructure(); err != nil {
		return nil, err
	}
	if err := c.validateLog(); err != nil {
		return nil, err
	}
	return &c, nil
}

// applyLogDefaults 填充 log 节缺省值（指针字段由 accessor 处理）。
func (c *Config) applyLogDefaults() {
	if c.Log.Level == "" {
		c.Log.Level = "info"
	}
}

// validateLog 校验 log 节取值（file 为空时其余字段仍校验）。
func (c *Config) validateLog() error {
	switch c.Log.Level {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("log.level 非法: %q（debug/info/warn/error）", c.Log.Level)
	}
	if c.Log.File != "" {
		if c.Log.SizeLimitMB() <= 0 {
			return fmt.Errorf("log.max_size_mb 必须为正数")
		}
		if c.Log.RetainFiles() < 0 {
			return fmt.Errorf("log.max_files 不能为负")
		}
	}
	return nil
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
