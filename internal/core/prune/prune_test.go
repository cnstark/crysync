// internal/core/prune/prune_test.go
package prune

import (
	"testing"
	"time"
)

// 构造 n 个快照：最新时间 now，每个间隔 step。
func mkSnaps(n int, step time.Duration) []Snapshot {
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.Local)
	var out []Snapshot
	for i := 0; i < n; i++ {
		out = append(out, Snapshot{ID: int64(i + 1), CreatedAt: now.Add(-step * time.Duration(i))})
	}
	return out
}

func ids(snaps []Snapshot) []int64 {
	out := make([]int64, 0, len(snaps))
	for _, s := range snaps {
		out = append(out, s.ID)
	}
	return out
}

func has(id int64, set map[int64]bool) bool { return set[id] }

// keep_last：最近 N 个保留，其余删除。
func TestKeepLast(t *testing.T) {
	snaps := mkSnaps(10, time.Hour)
	keep, remove := Policy{KeepLast: 2}.Apply(snaps)
	for _, id := range []int64{1, 2} { // 最新两个（id 1 = 最新）
		if !has(id, keep) {
			t.Fatalf("应保留 %d", id)
		}
	}
	if len(remove) != 8 {
		t.Fatalf("应删除 8 个: %v", ids(remove))
	}
}

// keep_last 大于快照总数：全保留。
func TestKeepLastMoreThanCount(t *testing.T) {
	snaps := mkSnaps(3, time.Hour)
	keep, remove := Policy{KeepLast: 5}.Apply(snaps)
	if len(keep) != 3 || len(remove) != 0 {
		t.Fatalf("应全保留: %d keep, %d remove", len(keep), len(remove))
	}
}

// keep_daily：每天保留当天最新，只保留最近 N 天。
func TestKeepDaily(t *testing.T) {
	day := 24 * time.Hour
	// 3 天各 2 个快照：id 1-2 第 1 天（最新），3-4 第 2 天，5-6 第 3 天（最旧）
	base := time.Date(2026, 8, 23, 12, 0, 0, 0, time.Local)
	snaps := []Snapshot{
		{ID: 1, CreatedAt: base},
		{ID: 2, CreatedAt: base.Add(-3 * time.Hour)},
		{ID: 3, CreatedAt: base.Add(-day)},
		{ID: 4, CreatedAt: base.Add(-day - 3*time.Hour)},
		{ID: 5, CreatedAt: base.Add(-2 * day)},
		{ID: 6, CreatedAt: base.Add(-2*day - 3*time.Hour)},
	}
	keep, remove := Policy{KeepDaily: 2}.Apply(snaps)
	// 最近 2 天（第 1、2 天）各保留最新：id 1、3；第 3 天的 id 5、6 删除
	for _, id := range []int64{1, 3} {
		if !has(id, keep) {
			t.Fatalf("应保留 %d", id)
		}
	}
	for _, id := range []int64{2, 4, 5, 6} {
		if has(id, keep) {
			t.Fatalf("不应保留 %d", id)
		}
	}
	if len(remove) != 4 {
		t.Fatalf("应删除 4 个: %v", ids(remove))
	}
}

// keep_weekly：ISO 周分桶，只保留最近 N 周。
func TestKeepWeekly(t *testing.T) {
	// 2026-08-23 是周日；构造跨 3 周的快照
	base := time.Date(2026, 8, 23, 12, 0, 0, 0, time.Local) // 第 34 周
	wk := 7 * 24 * time.Hour
	snaps := []Snapshot{
		{ID: 1, CreatedAt: base},
		{ID: 2, CreatedAt: base.Add(-2 * time.Hour)},      // 同周
		{ID: 3, CreatedAt: base.Add(-wk)},                 // 第 33 周
		{ID: 4, CreatedAt: base.Add(-2 * wk)},             // 第 32 周
		{ID: 5, CreatedAt: base.Add(-2*wk - 2*time.Hour)}, // 第 32 周
	}
	keep, _ := Policy{KeepWeekly: 2}.Apply(snaps)
	for _, id := range []int64{1, 3} {
		if !has(id, keep) {
			t.Fatalf("应保留 %d", id)
		}
	}
	for _, id := range []int64{2, 4, 5} {
		if has(id, keep) {
			t.Fatalf("不应保留 %d", id)
		}
	}
}

// keep_monthly：月分桶，只保留最近 N 月。
func TestKeepMonthly(t *testing.T) {
	base := time.Date(2026, 8, 23, 12, 0, 0, 0, time.Local)
	month := func(m time.Month, d int) time.Time {
		return time.Date(2026, m, d, 12, 0, 0, 0, time.Local)
	}
	snaps := []Snapshot{
		{ID: 1, CreatedAt: base},
		{ID: 2, CreatedAt: month(8, 1)},
		{ID: 3, CreatedAt: month(7, 20)},
		{ID: 4, CreatedAt: month(7, 5)},
		{ID: 5, CreatedAt: month(6, 15)},
		{ID: 6, CreatedAt: month(5, 10)},
	}
	keep, _ := Policy{KeepMonthly: 2}.Apply(snaps)
	// 最近 2 月（8 月、7 月）各保留最新：1、3
	for _, id := range []int64{1, 3} {
		if !has(id, keep) {
			t.Fatalf("应保留 %d", id)
		}
	}
	for _, id := range []int64{2, 4, 5, 6} {
		if has(id, keep) {
			t.Fatalf("不应保留 %d", id)
		}
	}
}

// 组合规则并集：keep_last 与 keep_daily 互不覆盖。
func TestCombined(t *testing.T) {
	day := 24 * time.Hour
	base := time.Date(2026, 8, 23, 12, 0, 0, 0, time.Local)
	snaps := []Snapshot{
		{ID: 1, CreatedAt: base}, // 最近
		{ID: 2, CreatedAt: base.Add(-6 * time.Hour)},
		{ID: 3, CreatedAt: base.Add(-day)},     // 昨天
		{ID: 4, CreatedAt: base.Add(-2 * day)}, // 前天
		{ID: 5, CreatedAt: base.Add(-3 * day)},
	}
	keep, _ := Policy{KeepLast: 2, KeepDaily: 3}.Apply(snaps)
	// last 保留 1、2；daily 保留 1、3、4 → 并集 1、2、3、4；删除 5
	for _, id := range []int64{1, 2, 3, 4} {
		if !has(id, keep) {
			t.Fatalf("应保留 %d", id)
		}
	}
	if has(5, keep) {
		t.Fatal("不应保留 5")
	}
}

// 未配置策略（全 0）：不删除任何快照。
func TestNoPolicy(t *testing.T) {
	snaps := mkSnaps(5, time.Hour)
	keep, remove := Policy{}.Apply(snaps)
	if len(keep) != 5 || len(remove) != 0 {
		t.Fatalf("无策略应全保留: %d keep, %d remove", len(keep), len(remove))
	}
}

// 空快照列表。
func TestEmpty(t *testing.T) {
	keep, remove := Policy{KeepLast: 3}.Apply(nil)
	if len(keep) != 0 || len(remove) != 0 {
		t.Fatalf("空列表应无操作: %d %d", len(keep), len(remove))
	}
}
