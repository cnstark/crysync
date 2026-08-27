// internal/core/prune/prune.go
package prune

import (
	"sort"
	"time"
)

// Policy 保留策略（restic forget 语义，设计文档 §9）：
// keep_last 保留最近 N 个；keep_daily/keep_weekly/keep_monthly 按时间桶
// （日/ISO 周/月）分组，每组保留最新一个，只保留最近 N 个桶。各规则并集。
type Policy struct {
	KeepLast    int
	KeepDaily   int
	KeepWeekly  int
	KeepMonthly int
}

// Snapshot 参与策略计算的快照（时间用于分桶）。
type Snapshot struct {
	ID        int64
	CreatedAt time.Time
}

// Apply 计算保留集合：返回 (保留的 ID 集合, 应删除的快照)。
// 未配置任何规则（全 0）时不删除任何快照。
func (p Policy) Apply(snaps []Snapshot) (map[int64]bool, []Snapshot) {
	if len(snaps) == 0 {
		return map[int64]bool{}, nil
	}
	// 未配置任何规则：全部保留（不删除），与"未配置 prune 不自动清理"一致
	if p.KeepLast == 0 && p.KeepDaily == 0 && p.KeepWeekly == 0 && p.KeepMonthly == 0 {
		all := make(map[int64]bool, len(snaps))
		for _, s := range snaps {
			all[s.ID] = true
		}
		return all, nil
	}
	// 时间降序（最新在前）——restic 按此序取 "最新"（applyRetentionPolicy）
	sorted := make([]Snapshot, len(snaps))
	copy(sorted, snaps)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].CreatedAt.After(sorted[j].CreatedAt) })

	keep := make(map[int64]bool, len(sorted))
	// keep_last：降序取前 N 个
	if p.KeepLast > 0 {
		n := min(p.KeepLast, len(sorted))
		for _, s := range sorted[:n] {
			keep[s.ID] = true
		}
	}
	// 时间桶规则：每组保留最新（降序中第一个遇到的），只保留最近 N 个桶
	keepBuckets := func(bucket func(Snapshot) int, count int) {
		if count <= 0 {
			return
		}
		newest := make(map[int]Snapshot) // 桶 -> 该桶最新快照
		for _, s := range sorted {
			b := bucket(s)
			if _, ok := newest[b]; !ok {
				newest[b] = s
			}
		}
		buckets := make([]int, 0, len(newest))
		for b := range newest {
			buckets = append(buckets, b)
		}
		sort.Ints(buckets)
		start := 0
		if len(buckets) > count {
			start = len(buckets) - count
		}
		for _, b := range buckets[start:] {
			keep[newest[b].ID] = true
		}
	}
	keepBuckets(func(s Snapshot) int { return s.CreatedAt.Year()*1000 + s.CreatedAt.YearDay() }, p.KeepDaily)
	keepBuckets(func(s Snapshot) int {
		y, w := s.CreatedAt.ISOWeek()
		return y*100 + w
	}, p.KeepWeekly)
	keepBuckets(func(s Snapshot) int { return s.CreatedAt.Year()*100 + int(s.CreatedAt.Month()) }, p.KeepMonthly)

	var remove []Snapshot
	for _, s := range snaps {
		if !keep[s.ID] {
			remove = append(remove, s)
		}
	}
	return keep, remove
}
