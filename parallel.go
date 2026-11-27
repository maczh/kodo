package kodo

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
)

// DeleteMany 并发删除多个 key。
//
// 语义：
//   - concurrency <= 0 时使用配置的 maxConcurrency（默认 8），超过 64 会被钳制为 64；
//   - ctx 取消即停止派发新任务（已派发的会被各自 ctx 感知）；
//   - 每个 key 的调用都包了 defer recover，单个 key 的 panic 只影响该 key；
//   - 返回聚合错误 *MultiError（内含 *KeyError），全部失败项都会返回而不是只返回第一个；
//   - 单个 key 不存在不算失败（Delete 本身幂等）。
func (b *Bucket) DeleteMany(ctx context.Context, keys []string, concurrency int) (err error) {
	defer recoverToError(&err, "DeleteMany")
	return b.runMany(ctx, "DeleteMany", keys, concurrency, func(ctx context.Context, key string) error {
		return b.Delete(ctx, key)
	})
}

// DownloadMany 并发下载多个 key，返回 key -> 内容 的 map。
//
// 并发/取消/panic 语义与 DeleteMany 一致；任一 key 失败不会丢弃其他已成功的下载结果，
// 但出错时返回的 map 仍包含成功的项，便于调用方按需使用。
//
// **内存约束**：所有结果会同时驻留内存，因此整批共用一个总量预算
// （maxDownloadManyBytes，默认 256MiB，硬上限 2GiB），超出后后续 key 会以
// ErrDownloadTooLarge 失败而不是把进程打爆。大文件批量下载请改用 DownloadFile。
func (b *Bucket) DownloadMany(ctx context.Context, keys []string, concurrency int) (data map[string][]byte, err error) {
	defer recoverToError(&err, "DownloadMany")
	if err := b.ready(); err != nil {
		return nil, err
	}
	budget := newByteBudget(b.cfg.downloadManyLimit())
	result := make(map[string][]byte, mapHint(len(keys)))
	var mu sync.Mutex
	err = b.runMany(ctx, "DownloadMany", keys, concurrency, func(ctx context.Context, key string) error {
		content, downloadErr := b.downloadBytes(ctx, key, 0, budget)
		if downloadErr != nil {
			return downloadErr
		}
		// 用 defer 释放互斥量：若赋值本身 panic（例如极端内存压力），
		// 不释放会让其他 worker 永远阻塞在 Lock 上，runMany 的 wg.Wait()
		// 随之永久挂死 —— 「被 recover 的 panic」不该演变成死锁。
		func() {
			mu.Lock()
			defer mu.Unlock()
			result[key] = content
		}()
		return nil
	})
	return result, err
}

// ExistsMany 并发判断多个 key 是否存在，返回 key -> 是否存在。
func (b *Bucket) ExistsMany(ctx context.Context, keys []string, concurrency int) (exists map[string]bool, err error) {
	defer recoverToError(&err, "ExistsMany")
	if err := b.ready(); err != nil {
		return nil, err
	}
	result := make(map[string]bool, mapHint(len(keys)))
	var mu sync.Mutex
	err = b.runMany(ctx, "ExistsMany", keys, concurrency, func(ctx context.Context, key string) error {
		ok, existsErr := b.Exists(ctx, key)
		if existsErr != nil {
			return existsErr
		}
		func() {
			mu.Lock()
			defer mu.Unlock()
			result[key] = ok
		}()
		return nil
	})
	return result, err
}

// maxMapHint 是 map 预分配提示的上限。
//
// keys 由调用方提供，长度不可控。make(map, len(keys)) 会按 n 一次性预分配桶数组；
// 当 n 达到千万级时，这次预分配本身就可能是数百 MB 的瞬时占用（OOM 不可 recover）。
// 预分配只是「避免多次扩容」的优化，钳制到合理值即可，实际容量会随插入自动增长。
const maxMapHint = 4096

// mapHint 把预分配提示钳制到安全区间。
func mapHint(n int) int {
	if n <= 0 {
		return 0
	}
	if n > maxMapHint {
		return maxMapHint
	}
	return n
}

// runMany 是批量操作的工作池实现。
//
// 错误收集按 key 原下标存放，保证聚合结果顺序稳定、可定位到具体 key。
// 未派发的 key 在 ctx 取消时会记录 ctx 错误，避免「静默当作成功」。
func (b *Bucket) runMany(ctx context.Context, op string, keys []string, concurrency int, fn func(context.Context, string) error) error {
	if err := b.ready(); err != nil {
		return err
	}
	if fn == nil {
		return nil
	}
	if len(keys) == 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	workers := normalizeConcurrency(concurrency, b.cfg)

	errs := make([]error, len(keys))
	attempted := make([]bool, len(keys))
	indexCh := make(chan int)

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for idx := range indexCh {
				if idx < 0 || idx >= len(keys) {
					continue
				}
				key := keys[idx]
				attempted[idx] = true
				errs[idx] = callSafely(ctx, op, key, func() error {
					return fn(ctx, key)
				})
			}
		}()
	}

dispatch:
	for i := range keys {
		select {
		case <-ctx.Done():
			break dispatch
		case indexCh <- i:
		}
	}
	close(indexCh)
	wg.Wait()

	// 失败项切片按「少量失败」预估容量：全部成功的常见路径不该按 len(keys)
	// 预先占住 len(keys)*16 字节（百万级 key 就是十几 MB 的纯浪费）。
	failureHint := len(keys)
	if failureHint > 64 {
		failureHint = 64
	}
	failures := make([]error, 0, failureHint)
	for i, e := range errs {
		if e != nil {
			failures = append(failures, &KeyError{Key: keys[i], Err: e})
			continue
		}
		if !attempted[i] {
			if ctxErr := ctx.Err(); ctxErr != nil {
				failures = append(failures, &KeyError{Key: keys[i], Err: ctxErr})
			}
		}
	}
	if len(failures) == 0 {
		return nil
	}
	return &MultiError{Op: op, Errors: failures}
}

// callSafely 在单次调用外层加 recover，panic 只影响当前 key。
//
// recover() 必须在这个 defer 的顶层函数体内直接调用，否则不生效。
func callSafely(ctx context.Context, op, key string, fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			// 已处于 recover 路径：格式化与日志都必须 panic 安全
			detail := panicText(r)
			logError(fmt.Sprintf("[kodo] panic in %s on key %q: %s\n%s", op, key, detail, debug.Stack()))
			err = fmt.Errorf("kodo: panic in %s on key %q: %s", op, key, detail)
		}
	}()
	if ctx != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
	}
	return fn()
}

// normalizeConcurrency 归一化并发度：<=0 取配置默认值，硬上限 64。
func normalizeConcurrency(n int, cfg *Config) int {
	if n <= 0 {
		n = DefaultConcurrency
		if cfg != nil && cfg.MaxConcurrency > 0 {
			n = cfg.MaxConcurrency
		}
	}
	if n > MaxConcurrency {
		n = MaxConcurrency
	}
	if n < 1 {
		n = 1
	}
	return n
}
