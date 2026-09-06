package repo

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"crysync/internal/backend"
	"crysync/internal/core/crypto"
	"crysync/internal/core/meta"
)

type controlledBackend struct {
	*backend.InMemory
	put func(context.Context, string, []byte) error
}

func (b *controlledBackend) Put(name string, data []byte) error {
	return b.PutContext(context.Background(), name, data)
}
func (b *controlledBackend) PutContext(ctx context.Context, name string, data []byte) error {
	if b.put != nil {
		return b.put(ctx, name, data)
	}
	return b.InMemory.PutContext(ctx, name, data)
}

func newStreamingTestRepo(t *testing.T, be backend.Backend, chunkSize, slots int) (*Repo, *meta.DB) {
	t.Helper()
	db, err := meta.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	key, _ := crypto.GenerateKey()
	r := New(db, be, key, chunkSize)
	r.SetInflightLimiter(NewInflightLimiter(slots))
	return r, db
}

type countingReader struct {
	mu     sync.Mutex
	data   []byte
	reads  int
	block  chan struct{}
	closed chan struct{}
	once   sync.Once
}

func (r *countingReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	r.reads++
	readNo := r.reads
	r.mu.Unlock()
	if readNo > 1 && r.block != nil {
		select {
		case <-r.block:
		case <-r.closed:
			return 0, io.ErrClosedPipe
		}
	}
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, nil
}

func (r *countingReader) Close() error {
	r.once.Do(func() { close(r.closed) })
	return nil
}
func (r *countingReader) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reads
}

func TestPutFileInflightBackpressure(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var startedOnce sync.Once
	be := &controlledBackend{InMemory: backend.NewInMemory()}
	be.put = func(ctx context.Context, name string, data []byte) error {
		startedOnce.Do(func() { close(started) })
		select {
		case <-release:
			return be.InMemory.PutContext(ctx, name, data)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	r, _ := newStreamingTestRepo(t, be, 4, 1)
	reader := &countingReader{data: []byte("abcdefgh"), block: make(chan struct{}), closed: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		_, err := r.PutFileContext(context.Background(), "x", 0o644, 1, reader)
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("首块未开始上传")
	}
	if got := reader.count(); got != 1 {
		t.Fatalf("inflight=1 时首块上传期间不应读取第二块，Read 次数=%d", got)
	}
	close(release)
	close(reader.block)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("流式上传未收口")
	}
	if got := reader.count(); got < 2 {
		t.Fatalf("释放槽位后应继续读取，Read 次数=%d", got)
	}
}

func TestPutFileBackendFailureClosesSource(t *testing.T) {
	be := &controlledBackend{InMemory: backend.NewInMemory()}
	be.put = func(context.Context, string, []byte) error { return errors.New("backend failed") }
	r, _ := newStreamingTestRepo(t, be, 4, 1)
	reader := &countingReader{data: []byte("abcdefgh"), block: make(chan struct{}), closed: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		_, err := r.PutFileContext(context.Background(), "x", 0o644, 1, reader)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil || !bytes.Contains([]byte(err.Error()), []byte("backend failed")) {
			t.Fatalf("应返回后端错误，得到 %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("后端失败后仍等待客户端 EOF")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	release, err := r.inflightLimiter.Acquire(ctx)
	if err != nil {
		t.Fatalf("失败后 inflight 槽位未释放: %v", err)
	}
	release()
	if _, ok, err := r.GetFileRow("x"); err != nil || ok {
		t.Fatalf("失败上传不应创建文件行: ok=%v err=%v", ok, err)
	}
}

func TestPutFileContextCancelClosesSourceAndReusesSlot(t *testing.T) {
	started := make(chan struct{})
	be := &controlledBackend{InMemory: backend.NewInMemory()}
	var startedOnce sync.Once
	be.put = func(ctx context.Context, name string, data []byte) error {
		startedOnce.Do(func() { close(started) })
		<-ctx.Done()
		return ctx.Err()
	}
	r, _ := newStreamingTestRepo(t, be, 4, 1)
	reader := &countingReader{data: []byte("abcdefgh"), block: make(chan struct{}), closed: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := r.PutFileContext(ctx, "x", 0o644, 1, reader)
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("首块未开始上传")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("取消应返回 context.Canceled，得到 %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("取消后流式上传未收口")
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	release, err := r.inflightLimiter.Acquire(ctx2)
	if err != nil {
		t.Fatalf("取消后 inflight 槽位未释放: %v", err)
	}
	release()
}

func TestPutFileContextCancelAfterLockWait(t *testing.T) {
	be := backend.NewInMemory()
	r, _ := newStreamingTestRepo(t, be, 4, 1)
	unlock := r.WriteSessionLock()
	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_, err := r.PutFileContext(ctx, "x", 0o644, 1, bytes.NewReader([]byte("data")))
		done <- err
	}()
	// 等待块已上传、调用方进入锁等待，再取消。
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if blobs, _ := be.List(); len(blobs) == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if blobs, _ := be.List(); len(blobs) != 1 {
		t.Fatal("上传块未在锁等待测试中完成")
	}
	cancel()
	unlock()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("取消应返回 context.Canceled，得到 %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("锁等待取消未收口")
	}
	if _, ok, err := r.GetFileRow("x"); err != nil || ok {
		t.Fatalf("锁等待取消不应提交文件: ok=%v err=%v", ok, err)
	}
}
