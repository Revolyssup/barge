package storage_test

import (
	"testing"

	"github.com/example/raft/storage"
)

func TestInMemoryStorage_SetGet(t *testing.T) {
	s := storage.NewInMemoryStorage()
	if err := s.Set("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	v, err := s.Get("k")
	if err != nil {
		t.Fatal(err)
	}
	if string(v) != "v" {
		t.Fatalf("got %q, want %q", v, "v")
	}
}

func TestInMemoryStorage_MissingKey(t *testing.T) {
	s := storage.NewInMemoryStorage()
	_, err := s.Get("missing")
	if err == nil {
		t.Fatal("expected error for missing key")
	}
}

func TestInMemoryStorage_Delete(t *testing.T) {
	s := storage.NewInMemoryStorage()
	_ = s.Set("k", []byte("v"))
	_ = s.Delete("k")
	_, err := s.Get("k")
	if err == nil {
		t.Fatal("expected error after delete")
	}
}

func TestInMemoryStorage_Overwrite(t *testing.T) {
	s := storage.NewInMemoryStorage()
	_ = s.Set("k", []byte("v1"))
	_ = s.Set("k", []byte("v2"))
	v, _ := s.Get("k")
	if string(v) != "v2" {
		t.Fatalf("got %q, want v2", v)
	}
}

func TestInMemoryLogStorage_AppendAndGet(t *testing.T) {
	ls := storage.NewInMemoryLogStorage()
	e := storage.LogEntry{Index: 1, Term: 1, Command: []byte("hello")}
	if err := ls.AppendLog(e); err != nil {
		t.Fatal(err)
	}
	got, err := ls.GetLog(1)
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Command) != "hello" {
		t.Fatalf("got %q, want hello", got.Command)
	}
}

func TestInMemoryLogStorage_LastIndexTerm(t *testing.T) {
	ls := storage.NewInMemoryLogStorage()
	if ls.LastIndex() != 0 {
		t.Fatal("empty log should have LastIndex 0")
	}
	_ = ls.AppendLog(storage.LogEntry{Index: 1, Term: 3})
	if ls.LastIndex() != 1 {
		t.Fatalf("expected LastIndex 1, got %d", ls.LastIndex())
	}
	if ls.LastTerm() != 3 {
		t.Fatalf("expected LastTerm 3, got %d", ls.LastTerm())
	}
}

func TestInMemoryLogStorage_TruncateSuffix(t *testing.T) {
	ls := storage.NewInMemoryLogStorage()
	for i := uint64(1); i <= 5; i++ {
		_ = ls.AppendLog(storage.LogEntry{Index: i, Term: 1})
	}
	_ = ls.TruncateSuffix(3)
	if ls.LastIndex() != 2 {
		t.Fatalf("expected LastIndex 2 after truncate, got %d", ls.LastIndex())
	}
}

func TestInMemoryLogStorage_Entries(t *testing.T) {
	ls := storage.NewInMemoryLogStorage()
	for i := uint64(1); i <= 4; i++ {
		_ = ls.AppendLog(storage.LogEntry{Index: i, Term: 1, Command: []byte{byte(i)}})
	}
	entries, err := ls.Entries(1, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}
}
