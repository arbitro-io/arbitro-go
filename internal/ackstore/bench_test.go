package ackstore

import "testing"

// Bench workload mirrors the client hot path:
//   - Fresh(seq) gate (lock-free) for the common "definitely new" case.
//   - CheckRecord(seq) test-and-set when recording a processed message.
//   - Sync() every N records (the ack-batch flush cadence).

func newMemStore(b *testing.B) Store { return NewMemory(1_000_000) }

func newWALStore(b *testing.B, fsync bool) Store {
	b.Helper()
	w, err := OpenWAL(Config{Dir: b.TempDir(), Fsync: fsync, MaxCap: 1_000_000})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { w.Close() })
	return w
}

// BenchmarkFreshHot — the lock-free bounds gate (seq always > max → fresh).
func BenchmarkFreshHot(b *testing.B) {
	run := func(b *testing.B, s Store) {
		ref, _ := s.Slot("stream", "consumer")
		for seq := uint64(1); seq <= 100; seq++ {
			ref.CheckRecord(seq)
		}
		s.Sync()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if !ref.Fresh(uint64(10_000 + i)) {
				b.Fatal("unexpected non-fresh")
			}
		}
	}
	b.Run("memory", func(b *testing.B) { run(b, newMemStore(b)) })
	b.Run("wal", func(b *testing.B) { run(b, newWALStore(b, false)) })
}

// BenchmarkCheckRecord — test-and-set + persist (Sync every 100).
func BenchmarkCheckRecord(b *testing.B) {
	run := func(b *testing.B, s Store) {
		ref, _ := s.Slot("stream", "consumer")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ref.CheckRecord(uint64(i))
			if (i+1)%100 == 0 {
				s.Sync()
			}
		}
		b.StopTimer()
		s.Sync()
	}
	b.Run("memory", func(b *testing.B) { run(b, newMemStore(b)) })
	b.Run("wal-nofsync", func(b *testing.B) { run(b, newWALStore(b, false)) })
	b.Run("wal-fsync", func(b *testing.B) { run(b, newWALStore(b, true)) })
}

// BenchmarkRecordSyncBurst — 1000 records + 1 Sync (real ack-batch shape).
func BenchmarkRecordSyncBurst(b *testing.B) {
	const burst = 1000
	run := func(b *testing.B, s Store) {
		ref, _ := s.Slot("stream", "consumer")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			base := uint64(i) * burst
			for j := uint64(0); j < burst; j++ {
				ref.CheckRecord(base + j)
			}
			s.Sync()
		}
	}
	b.Run("memory", func(b *testing.B) { run(b, newMemStore(b)) })
	b.Run("wal-nofsync", func(b *testing.B) { run(b, newWALStore(b, false)) })
	b.Run("wal-fsync", func(b *testing.B) { run(b, newWALStore(b, true)) })
}

// BenchmarkRestore — rebuild from disk after populating.
func BenchmarkRestore(b *testing.B) {
	dir := b.TempDir()
	seed, err := OpenWAL(Config{Dir: dir, Fsync: true, MaxCap: 1_000_000})
	if err != nil {
		b.Fatal(err)
	}
	ref, _ := seed.Slot("stream", "consumer")
	const preload = 50_000
	for i := uint64(0); i < preload; i++ {
		ref.CheckRecord(i)
	}
	seed.Sync()
	seed.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w, err := OpenWAL(Config{Dir: dir, MaxCap: 1_000_000, nowFn: func() int64 { return 0 }})
		if err != nil {
			b.Fatal(err)
		}
		w.Close()
	}
}
