use std::hint::black_box;
use std::time::Duration;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use upgrid_raft::benchmark::PersistenceBench;

fn persistence(criterion: &mut Criterion) {
    criterion.bench_function("fresh_migration_open", |bencher| {
        bencher.iter(|| black_box(PersistenceBench::new(0).unwrap()));
    });
    criterion.bench_function("log_append_1", |bencher| {
        bencher.iter_batched(
            || PersistenceBench::new(0).unwrap(),
            |bench| bench.append(1).unwrap(),
            BatchSize::SmallInput,
        );
    });
    criterion.bench_function("log_append_256", |bencher| {
        bencher.iter_batched(
            || PersistenceBench::new(0).unwrap(),
            |bench| bench.append(256).unwrap(),
            BatchSize::SmallInput,
        );
    });
    criterion.bench_function("log_range_read", |bencher| {
        bencher.iter_batched(
            || {
                let bench = PersistenceBench::new(0).unwrap();
                bench.seed_log(256).unwrap();
                bench
            },
            |bench| black_box(bench.range_read(64..192).unwrap()),
            BatchSize::SmallInput,
        );
    });
    criterion.bench_function("log_conflict_delete", |bencher| {
        bencher.iter_batched(
            || {
                let bench = PersistenceBench::new(0).unwrap();
                bench.seed_log(256).unwrap();
                bench
            },
            |bench| bench.conflict_delete(127).unwrap(),
            BatchSize::SmallInput,
        );
    });
    criterion.bench_function("log_purge", |bencher| {
        bencher.iter_batched(
            || {
                let bench = PersistenceBench::new(0).unwrap();
                bench.seed_log(256).unwrap();
                bench
            },
            |bench| bench.purge(128).unwrap(),
            BatchSize::SmallInput,
        );
    });
    criterion.bench_function("metadata_vote_update", |bencher| {
        bencher.iter_batched(
            || PersistenceBench::new(0).unwrap(),
            |bench| bench.update_vote().unwrap(),
            BatchSize::SmallInput,
        );
    });
    criterion.bench_function("metadata_committed_update", |bencher| {
        bencher.iter_batched(
            || PersistenceBench::new(0).unwrap(),
            |bench| bench.update_committed().unwrap(),
            BatchSize::SmallInput,
        );
    });
    state_benchmarks(criterion, 0, "small");
    state_benchmarks(criterion, 1_000, "1000_targets");
}

fn state_benchmarks(criterion: &mut Criterion, target_count: usize, label: &str) {
    criterion.bench_function(&format!("state_checkpoint_write_{label}"), |bencher| {
        bencher.iter_batched(
            || PersistenceBench::new(target_count).unwrap(),
            |bench| bench.write_checkpoint().unwrap(),
            BatchSize::SmallInput,
        );
    });
    criterion.bench_function(&format!("state_checkpoint_read_{label}"), |bencher| {
        bencher.iter_batched(
            || {
                let bench = PersistenceBench::new(target_count).unwrap();
                bench.write_checkpoint().unwrap();
                bench
            },
            |bench| black_box(bench.read_state().unwrap()),
            BatchSize::SmallInput,
        );
    });
    criterion.bench_function(&format!("snapshot_replace_{label}"), |bencher| {
        bencher.iter_batched(
            || PersistenceBench::new(target_count).unwrap(),
            |bench| bench.replace_snapshot().unwrap(),
            BatchSize::SmallInput,
        );
    });
    criterion.bench_function(&format!("snapshot_read_{label}"), |bencher| {
        bencher.iter_batched(
            || {
                let bench = PersistenceBench::new(target_count).unwrap();
                bench.replace_snapshot().unwrap();
                bench
            },
            |bench| black_box(bench.read_state().unwrap()),
            BatchSize::SmallInput,
        );
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(1));
    targets = persistence
}
criterion_main!(benches);
