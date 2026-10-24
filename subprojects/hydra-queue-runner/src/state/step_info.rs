use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use db::models::BuildID;
use harmonia_store_derivation::derivation::{BasicDerivation, Derivation};
use harmonia_store_derivation::derived_path::{OutputName, SingleDerivedPath};
use harmonia_store_path::{StoreDir, StorePath};

use super::Step;
use super::drv::flatten_chain;
use crate::config::StepSortFn;

/// Resolve an input-addressed derivation output from the `.drv` file on
/// disk.
///
/// The build-history lookup in the database can miss outputs that never
/// got a successful build step row, e.g. paths that were already valid
/// when the step was created or whose step was aborted by a restart
/// after the build finished. Worse, historical rows can actively
/// mislabel an output: distinct steps of one build have recorded
/// different paths (e.g. a `doc` path) under the same `out` name, and
/// the SQL lookup orders by build only, so it can return the wrong path
/// even when a row exists. For input-addressed derivations the output
/// path is fixed by the derivation itself, so the file is authoritative
/// and must be consulted before trusting any database row.
///
/// Returns [`None`] for dynamic outputs (`Deferred`, `CAFloating`,
/// `Impure`), which declare no path, and when the file cannot be read.
fn resolve_from_drv_file(
    store_dir: &StoreDir,
    drv_path: &StorePath,
    output_name: &OutputName,
) -> Option<StorePath> {
    let path = std::path::PathBuf::from(store_dir.display(drv_path).to_string());
    let content = fs_err::read(path).ok()?;
    let name = drv_path.name().strip_suffix(".drv")?.parse().ok()?;
    let drv = harmonia_store_aterm::parse_derivation_aterm(store_dir, &content, name).ok()?;
    let output = drv.outputs.get(output_name)?;
    output
        .path(store_dir, &drv.name, output_name)
        .ok()
        .flatten()
}

/// Resolve each link from its immutable declaration before consulting
/// build history. A DB-only link must not make a later fixed link trust
/// history, and a fixed link must select the next drv before a DB lookup.
fn resolve_chain(
    root: &StorePath,
    outputs: &[&OutputName],
    mut declared: impl FnMut(&StorePath, &OutputName) -> Option<StorePath>,
    mut history: impl FnMut(&StorePath, &OutputName) -> Option<StorePath>,
) -> Option<StorePath> {
    let mut current = root.clone();
    for output_name in outputs {
        current = declared(&current, output_name).or_else(|| history(&current, output_name))?;
    }
    Some(current)
}

/// `flatten_chain` appends each outer output to the root-first vector.
/// Consume that order directly; reversing it swaps distinct nested names.
fn resolve_input(
    drv_path: &SingleDerivedPath,
    output_name: &OutputName,
    declared: impl FnMut(&StorePath, &OutputName) -> Option<StorePath>,
    history: impl FnMut(&StorePath, &OutputName) -> Option<StorePath>,
) -> Option<StorePath> {
    let (root, chain) = flatten_chain(drv_path, output_name);
    let outputs: Vec<_> = chain.0.iter().collect();
    resolve_chain(&root, &outputs, declared, history)
}

#[cfg(test)]
fn resolve_chain_from_drv_files(
    store_dir: &StoreDir,
    root: &StorePath,
    outputs: &[&OutputName],
) -> Option<StorePath> {
    resolve_chain(
        root,
        outputs,
        |path, output| resolve_from_drv_file(store_dir, path, output),
        |_, _| None,
    )
}

#[derive(Debug)]
pub struct StepInfo {
    pub step: Arc<Step>,
    already_scheduled: AtomicBool,
    cancelled: AtomicBool,
    pub runnable_since: jiff::Timestamp,
    lowest_share_used: atomic_float::AtomicF64,
}

impl StepInfo {
    pub fn new(step: Arc<Step>) -> Self {
        Self {
            already_scheduled: false.into(),
            cancelled: false.into(),
            runnable_since: step.get_runnable_since(),
            lowest_share_used: step.get_lowest_share_used().into(),
            step,
        }
    }

    /// Resolve a derivation's inputs into concrete store paths, returning a
    /// [`BasicDerivation`](BasicDerivation).
    ///
    /// Returns [`None`] if a required output cannot be resolved from its
    /// declaration or build history.
    ///
    /// If the derivation has no [`Built`](SingleDerivedPath::Built) inputs, it is
    /// already resolved; the inputs are simply flattened to a [`StorePathSet`].
    ///
    /// We only need a store dir, not a store, because all the info we
    /// need comes from the Hydra database and the `.drv` files in the
    /// store dir.
    pub(super) async fn try_resolve_force(
        store_dir: &StoreDir,
        db: &db::Database,
        drv: &Derivation,
    ) -> Option<BasicDerivation> {
        // If there are no Built inputs, the derivation is already resolved.
        let has_built_inputs = drv
            .inputs
            .iter()
            .any(|i| matches!(i, SingleDerivedPath::Built { .. }));
        if !has_built_inputs {
            return Some(drv.clone().map_inputs(|inputs| {
                inputs
                    .into_iter()
                    .map(|sdp| match sdp {
                        SingleDerivedPath::Opaque(p) => p,
                        SingleDerivedPath::Built { .. } => unreachable!(),
                    })
                    .collect()
            }));
        }

        let mut conn = db.get().await.ok()?;

        drv.try_resolve_force(store_dir, &mut |inputs| {
            tokio::task::block_in_place(|| {
                let rt = tokio::runtime::Handle::current();
                inputs
                    .iter()
                    .map(|(drv_path, output_name)| {
                        resolve_input(
                            drv_path,
                            output_name,
                            |path, output| resolve_from_drv_file(store_dir, path, output),
                            |path, output| {
                                let names = [output];
                                let chain = [(path, names.as_slice())];
                                // Keep the alias/status-13 handling of the recursive
                                // resolver, but resolve only this one unknown link.
                                rt.block_on(conn.resolve_drv_output_chains(store_dir, &chain))
                                    .map_or_else(
                                        |e| {
                                            tracing::warn!("resolve_drv_output_chains failed: {e}");
                                            None
                                        },
                                        |paths| paths.into_iter().next().flatten(),
                                    )
                            },
                        )
                    })
                    .collect()
            })
        })
    }

    pub fn update_internal_stats(&self) {
        self.lowest_share_used
            .store(self.step.get_lowest_share_used(), Ordering::Relaxed);
    }

    pub fn get_lowest_share_used(&self) -> f64 {
        self.lowest_share_used.load(Ordering::Relaxed)
    }

    pub fn get_highest_global_priority(&self) -> i32 {
        self.step
            .atomic_state
            .highest_global_priority
            .load(Ordering::Relaxed)
    }

    pub fn get_highest_local_priority(&self) -> i32 {
        self.step
            .atomic_state
            .highest_local_priority
            .load(Ordering::Relaxed)
    }

    pub fn get_lowest_build_id(&self) -> BuildID {
        self.step
            .atomic_state
            .lowest_build_id
            .load(Ordering::Relaxed)
    }

    pub fn get_already_scheduled(&self) -> bool {
        self.already_scheduled.load(Ordering::SeqCst)
    }

    pub fn set_already_scheduled(&self, v: bool) {
        self.already_scheduled.store(v, Ordering::SeqCst);
    }

    pub fn set_cancelled(&self, v: bool) {
        self.cancelled.store(v, Ordering::SeqCst);
    }

    pub fn get_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    /// Snapshot the fields that `sort_fn` orders the queue by.
    pub(super) fn sort_key(&self, sort_fn: StepSortFn) -> SortKey {
        SortKey {
            global_priority: self.get_highest_global_priority(),
            share_used: self.get_lowest_share_used(),
            sort_fn_weight: match sort_fn {
                StepSortFn::Legacy => 0,
                StepSortFn::WithRdeps => self.step.get_rdeps_size(),
                StepSortFn::WithCriticalPath => self.step.get_cp_length(),
            },
            local_priority: self.get_highest_local_priority(),
            build_id: self.get_lowest_build_id(),
        }
    }
}

/// A step's position in the queue, read once per sort. The fields come from
/// live atomics. If the sort compared the atomics directly, they could change
/// mid-sort and break the total order that `sort_by` requires.
#[derive(Debug)]
pub(super) struct SortKey {
    global_priority: i32,
    share_used: f64,
    /// rdeps count or critical path length, depending on the sort function.
    /// Always 0 for Legacy.
    sort_fn_weight: u64,
    local_priority: i32,
    build_id: BuildID,
}

impl Ord for SortKey {
    /// Smaller keys sort first. Fields in order of precedence: higher global
    /// priority, lower share used, bigger `sort_fn_weight`, higher local priority,
    /// older build.
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .global_priority
            .cmp(&self.global_priority)
            .then(self.share_used.total_cmp(&other.share_used))
            .then(other.sort_fn_weight.cmp(&self.sort_fn_weight))
            .then(other.local_priority.cmp(&self.local_priority))
            .then(self.build_id.cmp(&other.build_id))
    }
}

impl PartialOrd for SortKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for SortKey {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl Eq for SortKey {}

#[cfg(test)]
mod tests {
    use super::*;
    use db::models::BuildID;
    use std::cmp::Ordering::{Equal, Greater, Less};

    /// Write a minimal `.drv` file with the given `ATerm` body into a
    /// scratch store dir. The body may contain `{store}`, which is
    /// replaced with the scratch dir since `ATerm` embeds absolute store
    /// paths. The returned guard keeps the dir alive.
    fn scratch_drv(
        drv_hash: &str,
        drv_name: &str,
        aterm: &str,
    ) -> (tempfile::TempDir, StoreDir, StorePath) {
        let dir = tempfile::tempdir().expect("scratch store dir");
        let store_dir = StoreDir::new(dir.path()).expect("scratch store dir is absolute");
        let drv_path: StorePath = format!("{drv_hash}-{drv_name}.drv").parse().unwrap();
        let aterm = aterm.replace("{store}", &dir.path().display().to_string());
        fs_err::write(dir.path().join(format!("{drv_hash}-{drv_name}.drv")), aterm)
            .expect("write scratch .drv file");
        (dir, store_dir, drv_path)
    }

    fn drv_aterm(outputs: &str, name: &str) -> String {
        format!("Derive([{outputs}],[],[],\"x86_64-linux\",\"/bin/sh\",[],[(\"name\",\"{name}\")])")
    }

    fn cmp(a: &StepInfo, b: &StepInfo, sort_fn: StepSortFn) -> std::cmp::Ordering {
        a.sort_key(sort_fn).cmp(&b.sort_key(sort_fn))
    }

    fn create_test_step(
        highest_global_priority: i32,
        highest_local_priority: i32,
        lowest_build_id: BuildID,
        lowest_share_used: f64,
        rdeps_len: u64,
    ) -> StepInfo {
        let step = Step::new(
            StorePath::from_base_path("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-test.drv").unwrap(),
            Arc::default(),
        );

        step.atomic_state
            .highest_global_priority
            .store(highest_global_priority, Ordering::Relaxed);
        step.atomic_state
            .highest_local_priority
            .store(highest_local_priority, Ordering::Relaxed);
        step.atomic_state
            .lowest_build_id
            .store(lowest_build_id, Ordering::Relaxed);
        step.atomic_state
            .rdeps_len
            .store(rdeps_len, Ordering::Relaxed);

        StepInfo {
            step,
            already_scheduled: false.into(),
            cancelled: false.into(),
            runnable_since: jiff::Timestamp::now(),
            lowest_share_used: lowest_share_used.into(),
        }
    }

    #[test]
    fn test_legacy_sort_key_global_priority() {
        let step1 = create_test_step(10, 1, 1, 1.0, 0);
        let step2 = create_test_step(5, 1, 2, 1.0, 0);

        assert_eq!(cmp(&step1, &step2, StepSortFn::Legacy), Less);
        assert_eq!(cmp(&step2, &step1, StepSortFn::Legacy), Greater);
    }

    #[test]
    fn test_legacy_sort_key_share_used() {
        let step1 = create_test_step(5, 1, 1, 0.5, 0);
        let step2 = create_test_step(5, 1, 2, 1.0, 0);

        assert_eq!(cmp(&step1, &step2, StepSortFn::Legacy), Less);
        assert_eq!(cmp(&step2, &step1, StepSortFn::Legacy), Greater);
    }

    #[test]
    fn test_legacy_sort_key_local_priority() {
        let step1 = create_test_step(5, 10, 1, 1.0, 0);
        let step2 = create_test_step(5, 5, 2, 1.0, 0);

        assert_eq!(cmp(&step1, &step2, StepSortFn::Legacy), Less);
        assert_eq!(cmp(&step2, &step1, StepSortFn::Legacy), Greater);
    }

    #[test]
    fn test_legacy_sort_key_build_id() {
        let step1 = create_test_step(5, 1, 1, 1.0, 0);
        let step2 = create_test_step(5, 1, 2, 1.0, 0);

        assert_eq!(cmp(&step1, &step2, StepSortFn::Legacy), Less);
        assert_eq!(cmp(&step2, &step1, StepSortFn::Legacy), Greater);
    }

    #[test]
    fn test_legacy_sort_key_equal() {
        let step1 = create_test_step(5, 1, 1, 1.0, 0);
        let step2 = create_test_step(5, 1, 1, 1.0, 0);

        assert_eq!(cmp(&step1, &step2, StepSortFn::Legacy), Equal);
    }

    #[test]
    fn test_rdeps_sort_key_global_priority() {
        let step1 = create_test_step(10, 1, 1, 1.0, 0);
        let step2 = create_test_step(5, 1, 2, 1.0, 0);

        assert_eq!(cmp(&step1, &step2, StepSortFn::WithRdeps), Less);
        assert_eq!(cmp(&step2, &step1, StepSortFn::WithRdeps), Greater);
    }

    #[test]
    fn test_rdeps_sort_key_share_used() {
        let step1 = create_test_step(5, 1, 1, 0.5, 0);
        let step2 = create_test_step(5, 1, 2, 1.0, 0);

        assert_eq!(cmp(&step1, &step2, StepSortFn::WithRdeps), Less);
        assert_eq!(cmp(&step2, &step1, StepSortFn::WithRdeps), Greater);
    }

    #[test]
    fn test_rdeps_sort_key_rdeps_size() {
        let step1 = create_test_step(5, 1, 1, 1.0, 10);
        let step2 = create_test_step(5, 1, 2, 1.0, 5);

        assert_eq!(cmp(&step1, &step2, StepSortFn::WithRdeps), Less);
        assert_eq!(cmp(&step2, &step1, StepSortFn::WithRdeps), Greater);
    }

    #[test]
    fn test_rdeps_sort_key_local_priority() {
        let step1 = create_test_step(5, 10, 1, 1.0, 0);
        let step2 = create_test_step(5, 5, 2, 1.0, 0);

        assert_eq!(cmp(&step1, &step2, StepSortFn::WithRdeps), Less);
        assert_eq!(cmp(&step2, &step1, StepSortFn::WithRdeps), Greater);
    }

    #[test]
    fn test_rdeps_sort_key_build_id() {
        let step1 = create_test_step(5, 1, 1, 1.0, 0);
        let step2 = create_test_step(5, 1, 2, 1.0, 0);

        assert_eq!(cmp(&step1, &step2, StepSortFn::WithRdeps), Less);
        assert_eq!(cmp(&step2, &step1, StepSortFn::WithRdeps), Greater);
    }

    #[test]
    fn test_rdeps_sort_key_equal() {
        let step1 = create_test_step(5, 1, 1, 1.0, 0);
        let step2 = create_test_step(5, 1, 1, 1.0, 0);

        assert_eq!(cmp(&step1, &step2, StepSortFn::WithRdeps), Equal);
    }

    #[test]
    fn test_sort_fn_weight_only_for_rdeps() {
        // Same global priority, share used, local priority, and build ID
        // but a different rdeps_len. WithRdeps should order them, Legacy should not.
        let step1 = create_test_step(5, 1, 1, 1.0, 10);
        let step2 = create_test_step(5, 1, 1, 1.0, 5);

        assert_eq!(cmp(&step1, &step2, StepSortFn::Legacy), Equal);

        assert_eq!(cmp(&step1, &step2, StepSortFn::WithRdeps), Less);
        assert_eq!(cmp(&step2, &step1, StepSortFn::WithRdeps), Greater);
    }

    /// Mirrors the production git/alex misresolution: the derivation's
    /// `.drv` file declares `out` as the real output path while
    /// historical buildstepoutputs rows label a different (`doc`/`data`)
    /// path under the same `out` name. The declared file path must win
    /// over any database row; `doc` still resolves under its own name.
    #[test]
    fn drv_file_selects_declared_out_despite_mislabeled_history() {
        let drv_hash = "a".repeat(32);
        let out_hash = "b".repeat(32);
        let doc_hash = "c".repeat(32);
        let aterm = drv_aterm(
            &format!(
                "(\"out\",\"{{store}}/{out_hash}-declared-lib\",\"\",\"\"),\
                 (\"doc\",\"{{store}}/{doc_hash}-declared-lib-doc\",\"\",\"\")"
            ),
            "declared-lib",
        );
        let (_guard, store_dir, drv_path) = scratch_drv(&drv_hash, "declared-lib", &aterm);

        let out: OutputName = "out".parse().unwrap();
        let doc: OutputName = "doc".parse().unwrap();
        let real: StorePath = format!("{out_hash}-declared-lib").parse().unwrap();
        let doc_path: StorePath = format!("{doc_hash}-declared-lib-doc").parse().unwrap();

        // Declared paths resolve straight from the file, no database.
        assert_eq!(
            resolve_from_drv_file(&store_dir, &drv_path, &out),
            Some(real.clone())
        );
        assert_eq!(
            resolve_from_drv_file(&store_dir, &drv_path, &doc),
            Some(doc_path.clone())
        );
        // The chain walk used ahead of the SQL lookup agrees.
        assert_eq!(
            resolve_chain_from_drv_files(&store_dir, &drv_path, &[&out]),
            Some(real.clone())
        );
        // In particular the `doc` path is never returned for `out`.
        assert_ne!(
            resolve_chain_from_drv_files(&store_dir, &drv_path, &[&out]),
            Some(doc_path)
        );
    }

    /// A depth-2 chain walks link by link through the files: the root
    /// drv's `out` names an intermediate `.drv`, whose `lib` output is
    /// the final path.
    #[test]
    fn drv_file_chain_walks_intermediate_drv() {
        let mid_hash = "d".repeat(32);
        let (guard, store_dir, _) = scratch_drv(
            &mid_hash,
            "mid",
            &drv_aterm(
                &format!("(\"lib\",\"{{store}}/{mid_hash}-final\",\"\",\"\")"),
                "mid",
            ),
        );
        // The second link lives in the same scratch store dir.
        let root_hash = "f".repeat(32);
        let root_aterm = drv_aterm(
            &format!("(\"out\",\"{{store}}/{mid_hash}-mid.drv\",\"\",\"\")"),
            "root",
        )
        .replace("{store}", store_dir.to_str());
        let root_path: StorePath = format!("{root_hash}-root.drv").parse().unwrap();
        fs_err::write(
            guard.path().join(format!("{root_hash}-root.drv")),
            &root_aterm,
        )
        .expect("write root .drv file");

        let out: OutputName = "out".parse().unwrap();
        let lib: OutputName = "lib".parse().unwrap();
        let expected: StorePath = format!("{mid_hash}-final").parse().unwrap();
        assert_eq!(
            resolve_chain_from_drv_files(&store_dir, &root_path, &[&out, &lib]),
            Some(expected)
        );
    }

    /// Dynamic outputs declare no path, so the file walk stays silent
    /// and those links keep resolving through the database. The same
    /// holds when the `.drv` file is absent from the store.
    #[test]
    fn drv_file_silent_for_dynamic_or_missing_drv() {
        let out: OutputName = "out".parse().unwrap();
        for (label, outputs) in [
            ("deferred", "(\"out\",\"\",\"\",\"\")"),
            ("floating", "(\"out\",\"\",\"r:sha256\",\"\")"),
            ("impure", "(\"out\",\"\",\"r:sha256\",\"impure\")"),
        ] {
            let drv_hash = "a".repeat(32);
            let aterm = drv_aterm(outputs, label);
            let (_guard, store_dir, drv_path) = scratch_drv(&drv_hash, label, &aterm);
            assert_eq!(
                resolve_from_drv_file(&store_dir, &drv_path, &out),
                None,
                "{label} declares no path"
            );
            assert_eq!(
                resolve_chain_from_drv_files(&store_dir, &drv_path, &[&out]),
                None,
                "{label} chain falls back to the database"
            );
        }

        // Missing file: both helpers stay silent.
        let present_hash = "a".repeat(32);
        let present_aterm = drv_aterm("(\"out\",\"\",\"\",\"\")", "present");
        let (_guard, store_dir, _) = scratch_drv(&present_hash, "present", &present_aterm);
        let absent_hash = "b".repeat(32);
        let absent: StorePath = format!("{absent_hash}-absent.drv").parse().unwrap();
        assert_eq!(resolve_from_drv_file(&store_dir, &absent, &out), None);
        assert_eq!(
            resolve_chain_from_drv_files(&store_dir, &absent, &[&out]),
            None
        );
    }

    #[test]
    fn db_only_link_does_not_override_a_later_declared_output() {
        let path = |name| {
            format!("{}-{name}", "a".repeat(32))
                .parse::<StorePath>()
                .unwrap()
        };
        let root = path("dynamic.drv");
        let middle = path("middle.drv");
        let real = path("real");
        let decoy = path("decoy");
        let out: OutputName = "out".parse().unwrap();
        let mut history_calls = Vec::new();

        let resolved = resolve_chain(
            &root,
            &[&out, &out],
            |drv, _| (drv == &middle).then(|| real.clone()),
            |drv, _| {
                history_calls.push(drv.clone());
                Some(if drv == &root {
                    middle.clone()
                } else {
                    decoy.clone()
                })
            },
        );
        assert_eq!(resolved, Some(real));
        assert_eq!(history_calls, vec![root]);
    }

    #[test]
    fn declared_link_selects_the_drv_for_a_later_db_only_output() {
        let path = |name| {
            format!("{}-{name}", "b".repeat(32))
                .parse::<StorePath>()
                .unwrap()
        };
        let root = path("root.drv");
        let middle = path("dynamic.drv");
        let real = path("real");
        let wrong_middle = path("wrong.drv");
        let out: OutputName = "out".parse().unwrap();
        let mut history_calls = Vec::new();

        let resolved = resolve_chain(
            &root,
            &[&out, &out],
            |drv, _| (drv == &root).then(|| middle.clone()),
            |drv, _| {
                history_calls.push(drv.clone());
                if drv == &root {
                    Some(wrong_middle.clone())
                } else if drv == &middle {
                    Some(real.clone())
                } else {
                    None
                }
            },
        );
        assert_eq!(resolved, Some(real));
        assert_eq!(history_calls, vec![middle]);
    }

    #[test]
    fn nested_input_resolves_distinct_output_names_from_root_to_leaf() {
        let path = |name| {
            format!("{}-{name}", "c".repeat(32))
                .parse::<StorePath>()
                .unwrap()
        };
        let root = path("root.drv");
        let middle = path("middle.drv");
        let final_path = path("final");
        let foo: OutputName = "foo".parse().unwrap();
        let bar: OutputName = "bar".parse().unwrap();
        let nested = SingleDerivedPath::Built {
            drv_path: Arc::new(SingleDerivedPath::Opaque(root.clone())),
            output: foo.clone(),
        };
        let mut lookups = Vec::new();
        let result = resolve_input(
            &nested,
            &bar,
            |_, _| None,
            |drv, output| {
                lookups.push((drv.clone(), output.clone()));
                if drv == &root && output == &foo {
                    Some(middle.clone())
                } else if drv == &middle && output == &bar {
                    Some(final_path.clone())
                } else {
                    None
                }
            },
        );
        assert_eq!(result, Some(final_path));
        assert_eq!(lookups, vec![(root, foo), (middle, bar)]);
    }
}
