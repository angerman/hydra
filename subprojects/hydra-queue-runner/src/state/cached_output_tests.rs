#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use db::models::BuildStatus;
use harmonia_store_path::{StoreDir, StorePath};
use sqlx::Row as _;

use super::{
    RealiseStepResult, State,
    machine::{Machine, Machines},
    metrics::PromMetrics,
    step_info::StepInfo,
    uploader::Uploader,
};
use crate::config::{App, MtlsConfig, StepSortFn};

static TEST_ID: AtomicU64 = AtomicU64::new(0);
struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new(label: &str) -> Self {
        let id = TEST_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "hydra-queue-runner-{label}-{}-{id}",
            std::process::id()
        ));
        fs_err::create_dir_all(&path).expect("create test directory");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs_err::remove_dir_all(&self.0);
    }
}

struct FixedOutputDrvs {
    root_drv: StorePath,
    root_output: StorePath,
    dependent_drv: StorePath,
    dependent_output: StorePath,
}

fn private_store_guard() -> PathBuf {
    let root = std::env::var_os("HYDRA_QUEUE_TEST_STORE")
        .expect("test driver must select a private Nix store");
    let root = fs_err::canonicalize(root).expect("private test-store root must exist");
    assert!(root.is_absolute(), "private store root must be absolute");
    assert!(
        !root.starts_with("/nix"),
        "the test store must never be the host /nix tree"
    );

    let expected_store = root.join("store");
    let store_dir = std::env::var_os("NIX_STORE_DIR")
        .map(PathBuf::from)
        .expect("test driver must set NIX_STORE_DIR to the private store");
    assert!(
        store_dir.is_absolute() && store_dir == expected_store,
        "NIX_STORE_DIR must be the guarded private store directory"
    );
    let state_dir = root.join("state");
    let log_dir = root.join("log");
    let remote = std::env::var("NIX_REMOTE")
        .expect("test driver must configure an explicit local private store URI");
    assert_eq!(
        remote,
        format!(
            "local?store={}&state={}&log={}",
            expected_store.display(),
            state_dir.display(),
            log_dir.display()
        ),
        "NIX_REMOTE must select the guarded private store, state, and log paths"
    );
    assert_eq!(
        std::env::var_os("NIX_STATE_DIR").map(PathBuf::from),
        Some(state_dir),
        "NIX_STATE_DIR must be inside the guarded test root"
    );
    assert_eq!(
        std::env::var_os("NIX_LOG_DIR").map(PathBuf::from),
        Some(log_dir),
        "NIX_LOG_DIR must be inside the guarded test root"
    );
    assert_eq!(
        std::env::var_os("NIX_CONF_DIR").map(PathBuf::from),
        Some(root.join("conf")),
        "NIX_CONF_DIR must be inside the guarded test root"
    );
    assert_eq!(
        std::env::var_os("NIX_USER_CONF_FILES").as_deref(),
        Some(std::ffi::OsStr::new("/dev/null")),
        "the test must not read user Nix configuration"
    );
    assert!(
        std::env::var_os("HYDRA_DATABASE_URL").is_none(),
        "HYDRA_DATABASE_URL can override the private TestPg database"
    );
    root
}

fn init_private_store(root: &Path) -> StoreDir {
    let nix_config = daemon_client_utils::parse_nix_remote()
        .expect("test driver must configure a private Nix store");
    let store_dir = nix_config.store_dir.clone();
    let store_dir_path = PathBuf::from(store_dir.to_string());
    assert!(
        store_dir_path.is_absolute() && store_dir_path.starts_with(root),
        "configured store {} is outside guarded test root {}",
        store_dir_path.display(),
        root.display()
    );
    assert_eq!(
        std::env::var_os("NIX_STORE_DIR").map(PathBuf::from),
        Some(store_dir_path),
        "NIX_STORE_DIR must match the configured private store"
    );
    store_dir
}

fn command_output(program: &str, args: &[&str]) -> std::process::Output {
    Command::new(program)
        .args(args)
        .output()
        .unwrap_or_else(|error| panic!("failed to run {program}: {error}"))
}

fn require_success(output: &std::process::Output, operation: &str) {
    assert!(
        output.status.success(),
        "{operation} failed: status={} stdout={} stderr={}",
        output.status,
        String::from_utf8(output.stdout.clone()).expect("command stdout is UTF-8"),
        String::from_utf8(output.stderr.clone()).expect("command stderr is UTF-8")
    );
}

fn create_fixed_output_drvs(
    directory: &Path,
    suffix: &str,
    store_dir: &StoreDir,
) -> FixedOutputDrvs {
    let root_content = "cached root output\n";
    let root_name = format!("cached-root-{suffix}");
    // `nix-store --add-fixed` uses the input file's basename as the output name.
    let root_file = directory.join(&root_name);
    fs_err::write(&root_file, root_content).expect("write fixed root output fixture");

    let root_file_expr =
        serde_json::to_string(&root_file.display().to_string()).expect("encode root fixture path");
    let dependent_name = format!("pending-dependent-{suffix}");
    let expression = format!(
        r#"
        let
          root = derivation {{
            name = "{root_name}";
            system = builtins.currentSystem;
            builder = "/bin/sh";
            args = [ "-c" "exit 1" ];
            outputHashMode = "flat";
            outputHashAlgo = "sha256";
            outputHash = builtins.hashFile "sha256" (builtins.toPath {root_file_expr});
          }};
          dependent = derivation {{
            name = "{dependent_name}";
            system = builtins.currentSystem;
            builder = "/bin/sh";
            args = [ "-c" "exit 1" ];
            rootInput = root;
          }};
        in {{
          rootDrv = root.drvPath;
          rootOutput = root.outPath;
          dependentDrv = dependent.drvPath;
          dependentOutput = dependent.outPath;
        }}
        "#
    );

    let expression_output = command_output(
        "nix-instantiate",
        &[
            "--eval",
            "--json",
            "--strict",
            "--read-write-mode",
            "--expr",
            &expression,
        ],
    );
    require_success(&expression_output, "evaluate fixed-output test derivations");
    let paths: serde_json::Value =
        serde_json::from_slice(&expression_output.stdout).expect("decode derivation paths");
    let parse = |field: &str| {
        store_dir
            .parse(
                paths[field]
                    .as_str()
                    .unwrap_or_else(|| panic!("Nix omitted {field}")),
            )
            .expect("Nix reported an unparseable store path")
    };

    FixedOutputDrvs {
        root_drv: parse("rootDrv"),
        root_output: parse("rootOutput"),
        dependent_drv: parse("dependentDrv"),
        dependent_output: parse("dependentOutput"),
    }
}

fn add_fixed_output(file: &Path, store_dir: &StoreDir) -> StorePath {
    let file = file.display().to_string();
    let output = command_output("nix-store", &["--add-fixed", "sha256", &file]);
    require_success(
        &output,
        "register fixed output in the guarded private store",
    );
    let path = String::from_utf8(output.stdout)
        .expect("nix-store path is UTF-8")
        .trim()
        .to_owned();
    store_dir
        .parse(&path)
        .expect("nix-store reported an unparseable store path")
}

async fn new_state(
    directory: &Path,
    database_url: &str,
    store_dir: &StoreDir,
) -> color_eyre::Result<Arc<State>> {
    let config_path = directory.join("queue-runner.toml");
    let data_dir = directory.join("data");
    let roots_dir = directory.join("roots");
    let config = format!(
        concat!(
            "hydraDataDir = {:?}\n",
            "dbUrl = {:?}\n",
            "rootsDir = {:?}\n",
            "dispatchTriggerTimerInS = 0\n",
            "queueTriggerTimerInS = 0\n",
            "stopQueueRunAfterInS = 0\n",
        ),
        data_dir.display(),
        database_url,
        roots_dir.display()
    );
    fs_err::write(&config_path, config)?;
    fs_err::create_dir_all(&data_dir)?;

    let app = App::init(&config_path.display().to_string())?;
    let db = db::Database::new(database_url, 4).await?;
    let log_dir = app.get_hydra_log_dir();
    fs_err::create_dir_all(&log_dir)?;

    let nix_config = daemon_client_utils::parse_nix_remote()
        .expect("test driver must configure a private Nix store");
    assert_eq!(
        nix_config.store_dir.to_string(),
        store_dir.to_string(),
        "daemon store config must match the guarded private store"
    );
    let connector =
        daemon_client_utils::DaemonConnector::new(nix_config.socket.clone(), store_dir.clone());
    let store = daemon_client_utils::DaemonStoreReader::new(connector.clone());
    let read_pool = daemon_client_utils::DaemonConnPool::new(store.clone(), 4);
    let (upload_completion_tx, upload_completion_rx) = tokio::sync::mpsc::unbounded_channel();
    let real_store_dir = nix_config.real_store_dir();

    Ok(Arc::new(State {
        connector,
        store,
        read_pool,
        remote_stores: parking_lot::RwLock::new(Vec::new()),
        overflow_store: parking_lot::RwLock::new(None),
        config: app,
        mtls: MtlsConfig::default(),
        db,
        machines: Machines::new(),
        log_dir,
        builds: super::Builds::new(),
        jobsets: super::Jobsets::new(),
        steps: super::Steps::new(),
        queues: super::Queues::new(),
        fod_checker: None,
        started_at: jiff::Timestamp::now(),
        build_step_locks: std::array::from_fn(|_| tokio::sync::Mutex::new(())),
        ingest_prefetch_sem: Arc::new(tokio::sync::Semaphore::new(4)),
        metrics: PromMetrics::new()?,
        notify_dispatch: tokio::sync::Notify::new(),
        uploader: Arc::new(
            Uploader::new(data_dir.join("uploader_state.json"), upload_completion_tx).await,
        ),
        upload_completion_rx: parking_lot::Mutex::new(Some(upload_completion_rx)),
        real_store_dir,
        nix_daemon_config: nix_config,
    }))
}

async fn insert_test_builds(
    pool: &sqlx::PgPool,
    store_dir: &StoreDir,
    suffix: &str,
    builds: &[(&str, &StorePath)],
) -> Vec<i32> {
    let user = format!("test-{suffix}");
    let project = format!("queue-recheck-{suffix}");
    let jobset = "test";
    sqlx::query("INSERT INTO users (username, emailaddress, password) VALUES ($1, $2, $3)")
        .bind(&user)
        .bind(format!("{user}@example.invalid"))
        .bind("unused-test-password")
        .execute(pool)
        .await
        .expect("insert test user");
    sqlx::query("INSERT INTO projects (name, displayname, owner) VALUES ($1, $2, $3)")
        .bind(&project)
        .bind(&project)
        .bind(&user)
        .execute(pool)
        .await
        .expect("insert test project");
    let jobset_id: i32 = sqlx::query_scalar(
        "INSERT INTO jobsets (name, project, nixexprinput, nixexprpath, emailoverride) \
         VALUES ($1, $2, 'source', 'default.nix', '') RETURNING id",
    )
    .bind(jobset)
    .bind(&project)
    .fetch_one(pool)
    .await
    .expect("insert test jobset");

    let mut ids = Vec::with_capacity(builds.len());
    for (name, drv_path) in builds {
        let id: i32 = sqlx::query_scalar(
            "INSERT INTO builds (finished, timestamp, jobset_id, job, drvpath, system) \
             VALUES (0, EXTRACT(EPOCH FROM NOW())::INT4, $1, $2, $3, 'x86_64-linux') \
             RETURNING id",
        )
        .bind(jobset_id)
        .bind(name)
        .bind(store_dir.display(*drv_path).to_string())
        .fetch_one(pool)
        .await
        .expect("insert pending test build");
        ids.push(id);
    }
    ids
}

async fn insert_previous_failed_build(
    pool: &sqlx::PgPool,
    store_dir: &StoreDir,
    build_id: i32,
    drv_path: &StorePath,
) {
    let failed_build_id: i32 = sqlx::query_scalar(
        "INSERT INTO builds \
         (finished, timestamp, jobset_id, job, drvpath, system, buildstatus, starttime, stoptime) \
         SELECT 1, EXTRACT(EPOCH FROM NOW())::INT4, jobset_id, 'previous-attempt', $2, \
                'x86_64-linux', $3, 1, 2 FROM builds WHERE id = $1 RETURNING id",
    )
    .bind(build_id)
    .bind(store_dir.display(drv_path).to_string())
    .bind(BuildStatus::Failed as i32)
    .fetch_one(pool)
    .await
    .expect("insert prior failed build");
    sqlx::query(
        "INSERT INTO buildsteps \
         (build, stepnr, type, drvpath, busy, status, starttime, stoptime, machine, system) \
         VALUES ($1, 1, 0, $2, 0, $3, 1, 2, '', 'x86_64-linux')",
    )
    .bind(failed_build_id)
    .bind(store_dir.display(drv_path).to_string())
    .bind(BuildStatus::Failed as i32)
    .execute(pool)
    .await
    .expect("insert prior failed build step");
}

#[derive(Debug, PartialEq, Eq)]
struct BuildDbState {
    finished: i32,
    status: Option<i32>,
    cached: Option<i32>,
}

async fn build_db_state(pool: &sqlx::PgPool, id: i32) -> BuildDbState {
    let row = sqlx::query("SELECT finished, buildstatus, iscachedbuild FROM builds WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("read build state");
    BuildDbState {
        finished: row.try_get("finished").expect("finished column"),
        status: row.try_get("buildstatus").expect("buildstatus column"),
        cached: row.try_get("iscachedbuild").expect("iscachedbuild column"),
    }
}

fn runnable_paths(state: &State) -> HashSet<StorePath> {
    state
        .steps
        .clone_runnable()
        .into_iter()
        .map(|step| step.get_drv_path().clone())
        .collect()
}

fn unfinished_runnable_paths(state: &State) -> HashSet<StorePath> {
    state
        .steps
        .clone_runnable()
        .into_iter()
        .filter(|step| !step.get_finished())
        .map(|step| step.get_drv_path().clone())
        .collect()
}

fn fake_machine() -> Arc<Machine> {
    let (sender, _receiver) = tokio::sync::mpsc::channel(1);
    let join = hydra_proto::JoinMessage {
        machine_id: uuid::Uuid::new_v4().to_string(),
        systems: vec!["x86_64-linux".to_owned()],
        hostname: "cached-output-test-builder".to_owned(),
        cpu_count: 1,
        bogomips: 1.0,
        speed_factor: 1.0,
        max_jobs: 1,
        build_dir_avail_threshold: 0.0,
        store_avail_threshold: 0.0,
        load1_threshold: 0.0,
        cpu_psi_threshold: 0.0,
        mem_psi_threshold: 0.0,
        io_psi_threshold: None,
        total_mem: 1024,
        supported_features: Vec::new(),
        mandatory_features: Vec::new(),
        cgroups: false,
        substituters: Vec::new(),
        use_substitutes: false,
        nix_version: "test".to_owned(),
        prefetch_slots: 0,
    };
    Arc::new(Machine::new(join, sender, false, &[]).expect("create in-memory builder"))
}

async fn queue_one_runnable_step(state: &State, drv: &StorePath) {
    let step = state
        .steps
        .clone_runnable()
        .into_iter()
        .find(|step| step.get_drv_path() == drv)
        .expect("expected derivation to be runnable");
    let step_info = StepInfo::new(step);
    state
        .queues
        .insert_new_jobs(
            "x86_64-linux",
            vec![step_info],
            &jiff::Timestamp::now(),
            StepSortFn::WithRdeps,
            &state.metrics,
        )
        .await;
}

async fn schedule_one_runnable_step(state: &State, drv: &StorePath) {
    queue_one_runnable_step(state, drv).await;
    let machine = fake_machine();
    let process = state
        .queues
        .process(
            async move |_constraint| Ok(RealiseStepResult::Valid(machine.clone())),
            |_| true,
            &state.metrics,
        )
        .await;
    assert_eq!(process, 0, "fake builder accepted the queued step");
}

async fn assert_scheduled_on_builder(state: &State, drv: &StorePath) {
    let scheduled = state.queues.get_scheduled().await;
    let step_info = scheduled
        .iter()
        .find(|item| item.step.get_drv_path() == drv)
        .expect("step remains in the scheduled builder queue");
    assert!(step_info.get_already_scheduled());
    let queue_stats = state.queues.get_stats_per_queue().await;
    assert_eq!(
        queue_stats["x86_64-linux"].active_runnable, 1,
        "the builder assignment still owns an active queue slot"
    );
}

#[tokio::test]
#[ignore = "requires an explicitly guarded private Nix store and TestPg"]
async fn cached_output_recheck_finishes_root_and_releases_dependent() {
    let private_root = private_store_guard();
    let store_dir = init_private_store(&private_root);
    assert!(
        std::env::var_os("LOGNAME").is_some(),
        "test driver must provide LOGNAME for Hydra config"
    );
    let directory = TestDirectory::new("cached-output-recheck");
    let fixtures = create_fixed_output_drvs(directory.path(), "dependency", &store_dir);
    let (pg, pool) = test_utils::TestPg::new().await;
    let state = Box::pin(new_state(directory.path(), &pg.url(), &store_dir))
        .await
        .expect("construct test state");
    let ids = insert_test_builds(
        &pool,
        state.store.store_dir(),
        "dependency",
        &[
            ("root", &fixtures.root_drv),
            ("dependent", &fixtures.dependent_drv),
        ],
    )
    .await;
    assert_eq!(ids.len(), 2);

    assert!(
        !state
            .store
            .is_valid_path(&fixtures.root_output)
            .await
            .expect("store must answer validity")
    );
    assert!(
        !state
            .store
            .is_valid_path(&fixtures.dependent_output)
            .await
            .expect("store must answer validity")
    );
    state
        .get_queued_builds()
        .await
        .expect("load initially missing dependency and dependent");
    assert_eq!(build_db_state(&pool, ids[0]).await.finished, 0);
    assert_eq!(build_db_state(&pool, ids[1]).await.finished, 0);
    let initial_runnable = runnable_paths(&state);
    assert!(initial_runnable.contains(&fixtures.root_drv));
    assert!(!initial_runnable.contains(&fixtures.dependent_drv));

    let root_file = directory.path().join("cached-root-dependency");
    let added_root = add_fixed_output(&root_file, &store_dir);
    assert_eq!(added_root, fixtures.root_output);
    assert!(
        state
            .store
            .is_valid_path(&fixtures.root_output)
            .await
            .expect("store must answer validity")
    );

    state
        .get_queued_builds()
        .await
        .expect("recheck root output and release dependent");
    assert_eq!(
        build_db_state(&pool, ids[0]).await.finished,
        1,
        "cached output must complete the existing root build"
    );
    assert_eq!(
        build_db_state(&pool, ids[0]).await,
        BuildDbState {
            finished: 1,
            status: Some(BuildStatus::Success as i32),
            cached: Some(1),
        }
    );
    assert_eq!(build_db_state(&pool, ids[1]).await.finished, 0);
    assert!(
        !state
            .store
            .is_valid_path(&fixtures.dependent_output)
            .await
            .expect("store must answer validity")
    );
    let released = runnable_paths(&state);
    assert!(released.contains(&fixtures.dependent_drv));
    let eligible = unfinished_runnable_paths(&state);
    assert!(eligible.contains(&fixtures.dependent_drv));
    assert!(!eligible.contains(&fixtures.root_drv));
}

#[tokio::test]
#[ignore = "requires an explicitly guarded private Nix store and TestPg"]
async fn missing_output_remains_pending_on_recheck() {
    let private_root = private_store_guard();
    let store_dir = init_private_store(&private_root);
    assert!(std::env::var_os("LOGNAME").is_some());
    let directory = TestDirectory::new("missing-output-control");
    let fixtures = create_fixed_output_drvs(directory.path(), "missing-control", &store_dir);
    let (pg, pool) = test_utils::TestPg::new().await;
    let state = Box::pin(new_state(directory.path(), &pg.url(), &store_dir))
        .await
        .expect("construct test state");
    let ids = insert_test_builds(
        &pool,
        state.store.store_dir(),
        "missing-control",
        &[("missing", &fixtures.root_drv)],
    )
    .await;
    state
        .get_queued_builds()
        .await
        .expect("load missing-output control");
    state
        .get_queued_builds()
        .await
        .expect("recheck still-missing output");
    assert_eq!(build_db_state(&pool, ids[0]).await.finished, 0);
    assert!(
        !state
            .store
            .is_valid_path(&fixtures.root_output)
            .await
            .expect("store must answer validity")
    );
    assert!(runnable_paths(&state).contains(&fixtures.root_drv));
}

#[tokio::test]
#[ignore = "requires an explicitly guarded private Nix store and TestPg"]
async fn new_step_still_honors_failedpaths_record() {
    let private_root = private_store_guard();
    let store_dir = init_private_store(&private_root);
    assert!(std::env::var_os("LOGNAME").is_some());
    let directory = TestDirectory::new("failedpaths-control");
    let fixtures = create_fixed_output_drvs(directory.path(), "failedpaths-control", &store_dir);
    let (pg, pool) = test_utils::TestPg::new().await;
    let state = Box::pin(new_state(directory.path(), &pg.url(), &store_dir))
        .await
        .expect("construct test state");
    let ids = insert_test_builds(
        &pool,
        state.store.store_dir(),
        "failedpaths-control",
        &[("previously-failed", &fixtures.root_drv)],
    )
    .await;
    insert_previous_failed_build(&pool, state.store.store_dir(), ids[0], &fixtures.root_drv).await;
    let output_path = state
        .store
        .store_dir()
        .display(&fixtures.root_output)
        .to_string();
    sqlx::query("INSERT INTO failedpaths (path) VALUES ($1)")
        .bind(output_path)
        .execute(&pool)
        .await
        .expect("record previous output failure");

    state
        .get_queued_builds()
        .await
        .expect("load failedpaths control");
    assert_eq!(
        build_db_state(&pool, ids[0]).await,
        BuildDbState {
            finished: 1,
            status: Some(BuildStatus::Failed as i32),
            cached: Some(1),
        }
    );
    let row =
        sqlx::query("SELECT status FROM buildsteps WHERE build = $1 ORDER BY stepnr DESC LIMIT 1")
            .bind(ids[0])
            .fetch_one(&pool)
            .await
            .expect("read cached-failure step");
    let step_status: i32 = row.try_get("status").expect("step status column");
    assert_eq!(step_status, BuildStatus::CachedFailure as i32);
}

#[tokio::test]
#[ignore = "requires an explicitly guarded private Nix store and TestPg"]
async fn existing_step_still_honors_failedpaths_record() {
    let private_root = private_store_guard();
    let store_dir = init_private_store(&private_root);
    assert!(std::env::var_os("LOGNAME").is_some());
    let directory = TestDirectory::new("existing-failedpaths-control");
    let fixtures =
        create_fixed_output_drvs(directory.path(), "existing-failedpaths-control", &store_dir);
    let (pg, pool) = test_utils::TestPg::new().await;
    let state = Box::pin(new_state(directory.path(), &pg.url(), &store_dir))
        .await
        .expect("construct test state");
    let ids = insert_test_builds(
        &pool,
        state.store.store_dir(),
        "existing-failedpaths-control",
        &[("previously-failed", &fixtures.root_drv)],
    )
    .await;
    insert_previous_failed_build(&pool, state.store.store_dir(), ids[0], &fixtures.root_drv).await;

    state
        .get_queued_builds()
        .await
        .expect("load initially missing output and create existing step");
    assert_eq!(build_db_state(&pool, ids[0]).await.finished, 0);
    assert!(runnable_paths(&state).contains(&fixtures.root_drv));

    let root_file = directory
        .path()
        .join("cached-root-existing-failedpaths-control");
    let added_root = add_fixed_output(&root_file, &store_dir);
    assert_eq!(added_root, fixtures.root_output);
    let output_path = state
        .store
        .store_dir()
        .display(&fixtures.root_output)
        .to_string();
    sqlx::query("INSERT INTO failedpaths (path) VALUES ($1)")
        .bind(output_path)
        .execute(&pool)
        .await
        .expect("record existing output failure");

    state
        .get_queued_builds()
        .await
        .expect("recheck failedpaths before cached completion");
    assert_eq!(
        build_db_state(&pool, ids[0]).await,
        BuildDbState {
            finished: 1,
            status: Some(BuildStatus::Failed as i32),
            cached: Some(1),
        }
    );
    let row =
        sqlx::query("SELECT status FROM buildsteps WHERE build = $1 ORDER BY stepnr DESC LIMIT 1")
            .bind(ids[0])
            .fetch_one(&pool)
            .await
            .expect("read cached-failure step");
    let step_status: i32 = row.try_get("status").expect("step status column");
    assert_eq!(step_status, BuildStatus::CachedFailure as i32);
}

#[tokio::test]
#[ignore = "requires an explicitly guarded private Nix store and TestPg"]
async fn cached_output_recheck_defers_until_scheduled_item_is_dropped() {
    let private_root = private_store_guard();
    let store_dir = init_private_store(&private_root);
    assert!(std::env::var_os("LOGNAME").is_some());
    let directory = TestDirectory::new("scheduled-output-control");
    let fixtures = create_fixed_output_drvs(directory.path(), "scheduled-control", &store_dir);
    let (pg, pool) = test_utils::TestPg::new().await;
    let state = Box::pin(new_state(directory.path(), &pg.url(), &store_dir))
        .await
        .expect("construct test state");
    let ids = insert_test_builds(
        &pool,
        state.store.store_dir(),
        "scheduled-control",
        &[("scheduled", &fixtures.root_drv)],
    )
    .await;

    state
        .get_queued_builds()
        .await
        .expect("load initially missing output");
    schedule_one_runnable_step(&state, &fixtures.root_drv).await;
    assert_scheduled_on_builder(&state, &fixtures.root_drv).await;
    assert_eq!(build_db_state(&pool, ids[0]).await.finished, 0);

    let root_file = directory.path().join("cached-root-scheduled-control");
    let added_root = add_fixed_output(&root_file, &store_dir);
    assert_eq!(added_root, fixtures.root_output);
    let scheduled_item = state
        .queues
        .remove_job_from_scheduled(&fixtures.root_drv)
        .await
        .expect("take scheduled item while its dispatch claim remains alive");
    state.steps.remove(&fixtures.root_drv);
    state
        .get_queued_builds()
        .await
        .expect("defer cached recheck while scheduled item owns the claim");

    assert_eq!(
        build_db_state(&pool, ids[0]).await.finished,
        0,
        "cached output must not finish a build while its scheduled item owns the claim"
    );
    assert!(state.queues.get_scheduled().await.is_empty());

    drop(scheduled_item);
    state
        .get_queued_builds()
        .await
        .expect("complete cached output after the scheduled claim is released");
    assert_eq!(build_db_state(&pool, ids[0]).await.finished, 1);
}

#[tokio::test]
#[ignore = "requires an explicitly guarded private Nix store and TestPg"]
async fn dispatch_in_progress_defers_cached_output_recheck() {
    let private_root = private_store_guard();
    let store_dir = init_private_store(&private_root);
    assert!(std::env::var_os("LOGNAME").is_some());
    let directory = TestDirectory::new("dispatch-claim-control");
    let fixtures = create_fixed_output_drvs(directory.path(), "dispatch-claim-control", &store_dir);
    let (pg, pool) = test_utils::TestPg::new().await;
    let state = Box::pin(new_state(directory.path(), &pg.url(), &store_dir))
        .await
        .expect("construct test state");
    let ids = insert_test_builds(
        &pool,
        state.store.store_dir(),
        "dispatch-claim-control",
        &[("claimed", &fixtures.root_drv)],
    )
    .await;

    state
        .get_queued_builds()
        .await
        .expect("load initially missing output");
    queue_one_runnable_step(&state, &fixtures.root_drv).await;

    let entered_claim = Arc::new(tokio::sync::Barrier::new(2));
    let release_claim = Arc::new(tokio::sync::Barrier::new(2));
    let machine = fake_machine();
    let processor_entered = entered_claim.clone();
    let processor_release = release_claim.clone();
    let processing = state.queues.process(
        async move |_constraint| {
            processor_entered.wait().await;
            processor_release.wait().await;
            Ok(RealiseStepResult::Valid(machine.clone()))
        },
        |_| true,
        &state.metrics,
    );
    let cached_recheck = async {
        entered_claim.wait().await;
        let root_file = directory.path().join("cached-root-dispatch-claim-control");
        let added_root = add_fixed_output(&root_file, &store_dir);
        assert_eq!(added_root, fixtures.root_output);
        state
            .get_queued_builds()
            .await
            .expect("defer cached recheck while dispatch owns the step claim");
        assert_eq!(
            build_db_state(&pool, ids[0]).await.finished,
            0,
            "an in-progress dispatch claim must keep the build unfinished"
        );
        release_claim.wait().await;
    };
    let (waiting, ()) = tokio::join!(processing, cached_recheck);
    assert_eq!(waiting, 0);
    assert_eq!(build_db_state(&pool, ids[0]).await.finished, 0);
    assert_scheduled_on_builder(&state, &fixtures.root_drv).await;
}
