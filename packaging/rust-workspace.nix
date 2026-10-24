# One dependency build (cargoArtifacts) shared by every workspace crate, so
# CI only recompiles hydra's own crates.
{
  lib,
  runCommand,
  version,
  craneLib,
  protobuf,
  pkg-config,
  rust-jemalloc-sys,
  postgresql_17,
  # Provides nix-store, nix-instantiate and nix-daemon for the
  # cached-outputs regression check below.
  nixCli,
  # Features are a property of the workspace, not of an individual crate: cargo
  # resolves them once for the whole `--workspace` build, so a per-crate knob
  # would just fork the build. Override this on the scope to move every crate
  # at once.
  withOtel ? false,
}:
let
  src = lib.fileset.toSource {
    root = ../.;
    fileset = lib.fileset.unions [
      ../Cargo.toml
      ../Cargo.lock
      ../clippy.toml
      ../.cargo
      ../.sqlx
      ../subprojects/hydra-queue-runner/Cargo.toml
      ../subprojects/hydra-queue-runner/src
      ../subprojects/hydra-queue-runner/examples
      ../subprojects/hydra-builder/Cargo.toml
      ../subprojects/hydra-builder/src
      ../subprojects/hydra-evaluator/Cargo.toml
      ../subprojects/hydra-evaluator/src
      ../subprojects/hydra-ws/Cargo.toml
      ../subprojects/hydra-ws/src
      ../subprojects/hydra-ad-hoc/Cargo.toml
      ../subprojects/hydra-ad-hoc/src
      ../subprojects/crates
      # For unit tests which want to spin up a fresh database
      ../subprojects/hydra/sql/hydra.sql
      ../subprojects/proto
    ];
  };

  commonArgs = {
    inherit src;
    pname = "hydra-rust-workspace";
    version = "0.0.0";
    strictDeps = true;

    nativeBuildInputs = [
      pkg-config
      protobuf
    ];

    buildInputs = [
      rust-jemalloc-sys
    ];
  };

  cargoArtifacts = craneLib.buildDepsOnly commonArgs;

  # A virtual manifest rejects a bare `--features`, so name the members. Every
  # member that has the feature gets it: enabling it for only some of them
  # would fork the workspace build.
  features = lib.optionals withOtel [
    "hydra-builder/otel"
    "hydra-queue-runner/otel"
    "hydra-ws/otel"
    "hydra-ad-hoc/otel"
  ];

  # `cargoArtifacts` is a whole-workspace `buildDepsOnly`, so its dependencies
  # are compiled with the features unified across every member. Building a
  # single member with `--package` resolves a *narrower* feature set
  # (`resolver = "2"` does not unify across members), so the fingerprints do not
  # match and cargo recompiles the entire dependency tree. Build `--workspace`
  # here instead, matching the deps build exactly, and let each crate pick its
  # binary out of the result.
  workspace = craneLib.buildPackage (
    commonArgs
    // {
      inherit version cargoArtifacts;
      cargoExtraArgs = lib.concatStringsSep " " (
        [ "--workspace" ]
        ++ lib.optional (features != [ ]) "--features ${lib.concatStringsSep "," features}"
      );
      # Tests run in the separate `tests` check below.
      doCheck = false;
    }
  );

  # Lints with the default features only: enabling more would not match
  # `cargoArtifacts` and recompile every dependency.
  clippy = craneLib.cargoClippy (
    commonArgs
    // {
      inherit cargoArtifacts;
      cargoClippyExtraArgs = "--workspace --all-targets -- -D warnings";
    }
  );

  # Runs the workspace's unit tests. DB tests spin up their own Postgres via
  # `test-utils`, so it only has to be on PATH.
  tests = craneLib.cargoNextest (
    commonArgs
    // {
      inherit cargoArtifacts;
      nativeBuildInputs = commonArgs.nativeBuildInputs ++ [ postgresql_17 ];
      cargoNextestExtraArgs = "--workspace";
    }
  );

  # Regression test for cached-output queue rechecks (see
  # `hydra-queue-runner/src/state/cached_output_tests.rs`). Those tests are
  # `#[ignore]`d: each needs a private Nix store served by a private daemon
  # plus a throwaway Postgres, so the workspace-wide nextest check above
  # skips them. This check provides that fixture and runs them.
  cached-outputs = craneLib.cargoNextest (
    commonArgs
    // {
      inherit cargoArtifacts;
      nativeBuildInputs = commonArgs.nativeBuildInputs ++ [
        postgresql_17
        nixCli
      ];
      # The binary also includes `state`, duplicating these library tests.
      # Running both copies concurrently shares fixed-output paths in the
      # private store, so one copy can import the other copy's missing output.
      cargoNextestExtraArgs = "--workspace --lib --run-ignored ignored-only cached_output failedpaths_record missing_output_remains_pending_on_recheck";
      preCheck = ''
        export LOGNAME="$(id -un)"
        export HYDRA_QUEUE_TEST_STORE="$TMPDIR/queue-test-store"
        export NIX_STORE_DIR="$HYDRA_QUEUE_TEST_STORE/store"
        export NIX_STATE_DIR="$HYDRA_QUEUE_TEST_STORE/state"
        export NIX_LOG_DIR="$HYDRA_QUEUE_TEST_STORE/log"
        export NIX_CONF_DIR="$HYDRA_QUEUE_TEST_STORE/conf"
        export NIX_USER_CONF_FILES=/dev/null
        unset NIX_CONFIG HYDRA_DATABASE_URL
        mkdir -p "$NIX_STORE_DIR" "$NIX_STATE_DIR" "$NIX_LOG_DIR" "$NIX_CONF_DIR"
        export NIX_REMOTE="local?store=$NIX_STORE_DIR&state=$NIX_STATE_DIR&log=$NIX_LOG_DIR"
        printf '%s\n' 'build-users-group =' 'substituters =' \
          'experimental-features = nix-command' > "$NIX_CONF_DIR/nix.conf"

        # Finish the schema initialization before concurrent local clients
        # and the private daemon open the same SQLite database.
        nix-store --init

        # The queue runner talks to the store through the daemon, which
        # listens at $NIX_STATE_DIR/daemon-socket/socket by default.
        nix-daemon &
        hydra_queue_daemon_pid=$!
        trap 'kill "$hydra_queue_daemon_pid"' EXIT
        for _ in $(seq 1 100); do
          if [ -S "$NIX_STATE_DIR/daemon-socket/socket" ]; then break; fi
          sleep 0.1
        done
        [ -S "$NIX_STATE_DIR/daemon-socket/socket" ]
      '';
    }
  );

  # Lints feature-gated code (e.g. `otel`) too, at the cost of its own
  # dependency build, since the features change the dependency graph.
  allFeaturesArgs = commonArgs // {
    cargoExtraArgs = "--locked --workspace --all-features";
  };
  clippyAll = craneLib.cargoClippy (
    allFeaturesArgs
    // {
      cargoArtifacts = craneLib.buildDepsOnly allFeaturesArgs;
      cargoClippyExtraArgs = "--all-targets -- -D warnings";
    }
  );
  # Each crate is just its binary lifted out of the shared build, so there is
  # nothing crate-specific left to keep in a `package.nix` of its own.
  mkCrate =
    {
      pname,
      meta,
    }:
    runCommand "${pname}-${version}"
      {
        meta = {
          mainProgram = pname;
        }
        // meta;
      }
      ''
        mkdir -p "$out/bin"
        cp "${workspace}/bin/${pname}" "$out/bin/${pname}"
      '';
in
{
  inherit
    cargoArtifacts
    workspace
    clippy
    clippyAll
    tests
    cached-outputs
    ;

  hydra-builder = mkCrate {
    pname = "hydra-builder";
    meta.description = "Hydra builder (Rust)";
  };

  hydra-queue-runner = mkCrate {
    pname = "hydra-queue-runner";
    meta.description = "Hydra queue runner (Rust)";
  };

  hydra-evaluator = mkCrate {
    pname = "hydra-evaluator";
    meta.description = "Hydra evaluator (Rust)";
  };

  hydra-ws = mkCrate {
    pname = "hydra-ws";
    meta.description = "Hydra ws server (Rust)";
  };

  hydra-ad-hoc = mkCrate {
    pname = "hydra-ad-hoc";
    meta.description = "hydra-ad-hoc (experimental, optional): Hydra Builds for ad hoc jobs and ad hoc store usage, over the nix daemon protocol";
  };
}
