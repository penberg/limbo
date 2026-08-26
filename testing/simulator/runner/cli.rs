use clap::{
    Arg, Command, Error, Parser, ValueEnum,
    builder::{PossibleValue, TypedValueParser, ValueParserFactory},
    command,
    error::{ContextKind, ContextValue, ErrorKind},
};
use serde::{Deserialize, Serialize};

use crate::profiles::ProfileType;

/// IO backend selection for the simulator
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ValueEnum,
)]
#[serde(rename_all = "snake_case")]
pub enum IoBackend {
    /// Use the default platform IO (SimulatorIO wrapping PlatformIO)
    #[default]
    Default,
    /// Use in-memory IO backend
    Memory,
    /// Use io_uring backend (linux only)
    #[cfg(target_os = "linux")]
    IoUring,
    /// Use win_iocp backend (windows only)
    #[cfg(target_os = "windows")]
    #[value(name = "experimental_win_iocp")]
    WindowsIOCP,
}

#[derive(Parser, Debug, Clone, Serialize, Deserialize, PartialEq, PartialOrd, Eq, Ord)]
#[command(name = "limbo-simulator")]
#[command(author, version, about, long_about = None)]
pub struct SimulatorCLI {
    #[clap(
        short,
        long,
        help = "set seed for reproducible runs",
        conflicts_with = "load"
    )]
    pub seed: Option<u64>,
    #[clap(
        short,
        long,
        help = "enable doublechecking, run the simulator with the plan twice and check output equality",
        conflicts_with = "differential"
    )]
    pub doublecheck: bool,
    #[clap(
        short = 'n',
        long,
        help = "change the maximum size of the randomly generated sequence of interactions",
        default_value_t = normal_or_miri(5000, 50),
        value_parser = clap::value_parser!(u32).range(1..)
    )]
    pub maximum_tests: u32,
    #[clap(
        short = 'k',
        long,
        help = "change the minimum size of the randomly generated sequence of interactions",
        default_value_t = normal_or_miri(1000, 10),
        value_parser = clap::value_parser!(u32).range(1..)
    )]
    pub minimum_tests: u32,
    #[clap(
        short = 't',
        long,
        help = "change the maximum time of the simulation(in seconds)",
        default_value_t = 60 * 60 // default to 1 hour
    )]
    pub maximum_time: usize,
    #[clap(
        short = 'l',
        long,
        help = "load plan from the bug base",
        conflicts_with = "seed"
    )]
    pub load: Option<u64>,
    #[clap(
        short = 'w',
        long,
        help = "enable watch mode that reruns the simulation on file changes"
    )]
    pub watch: bool,
    #[clap(
        long,
        help = "run differential testing between sqlite and Limbo",
        conflicts_with = "doublecheck"
    )]
    pub differential: bool,
    #[clap(
        long,
        help = "enable brute force shrink (warning: it might take a long time)"
    )]
    pub enable_brute_force_shrinking: bool,
    #[clap(subcommand)]
    pub subcommand: Option<SimulatorCommand>,
    #[clap(long, help = "disable BugBase")]
    pub disable_bugbase: bool,
    #[clap(long, help = "disable heuristic shrinking")]
    pub disable_heuristic_shrinking: bool,
    #[clap(long, help = "disable UPDATE Statement")]
    pub disable_update: bool,
    #[clap(long, help = "disable DELETE Statement")]
    pub disable_delete: bool,
    #[clap(long, help = "disable CREATE Statement")]
    pub disable_create: bool,
    #[clap(long, help = "disable CREATE INDEX Statement")]
    pub disable_create_index: bool,
    #[clap(long, help = "disable DROP Statement")]
    pub disable_drop: bool,
    #[clap(
        long,
        help = "disable Insert-Values-Select Property",
        default_value_t = false
    )]
    pub disable_insert_values_select: bool,
    #[clap(
        long,
        help = "disable Double-Create-Failure Property",
        default_value_t = false
    )]
    pub disable_double_create_failure: bool,
    #[clap(long, help = "disable Select-Limit Property")]
    pub disable_select_limit: bool,
    #[clap(long, help = "disable Delete-Select Property")]
    pub disable_delete_select: bool,
    #[clap(long, help = "disable Drop-Select Property")]
    pub disable_drop_select: bool,
    #[clap(
        long,
        help = "disable Select-Select-Optimizer Property",
        default_value_t = false
    )]
    pub disable_select_optimizer: bool,
    #[clap(
        long,
        help = "disable Where-True-False-Null Property",
        default_value_t = false
    )]
    pub disable_where_true_false_null: bool,
    #[clap(
        long,
        help = "disable UNION ALL preserves cardinality Property",
        default_value_t = false
    )]
    pub disable_union_all_preserves_cardinality: bool,
    #[clap(
        long,
        help = "disable Savepoint-Rollback Property",
        default_value_t = false
    )]
    pub disable_savepoint_rollback: bool,
    #[clap(long, help = "disable FsyncNoWait Property", default_value_t = false)]
    pub disable_fsync_no_wait: bool,
    #[clap(long, help = "disable FaultyQuery Property")]
    pub disable_faulty_query: bool,
    #[clap(long, help = "disable Reopen-Database fault")]
    pub disable_reopen_database: bool,
    #[clap(long = "latency-prob", help = "added IO latency probability", value_parser = clap::value_parser!(u8).range(0..=100))]
    pub latency_probability: Option<u8>,
    #[clap(long, help = "Minimum tick time in microseconds for simulated time")]
    pub min_tick: Option<u64>,
    #[clap(long, help = "Maximum tick time in microseconds for simulated time")]
    pub max_tick: Option<u64>,
    #[clap(long, help = "Enable MVCC feature")]
    pub mvcc: Option<bool>,
    #[clap(
        long,
        help = "Keep all database and plan files",
        default_value_t = false
    )]
    pub keep_files: bool,
    #[clap(
        long,
        help = "Disable the SQLite integrity check at the end of a simulation",
        default_value_t = normal_or_miri(false, true)
    )]
    pub disable_integrity_check: bool,
    #[clap(
        long = "io-backend",
        help = "Select the IO backend for the simulator",
        value_enum,
        default_value_t = IoBackend::Default
    )]
    pub io_backend: IoBackend,
    #[clap(long, default_value_t = ProfileType::Default)]
    /// Profile selector for Simulation run
    pub profile: ProfileType,
}

#[derive(Parser, Debug, Clone, Serialize, Deserialize, PartialEq, PartialOrd, Eq, Ord)]
pub enum SimulatorCommand {
    #[clap(about = "run the simulator in a loop")]
    Loop {
        #[clap(
            short = 'n',
            long,
            help = "number of iterations to run the simulator",
            default_value_t = 5
        )]
        n: usize,
        #[clap(
            short = 's',
            long,
            help = "short circuit the simulator, stop on the first failure",
            default_value_t = false
        )]
        short_circuit: bool,
    },
    #[clap(about = "list all the bugs in the base")]
    List,
    #[clap(about = "run the simulator against a specific bug")]
    Test {
        #[clap(
            short = 'b',
            long,
            help = "run the simulator with previous buggy runs for the specific filter"
        )]
        filter: String,
    },
    /// Print profile Json Schema
    PrintSchema,
}

impl SimulatorCLI {
    pub fn validate(&mut self) -> anyhow::Result<()> {
        if self.watch {
            anyhow::bail!("watch mode is disabled for now");
        }
        if self.minimum_tests > self.maximum_tests {
            tracing::warn!(
                "minimum size '{}' is greater than '{}' maximum size, setting both to '{}'",
                self.minimum_tests,
                self.maximum_tests,
                self.maximum_tests
            );
            self.minimum_tests = self.maximum_tests;
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct ProfileTypeParser;

impl TypedValueParser for ProfileTypeParser {
    type Value = ProfileType;

    fn parse_ref(
        &self,
        cmd: &Command,
        arg: Option<&Arg>,
        value: &std::ffi::OsStr,
    ) -> Result<Self::Value, Error> {
        let s = value
            .to_str()
            .ok_or_else(|| Error::new(ErrorKind::InvalidUtf8).with_cmd(cmd))?;

        ProfileType::parse(s).map_err(|_| {
            let mut err = Error::new(ErrorKind::InvalidValue).with_cmd(cmd);
            if let Some(arg) = arg {
                err.insert(
                    ContextKind::InvalidArg,
                    ContextValue::String(arg.to_string()),
                );
            }
            err.insert(
                ContextKind::InvalidValue,
                ContextValue::String(s.to_string()),
            );
            err.insert(
                ContextKind::ValidValue,
                ContextValue::Strings(
                    self.possible_values()
                        .unwrap()
                        .map(|s| s.get_name().to_string())
                        .collect(),
                ),
            );
            err
        })
    }

    fn possible_values(&self) -> Option<Box<dyn Iterator<Item = PossibleValue> + '_>> {
        use strum::VariantNames;
        Some(Box::new(
            Self::Value::VARIANTS
                .iter()
                .map(|variant| {
                    // Custom variant should be listed as a Custom path
                    if variant.eq_ignore_ascii_case("custom") {
                        "CUSTOM_PATH"
                    } else {
                        variant
                    }
                })
                .map(PossibleValue::new),
        ))
    }
}

impl ValueParserFactory for ProfileType {
    type Parser = ProfileTypeParser;

    fn value_parser() -> Self::Parser {
        ProfileTypeParser
    }
}

const fn normal_or_miri<T: Copy>(normal_val: T, miri_val: T) -> T {
    if cfg!(miri) { miri_val } else { normal_val }
}
