use clap::{command, Parser};

#[derive(Parser)]
#[command(name = "limbo-simulator")]
#[command(author, version, about, long_about = None)]
pub struct SimulatorCLI {
    #[clap(short, long, help = "set seed for reproducible runs", default_value = None)]
    pub seed: Option<u64>,
    #[clap(short, long, help = "set custom output directory for produced files", default_value = None)]
    pub output_dir: Option<String>,
    #[clap(
        short,
        long,
        help = "enable doublechecking, run the simulator with the plan twice and check output equality"
    )]
    pub doublecheck: bool,
    #[clap(
        short = 'n',
        long,
        help = "change the maximum size of the randomly generated sequence of interactions",
        default_value_t = 1024
    )]
    pub maximum_size: usize,
    #[clap(
        short = 'k',
        long,
        help = "change the minimum size of the randomly generated sequence of interactions",
        default_value_t = 1
    )]
    pub minimum_size: usize,
    #[clap(
        short = 't',
        long,
        help = "change the maximum time of the simulation(in seconds)",
        default_value_t = 60 * 60 // default to 1 hour
    )]
    pub maximum_time: usize,
    #[clap(
        short = 'm',
        long,
        help = "minimize(shrink) the failing counterexample"
    )]
    pub shrink: bool,
}

impl SimulatorCLI {
    pub fn validate(&self) -> Result<(), String> {
        if self.minimum_size < 1 {
            return Err("minimum size must be at least 1".to_string());
        }
        if self.maximum_size < 1 {
            return Err("maximum size must be at least 1".to_string());
        }
        if self.minimum_size > self.maximum_size {
            return Err("Minimum size cannot be greater than maximum size".to_string());
        }
        Ok(())
    }
}
