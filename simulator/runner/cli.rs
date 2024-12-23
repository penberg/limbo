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
        short,
        long,
        help = "change the maximum size of the randomly generated sequence of interactions",
        default_value_t = 1024
    )]
    pub maximum_size: usize,
}
