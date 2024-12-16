use anarchist_readable_name_generator_lib::readable_name_custom;
use rand::Rng;

pub mod query;
pub mod table;

pub trait Arbitrary {
    fn arbitrary<R: Rng>(rng: &mut R) -> Self;
}

pub trait ArbitraryFrom<T> {
    fn arbitrary_from<R: Rng>(rng: &mut R, t: &T) -> Self;
}

fn gen_random_text<T: Rng>(rng: &mut T) -> String {
    let big_text = rng.gen_ratio(1, 1000);
    if big_text {
        let max_size: u64 = 2 * 1024 * 1024 * 1024;
        let size = rng.gen_range(1024..max_size);
        let mut name = String::new();
        for i in 0..size {
            name.push(((i % 26) as u8 + b'A') as char);
        }
        name
    } else {
        let name = readable_name_custom("_", rng);
        name.replace("-", "_")
    }
}
