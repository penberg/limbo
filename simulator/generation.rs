use anarchist_readable_name_generator_lib::readable_name_custom;
use rand::Rng;

pub mod plan;
pub mod query;
pub mod table;

pub trait Arbitrary {
    fn arbitrary<R: Rng>(rng: &mut R) -> Self;
}

pub trait ArbitraryFrom<T> {
    fn arbitrary_from<R: Rng>(rng: &mut R, t: &T) -> Self;
}

pub(crate) fn frequency<'a, T, R: rand::Rng>(
    choices: Vec<(usize, Box<dyn FnOnce(&mut R) -> T + 'a>)>,
    rng: &mut R,
) -> T {
    let total = choices.iter().map(|(weight, _)| weight).sum::<usize>();
    let mut choice = rng.gen_range(0..total);

    for (weight, f) in choices {
        if choice < weight {
            return f(rng);
        }
        choice -= weight;
    }

    unreachable!()
}

pub(crate) fn one_of<'a, T, R: rand::Rng>(
    choices: Vec<Box<dyn Fn(&mut R) -> T + 'a>>,
    rng: &mut R,
) -> T {
    let index = rng.gen_range(0..choices.len());
    choices[index](rng)
}

pub(crate) fn pick<'a, T, R: rand::Rng>(choices: &'a Vec<T>, rng: &mut R) -> &'a T {
    let index = rng.gen_range(0..choices.len());
    &choices[index]
}

pub(crate) fn pick_index<R: rand::Rng>(choices: usize, rng: &mut R) -> usize {
    rng.gen_range(0..choices)
}

fn gen_random_text<T: Rng>(rng: &mut T) -> String {
    let big_text = rng.gen_ratio(1, 1000);
    if big_text {
        // let max_size: u64 = 2 * 1024 * 1024 * 1024;
        let max_size: u64 = 2 * 1024; // todo: change this back to 2 * 1024 * 1024 * 1024
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
