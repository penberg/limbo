use std::{iter::Sum, ops::SubAssign};

use anarchist_readable_name_generator_lib::readable_name_custom;
use rand::{distributions::uniform::SampleUniform, Rng};

pub mod plan;
pub mod property;
pub mod query;
pub mod table;

/// Arbitrary trait for generating random values
/// An implementation of arbitrary is assumed to be a uniform sampling of
/// the possible values of the type, with a bias towards smaller values for
/// practicality.
pub trait Arbitrary {
    fn arbitrary<R: Rng>(rng: &mut R) -> Self;
}

/// ArbitraryFrom trait for generating random values from a given value
/// ArbitraryFrom allows for constructing relations, where the generated
/// value is dependent on the given value. These relations could be constraints
/// such as generating an integer within an interval, or a value that fits in a table,
/// or a predicate satisfying a given table row.
pub trait ArbitraryFrom<T> {
    fn arbitrary_from<R: Rng>(rng: &mut R, t: T) -> Self;
}

/// Frequency is a helper function for composing different generators with different frequency
/// of occurences.
/// The type signature for the `N` parameter is a bit complex, but it
/// roughly corresponds to a type that can be summed, compared, subtracted and sampled, which are
/// the operations we require for the implementation.
// todo: switch to a simpler type signature that can accomodate all integer and float types, which
//       should be enough for our purposes.
pub(crate) fn frequency<
    'a,
    T,
    R: Rng,
    N: Sum + PartialOrd + Copy + Default + SampleUniform + SubAssign,
>(
    choices: Vec<(N, Box<dyn Fn(&mut R) -> T + 'a>)>,
    rng: &mut R,
) -> T {
    let total = choices.iter().map(|(weight, _)| *weight).sum::<N>();
    let mut choice = rng.gen_range(N::default()..total);

    for (weight, f) in choices {
        if choice < weight {
            return f(rng);
        }
        choice -= weight;
    }

    unreachable!()
}

/// one_of is a helper function for composing different generators with equal probability of occurence.
pub(crate) fn one_of<'a, T, R: Rng>(choices: Vec<Box<dyn Fn(&mut R) -> T + 'a>>, rng: &mut R) -> T {
    let index = rng.gen_range(0..choices.len());
    choices[index](rng)
}

/// pick is a helper function for uniformly picking a random element from a slice
pub(crate) fn pick<'a, T, R: Rng>(choices: &'a [T], rng: &mut R) -> &'a T {
    let index = rng.gen_range(0..choices.len());
    &choices[index]
}

/// pick_index is typically used for picking an index from a slice to later refer to the element
/// at that index.
pub(crate) fn pick_index<R: Rng>(choices: usize, rng: &mut R) -> usize {
    rng.gen_range(0..choices)
}

/// gen_random_text uses `anarchist_readable_name_generator_lib` to generate random
/// readable names for tables, columns, text values etc.
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
