//! Code adapted from Chrono StrftimeItems but for sqlite strftime compatibility
//! Sqlite reference https://www.sqlite.org/lang_datefunc.html

use chrono::format::{Fixed, Item, Numeric, Pad};

const fn num(numeric: Numeric) -> Item<'static> {
    Item::Numeric(numeric, Pad::None)
}

const fn num0(numeric: Numeric) -> Item<'static> {
    Item::Numeric(numeric, Pad::Zero)
}

const fn nums(numeric: Numeric) -> Item<'static> {
    Item::Numeric(numeric, Pad::Space)
}

const fn fixed(fixed: Fixed) -> Item<'static> {
    Item::Fixed(fixed)
}

#[derive(Clone, Debug)]
pub struct CustomStrftimeItems<'a> {
    // Remaining portion of the string.
    remainder: &'a str,
    /// If the current specifier is composed of multiple formatting items (e.g. `%+`),
    /// `queue` stores a slice of `Item`s that have to be returned one by one.
    queue: &'static [Item<'static>],
}

impl<'a> CustomStrftimeItems<'a> {
    pub const fn new(s: &'a str) -> CustomStrftimeItems<'a> {
        CustomStrftimeItems {
            remainder: s,
            queue: &[],
        }
    }
}

// const HAVE_ALTERNATES: &str = "z";

impl<'a> Iterator for CustomStrftimeItems<'a> {
    type Item = Item<'a>;

    fn next(&mut self) -> Option<Item<'a>> {
        // We have items queued to return from a specifier composed of multiple formatting items.
        if let Some((item, remainder)) = self.queue.split_first() {
            self.queue = remainder;
            return Some(item.clone());
        }

        // Normal: we are parsing the formatting string.
        let (remainder, item) = self.parse_next_item(self.remainder)?;
        self.remainder = remainder;
        Some(item)
    }
}

impl<'a> CustomStrftimeItems<'a> {
    fn parse_next_item(&mut self, mut remainder: &'a str) -> Option<(&'a str, Item<'a>)> {
        // use InternalInternal::*;
        use Item::{Literal, Space};
        use Numeric::*;

        match remainder.chars().next() {
            // we are done
            None => None,

            // the next item is a specifier
            Some('%') => {
                remainder = &remainder[1..];

                macro_rules! next {
                    () => {
                        match remainder.chars().next() {
                            Some(x) => {
                                remainder = &remainder[x.len_utf8()..];
                                x
                            }
                            None => return Some((remainder, Item::Error)), // premature end of string
                        }
                    };
                }

                let spec = next!();
                let pad_override = match spec {
                    '-' => Some(Pad::None),
                    '0' => Some(Pad::Zero),
                    '_' => Some(Pad::Space),
                    _ => None,
                };

                // let is_alternate = spec == '#';
                // let spec = if pad_override.is_some() || is_alternate { next!() } else { spec };
                // if is_alternate && !HAVE_ALTERNATES.contains(spec) {
                //     return Some((remainder, Item::Error));
                // }

                macro_rules! queue {
                    [$head:expr, $($tail:expr),+ $(,)*] => ({
                        const QUEUE: &'static [Item<'static>] = &[$($tail),+];
                        self.queue = QUEUE;
                        $head
                    })
                }

                // macro_rules! queue_from_slice {
                //     ($slice:expr) => {{
                //         self.queue = &$slice[1..];
                //         $slice[0].clone()
                //     }};
                // }

                let item = match spec {
                    // day of month: 01-31
                    'd' => num0(Day),
                    // day of month without leading zero: 1-31
                    'e' => nums(Day),
                    // fractional seconds: SS.SSS
                    'f' => {
                        queue![num0(Second), fixed(Fixed::Nanosecond3)]
                    }
                    // ISO 8601 date: YYYY-MM-DD
                    'F' => queue![
                        num0(Year),
                        Literal("-"),
                        num0(Month),
                        Literal("-"),
                        num0(Day)
                    ],
                    // ISO 8601 year corresponding to %V
                    'G' => num0(IsoYear),
                    // 2-digit ISO 8601 year corresponding to %V
                    'g' => num0(IsoYearMod100),
                    // hour: 00-24
                    'H' => num0(Hour),
                    // hour for 12-hour clock: 01-12
                    'I' => num0(Hour12),
                    // day of year: 001-366
                    'j' => num0(Ordinal),
                    // hour without leading zero: 0-24
                    'k' => nums(Hour),
                    // %I without leading zero: 1-12
                    'l' => nums(Hour12),
                    // month: 01-12
                    'm' => num0(Month),
                    // minute: 00-59
                    'M' => num0(Minute),
                    // "AM" or "PM" depending on the hour
                    'p' => fixed(Fixed::UpperAmPm),
                    // "am" or "pm" depending on the hour
                    'P' => fixed(Fixed::LowerAmPm),
                    // ISO 8601 time: HH:MM
                    'R' => queue![num0(Hour), Literal(":"), num0(Minute)],
                    // seconds since 1970-01-01
                    's' => num(Timestamp),
                    // seconds: 00-59
                    'S' => num0(Second),
                    // ISO 8601 time: HH:MM:SS
                    'T' => {
                        queue![
                            num0(Hour),
                            Literal(":"),
                            num0(Minute),
                            Literal(":"),
                            num0(Second)
                        ]
                    }
                    // week of year (00-53) - week 01 starts on the first Sunday
                    'U' => num0(WeekFromSun),
                    // day of week 1-7 with Monday==1
                    'u' => num(WeekdayFromMon),
                    // ISO 8601 week of year
                    'V' => num0(IsoWeek),
                    // day of week 0-6 with Sunday==0
                    'w' => num(NumDaysFromSun),
                    // week of year (00-53) - week 01 starts on the first Monday
                    'W' => num0(WeekFromMon),
                    // year: 0000-9999
                    'Y' => num0(Year),
                    // %
                    '%' => Literal("%"),
                    // TODO instead of doing a preprocessing of the %J specifier, it could be done post as postprocessing
                    // step by just emitting the formatter again to the string
                    // 'J' => Literal("%J"),
                    _ => Item::Error, // no such specifier
                };

                // Adjust `item` if we have any padding modifier.
                // Not allowed on non-numeric items or on specifiers composed out of multiple
                // formatting items.
                if let Some(new_pad) = pad_override {
                    match item {
                        Item::Numeric(ref kind, _pad) if self.queue.is_empty() => {
                            Some((remainder, Item::Numeric(kind.clone(), new_pad)))
                        }
                        _ => Some((remainder, Item::Error)),
                    }
                } else {
                    Some((remainder, item))
                }
            }

            // the next item is space
            Some(c) if c.is_whitespace() => {
                // `%` is not a whitespace, so `c != '%'` is redundant
                let nextspec = remainder
                    .find(|c: char| !c.is_whitespace())
                    .unwrap_or(remainder.len());
                assert!(nextspec > 0);
                let item = Space(&remainder[..nextspec]);
                remainder = &remainder[nextspec..];
                Some((remainder, item))
            }

            // the next item is literal
            _ => {
                let nextspec = remainder
                    .find(|c: char| c.is_whitespace() || c == '%')
                    .unwrap_or(remainder.len());
                assert!(nextspec > 0);
                let item = Literal(&remainder[..nextspec]);
                remainder = &remainder[nextspec..];
                Some((remainder, item))
            }
        }
    }
}
