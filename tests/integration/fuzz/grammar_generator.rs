/// Grammar generator is a helper to build a probabilistic grammar and generate random string from it
/// Grammar consists of terminal (characters) and symbols (non-terminal with some expansion rule)
///
/// Current, supported expansion rules are:
/// 1. Symbol -> [Str]: generate terminals which form fixed length string with constant prefix and random suffix
/// 2. Symbol -> [Int]: generate terminals which form integer from specified range
/// 3. Symbol -> (Inner)?: generate expansion for Inner symbol with some probability
/// 4. Symbol -> (Inner){n..m}: generate k expansions for Inner symbol where k \in [n..m) with uniform distribution
/// (note, that every repetition will be expanded independently)
/// 5. Symbol -> Inner1 Inner2 .. Inner[n]: concatenate expansions from inner symbols and insert separator string between them
/// 6. Symbol -> Choice1 | Choice2 | .. | Choice[n]: pick random choice according to their weights randomly and generate expansion for it
///
/// (this is more or less [context-free grammar](https://en.wikipedia.org/wiki/Context-free_grammar) with very minor differences)
///
/// The idea behind this code is to provide a way to "build" grammar generator with all these rules and their dependencies and after that
/// we can randomly sample strings from this generator easily.
use std::{cell::RefCell, collections::HashMap, ops::Range, rc::Rc};

use rand::Rng;
use rand_chacha::ChaCha8Rng;

#[derive(Clone, Debug)]
pub enum SymbolType {
    Str {
        fixed_prefix: String,
        random_length: usize,
    },
    Int {
        range: Range<i32>,
    },
    #[allow(dead_code)]
    Optional {
        value: SymbolHandle,
        prob: f64,
    },
    #[allow(dead_code)]
    Repeat {
        value: SymbolHandle,
        range: Range<usize>,
        separator: String,
    },
    Concat {
        values: Vec<SymbolHandle>,
        separator: String,
    },
    Choice {
        values: Vec<(SymbolHandle, f64)>,
    },
}

pub fn const_str(s: &str) -> SymbolType {
    SymbolType::Str {
        fixed_prefix: s.to_string(),
        random_length: 0,
    }
}

#[allow(dead_code)]
pub fn rand_str(fixed_prefix: &str, random_length: usize) -> SymbolType {
    SymbolType::Str {
        fixed_prefix: fixed_prefix.to_string(),
        random_length,
    }
}

pub fn rand_int(range: Range<i32>) -> SymbolType {
    SymbolType::Int { range }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct SymbolHandle(i32);
pub struct SymbolDefinitionBuilder {
    generator: GrammarGenerator,
    handle: SymbolHandle,
    symbol: Option<SymbolType>,
}

#[derive(Debug)]
enum GrammarFrontierNode {
    Handle(SymbolHandle),
    String(String),
}

#[derive(Clone)]
pub struct GrammarGenerator(Rc<RefCell<GrammarGeneratorInner>>);

struct GrammarGeneratorInner {
    last_symbol_id: i32,
    symbols: HashMap<SymbolHandle, SymbolType>,
}

impl GrammarGenerator {
    pub fn new() -> Self {
        GrammarGenerator(Rc::new(RefCell::new(GrammarGeneratorInner {
            last_symbol_id: 0,
            symbols: HashMap::new(),
        })))
    }
    pub fn create_handle(&self) -> (SymbolHandle, SymbolDefinitionBuilder) {
        let handle = SymbolHandle(self.0.borrow().last_symbol_id);
        self.0.borrow_mut().last_symbol_id += 1;

        let builder = SymbolDefinitionBuilder {
            generator: self.clone(),
            handle,
            symbol: None,
        };
        (handle, builder)
    }
    pub fn create(&self) -> SymbolDefinitionBuilder {
        let (_, builder) = self.create_handle();
        builder
    }
    pub fn register(&self, handle: SymbolHandle, value: SymbolType) {
        let result = self.0.borrow_mut().symbols.insert(handle, value);
        assert!(result.is_none(), "handle can be registered only once");
    }

    // this helper runs DFS for directed graph and set is_recursive[v] = true for all reachable from root vertices
    // if path of infinite lengths exists for v
    fn is_recursive_from_root(
        &self,
        root: SymbolHandle,
        is_recursive: &mut HashMap<SymbolHandle, bool>,
    ) -> bool {
        if let Some(_) = is_recursive.get(&root) {
            is_recursive.insert(root, true);
            return true;
        }
        is_recursive.insert(root, false);
        let symbols = &self.0.borrow().symbols;
        let recursive = match symbols.get(&root).expect("symbol must be registered") {
            SymbolType::Str { .. } | SymbolType::Int { .. } => false,
            SymbolType::Optional { value, .. } | SymbolType::Repeat { value, .. } => {
                self.is_recursive_from_root(*value, is_recursive)
            }
            SymbolType::Concat { values, .. } => {
                let mut recursive = false;
                for value in values.iter() {
                    recursive |= self.is_recursive_from_root(*value, is_recursive);
                }
                recursive
            }
            SymbolType::Choice { values, .. } => {
                let mut recursive = false;
                for (value, _) in values.iter() {
                    recursive |= self.is_recursive_from_root(*value, is_recursive);
                }
                recursive
            }
        };
        is_recursive.insert(root, recursive);
        recursive
    }

    // we generate random sample from grammar in BFS fashion instead of DFS because in such a way we can force abort generation of string in more fair fashion
    // the problem with probabilistic grammar, is that it's recursive rules can have infinite (or very large) average length of expanded terminals
    // in order to fight with this problem, we provide length_limit_hint which will change logic of generation and start using only non-recursive rules (if this is possible) in case
    // when "frontier" of the generation already have >= length_limit_hint nodes
    pub fn generate(
        &self,
        rng: &mut ChaCha8Rng,
        root: SymbolHandle,
        length_limit_hint: usize,
    ) -> String {
        let mut frontier = vec![GrammarFrontierNode::Handle(root)];

        let mut is_recursive = HashMap::new();
        self.is_recursive_from_root(root, &mut is_recursive);

        let symbols = &self.0.borrow().symbols;
        let terminals = loop {
            let mut next = Vec::new();
            let mut expanded = false;
            let limit_exceeded = frontier.len() >= length_limit_hint;
            for node in frontier.into_iter() {
                let GrammarFrontierNode::Handle(handle) = node else {
                    next.push(node);
                    continue;
                };

                expanded = true;
                match symbols.get(&handle).expect("symbol must be registered") {
                    SymbolType::Str {
                        fixed_prefix,
                        random_length,
                    } => {
                        let mut s = fixed_prefix.clone();
                        for _ in 0..*random_length {
                            s.push(rng.random_range('A'..='Z'));
                        }
                        next.push(GrammarFrontierNode::String(s));
                    }
                    SymbolType::Int { range } => {
                        next.push(GrammarFrontierNode::String(
                            rng.random_range(range.clone()).to_string(),
                        ));
                    }
                    SymbolType::Optional { value, prob } => {
                        if !limit_exceeded && rng.random_bool(*prob) {
                            next.push(GrammarFrontierNode::Handle(*value));
                        }
                    }
                    SymbolType::Repeat {
                        value,
                        range,
                        separator,
                    } => {
                        let repetitions = if !limit_exceeded {
                            rng.random_range(range.clone())
                        } else {
                            range.start
                        };
                        for i in 0..repetitions {
                            if i > 0 {
                                next.push(GrammarFrontierNode::String(separator.to_string()));
                            }
                            next.push(GrammarFrontierNode::Handle(*value));
                        }
                    }
                    SymbolType::Concat { values, separator } => {
                        for (i, value) in values.iter().enumerate() {
                            if i > 0 {
                                next.push(GrammarFrontierNode::String(separator.to_string()));
                            }
                            next.push(GrammarFrontierNode::Handle(*value));
                        }
                    }
                    SymbolType::Choice { values } => {
                        let mut handles = if !limit_exceeded {
                            values.clone()
                        } else {
                            values
                                .iter()
                                .filter(|x| is_recursive.get(&x.0) != Some(&true))
                                .map(|x| *x)
                                .collect::<Vec<_>>()
                        };
                        if handles.len() == 0 {
                            handles = values.clone();
                        }

                        let sum: f64 = handles.iter().map(|x| x.1).sum();
                        let mut sample = rng.random_range(0.0..sum);
                        for (i, (handle, weight)) in handles.iter().enumerate() {
                            sample -= weight;
                            if sample > 0.0 && i < handles.len() - 1 {
                                continue;
                            }
                            next.push(GrammarFrontierNode::Handle(*handle));
                            break;
                        }
                    }
                }
            }
            if !expanded {
                break next;
            }
            frontier = next;
        };
        let mut result = String::new();
        for node in terminals {
            let GrammarFrontierNode::String(string) = node else {
                panic!("frontier in the end must contain only string nodes");
            };
            result.push_str(&string);
        }
        result
    }
}

impl SymbolDefinitionBuilder {
    pub fn use_symbol(self, symbol: SymbolType) -> Self {
        assert!(self.symbol.is_none(), "symbol must be unset");
        Self {
            symbol: Some(symbol),
            ..self
        }
    }
    pub fn concat(self, separator: &str) -> Self {
        assert!(self.symbol.is_none(), "symbol must be unset");
        Self {
            symbol: Some(SymbolType::Concat {
                values: vec![],
                separator: separator.to_string(),
            }),
            ..self
        }
    }
    pub fn push(mut self, handle: SymbolHandle) -> Self {
        let Some(SymbolType::Concat {
            mut values,
            separator,
        }) = self.symbol.take()
        else {
            panic!("symbol must be set to Concat type");
        };
        values.push(handle);
        Self {
            symbol: Some(SymbolType::Concat { values, separator }),
            ..self
        }
    }
    pub fn push_symbol(self, symbol: SymbolType) -> Self {
        let (handle, builder) = self.generator.create_handle();
        builder.use_symbol(symbol).build();
        self.push(handle)
    }
    pub fn push_str(self, s: &str) -> Self {
        self.push_symbol(const_str(s))
    }
    pub fn choice(self) -> Self {
        assert!(self.symbol.is_none(), "symbol must be unset");
        Self {
            symbol: Some(SymbolType::Choice { values: vec![] }),
            ..self
        }
    }
    pub fn option_w(mut self, handle: SymbolHandle, weight: f64) -> Self {
        let Some(SymbolType::Choice { mut values }) = self.symbol.take() else {
            panic!("symbol must be set to Choice type");
        };
        values.push((handle, weight));
        Self {
            symbol: Some(SymbolType::Choice { values }),
            ..self
        }
    }
    #[allow(dead_code)]
    pub fn option(self, handle: SymbolHandle) -> Self {
        self.option_w(handle, 1.0)
    }
    pub fn option_symbol_w(self, symbol: SymbolType, weight: f64) -> Self {
        let (handle, builder) = self.generator.create_handle();
        builder.use_symbol(symbol).build();
        self.option_w(handle, weight)
    }
    pub fn option_symbol(self, symbol: SymbolType) -> Self {
        self.option_symbol_w(symbol, 1.0)
    }
    pub fn option_str(self, s: &str) -> Self {
        self.option_symbol(const_str(s))
    }
    #[allow(dead_code)]
    pub fn options_symbol<const N: usize>(mut self, symbols: [SymbolType; N]) -> Self {
        for symbol in symbols {
            self = self.option_symbol(symbol)
        }
        self
    }
    pub fn options_str<const N: usize>(mut self, strs: [&str; N]) -> Self {
        for s in strs {
            self = self.option_str(s)
        }
        self
    }
    #[allow(dead_code)]
    pub fn repeat(self, range: Range<usize>, separator: &str) -> Self {
        let symbol = self.symbol.expect("symbol must be set");
        let (handle, builder) = self.generator.create_handle();
        builder.use_symbol(symbol).build();
        Self {
            symbol: Some(SymbolType::Repeat {
                value: handle,
                range,
                separator: separator.to_string(),
            }),
            ..self
        }
    }
    #[allow(dead_code)]
    pub fn optional(self, prob: f64) -> Self {
        let symbol = self.symbol.expect("symbol must be set");
        let (handle, builder) = self.generator.create_handle();
        builder.use_symbol(symbol).build();
        Self {
            symbol: Some(SymbolType::Optional {
                value: handle,
                prob,
            }),
            ..self
        }
    }

    pub fn build(self) -> SymbolHandle {
        let symbol = self.symbol.expect("symbol must be set");
        self.generator.register(self.handle, symbol);
        self.handle
    }
}
