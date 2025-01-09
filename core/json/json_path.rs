use pest::Parser as P;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "json/json.pest"]
#[grammar = "json/json_path.pest"]
struct Parser;

/// Describes a JSON path, which is a sequence of keys and/or array locators.
#[derive(Clone, Debug)]
pub struct JsonPath {
    pub elements: Vec<PathElement>,
}

/// PathElement describes a single element of a JSON path.
#[derive(Clone, Debug, PartialEq)]
pub enum PathElement {
    /// Root element: '$'
    Root(),
    /// JSON key
    Key(String),
    /// Array locator, eg. [2], [#-5]
    ArrayLocator(i32),
}

pub fn json_path(path: &str, strict: bool) -> crate::Result<JsonPath> {
    if strict {
        strict_json_path(path)
    } else {
        relaxed_json_path(path)
    }
}

/// Parses path into a Vec of Strings, where each string is a key or an array locator.
fn strict_json_path(path: &str) -> crate::Result<JsonPath> {
    let parsed = Parser::parse(Rule::path, path);

    if let Ok(mut parsed) = parsed {
        let mut result = vec![];
        let parsed = parsed.next().unwrap();
        for pair in parsed.into_inner() {
            match pair.as_rule() {
                Rule::EOI => (),
                Rule::root => result.push(PathElement::Root()),
                Rule::json_path_key => result.push(PathElement::Key(pair.as_str().to_string())),
                Rule::array_locator => handle_array_locator(pair, &mut result),
                _ => unreachable!("Unexpected rule: {:?}", pair.as_rule()),
            }
        }

        Ok(JsonPath { elements: result })
    } else {
        crate::bail_constraint_error!("JSON path error near: {:?}", path.to_string());
    }
}

/// Parses path into a Vec of Strings, where each string is a key or an array locator.
/// Handles relaxed grammar for JSON path that is applicable for the -> and ->> operators.
/// https://sqlite.org/json1.html#the_and_operators
pub fn relaxed_json_path(path: &str) -> crate::Result<JsonPath> {
    let parsed = Parser::parse(Rule::relaxed_path, path);

    if let Ok(mut parsed) = parsed {
        let mut result = vec![PathElement::Root()];
        let parsed = parsed.next().unwrap();
        for pair in parsed.into_inner() {
            match pair.as_rule() {
                Rule::EOI => (),
                Rule::root => (),
                Rule::json_path_key => result.push(PathElement::Key(pair.as_str().to_string())),
                Rule::array_locator => handle_array_locator(pair, &mut result),
                Rule::relaxed_array_locator => handle_array_locator(pair, &mut result),
                _ => unreachable!("Unexpected rule: {:?}", pair.as_rule()),
            }
        }
        Ok(JsonPath { elements: result })
    } else {
        crate::bail_constraint_error!("JSON path error near: {:?}", path.to_string());
    }
}

fn handle_array_locator(pair: pest::iterators::Pair<Rule>, result: &mut Vec<PathElement>) {
    let mut array_locator = pair.into_inner();
    let index_or_negative_indicator = array_locator.next().unwrap();

    match index_or_negative_indicator.as_rule() {
        Rule::negative_index_indicator => {
            let negative_offset = array_locator.next().unwrap();
            // TODO: sqlite is able to parse arbitrarily big numbers, but they
            //  always get overflown and cast to i32. Handle this.
            let parsed = negative_offset
                .as_str()
                .parse::<i128>()
                .unwrap_or(i128::MAX);

            result.push(PathElement::ArrayLocator(-parsed as i32));
        }
        Rule::array_offset => {
            let array_offset = index_or_negative_indicator.as_str();
            // TODO: sqlite is able to parse arbitrarily big numbers, but they
            //  always get overflown and cast to i32. Handle this.
            let parsed = array_offset.parse::<i128>().unwrap_or(i128::MAX);

            result.push(PathElement::ArrayLocator(parsed as i32));
        }
        _ => unreachable!(
            "Unexpected rule: {:?}",
            index_or_negative_indicator.as_rule()
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_path_root() {
        let path = json_path("$", true).unwrap();
        assert_eq!(path.elements.len(), 1);
        assert_eq!(path.elements[0], PathElement::Root());
    }

    #[test]
    fn test_json_path_single_locator() {
        let path = json_path("$.x", true).unwrap();
        assert_eq!(path.elements.len(), 2);
        assert_eq!(path.elements[0], PathElement::Root());
        assert_eq!(path.elements[1], PathElement::Key("x".to_string()));
    }

    #[test]
    fn test_json_path_single_array_locator() {
        let path = json_path("$[0]", true).unwrap();
        assert_eq!(path.elements.len(), 2);
        assert_eq!(path.elements[0], PathElement::Root());
        assert_eq!(path.elements[1], PathElement::ArrayLocator(0));
    }

    #[test]
    fn test_json_path_single_negative_array_locator() {
        let path = json_path("$[#-2]", true).unwrap();
        assert_eq!(path.elements.len(), 2);
        assert_eq!(path.elements[0], PathElement::Root());
        assert_eq!(path.elements[1], PathElement::ArrayLocator(-2));
    }

    #[test]
    fn test_json_path_invalid() {
        let invalid_values = vec![
            "", "$$$", "$.", "$ ", "$[", "$]", "$[-1]", "x", "[]", "$[0", "$[0x]", "$\"",
        ];

        for value in invalid_values {
            let path = json_path(value, true);

            match path {
                Err(crate::error::LimboError::Constraint(e)) => {
                    // happy path
                }
                _ => panic!("Expected error for: {:?}, got: {:?}", value, path),
            }
        }
    }

    #[test]
    fn test_json_path() {
        let path = json_path("$.store.book[0].title", true).unwrap();
        assert_eq!(path.elements.len(), 5);
        assert_eq!(path.elements[0], PathElement::Root());
        assert_eq!(path.elements[1], PathElement::Key("store".to_string()));
        assert_eq!(path.elements[2], PathElement::Key("book".to_string()));
        assert_eq!(path.elements[3], PathElement::ArrayLocator(0));
        assert_eq!(path.elements[4], PathElement::Key("title".to_string()));
    }

    #[test]
    fn test_relaxed_json_path_array_locator() {
        let path = json_path("1", false).unwrap();

        assert_eq!(path.elements.len(), 2);
        assert_eq!(path.elements[0], PathElement::Root());
        assert_eq!(path.elements[1], PathElement::ArrayLocator(1));
    }

    #[test]
    fn test_relaxed_json_path_negative_array_locator() {
        let path = json_path("-1", false).unwrap();

        assert_eq!(path.elements.len(), 2);
        assert_eq!(path.elements[0], PathElement::Root());
        assert_eq!(path.elements[1], PathElement::ArrayLocator(-1));
    }

    #[test]
    fn test_relaxed_json_path_key() {
        let path = json_path("x", false).unwrap();

        assert_eq!(path.elements.len(), 2);
        assert_eq!(path.elements[0], PathElement::Root());
        assert_eq!(path.elements[1], PathElement::Key("x".to_string()));
    }
}
