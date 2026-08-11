use regex::Regex;
use rhai::Engine;
use serde::{Deserialize, Serialize};
use serde_json_path::JsonPath;

use super::DomainError;
use super::model::is_http_token;

pub const MAX_HTTP_ASSERTIONS: usize = 32;
pub const MAX_ASSERTION_VALUE_BYTES: usize = 4 * 1_024;
pub const MAX_SCRIPT_BYTES: usize = 8 * 1_024;
pub const MAX_SCRIPT_INPUT_BYTES: usize = 64 * 1_024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum HttpAssertion {
    BodyContains {
        value: String,
    },
    BodyRegex {
        pattern: String,
    },
    JsonPath {
        path: String,
        #[serde(default)]
        expected: Option<String>,
    },
    ResponseHeader {
        name: String,
        #[serde(default)]
        value: Option<String>,
    },
    Latency {
        max_ms: u64,
    },
    Script {
        source: String,
    },
}

impl HttpAssertion {
    pub(super) fn validate(&self) -> Result<(), DomainError> {
        match self {
            Self::BodyContains { value } => validate_value("body substring", value),
            Self::BodyRegex { pattern } => {
                validate_value("body regex", pattern)?;
                Regex::new(pattern)
                    .map(|_| ())
                    .map_err(|error| invalid(format!("invalid body regex: {error}")))
            }
            Self::JsonPath { path, expected } => {
                JsonPath::parse(path)
                    .map_err(|error| invalid(format!("invalid JSONPath: {error}")))?;
                if let Some(expected) = expected {
                    validate_size(
                        "JSONPath expected value",
                        expected,
                        MAX_ASSERTION_VALUE_BYTES,
                    )?;
                }
                Ok(())
            }
            Self::ResponseHeader { name, value } => {
                if !is_http_token(name) {
                    return Err(invalid(format!("invalid response header name: {name}")));
                }
                if let Some(value) = value {
                    validate_size("response header value", value, MAX_ASSERTION_VALUE_BYTES)?;
                }
                Ok(())
            }
            Self::Latency { max_ms: 0 } => Err(invalid(
                "latency assertion threshold must be greater than zero".to_owned(),
            )),
            Self::Latency { .. } => Ok(()),
            Self::Script { source } => {
                validate_size("assertion script", source, MAX_SCRIPT_BYTES)?;
                if source.trim().is_empty() {
                    return Err(invalid("assertion script must not be empty".to_owned()));
                }
                script_engine()
                    .compile(source)
                    .map(|_| ())
                    .map_err(|error| invalid(format!("invalid assertion script: {error}")))
            }
        }
    }
}

pub fn script_engine() -> Engine {
    let mut engine = Engine::new();
    engine.set_max_operations(10_000);
    engine.set_max_expr_depths(32, 16);
    engine.set_max_call_levels(16);
    engine.set_max_string_size(MAX_SCRIPT_INPUT_BYTES);
    engine.set_max_array_size(1_024);
    engine.set_max_map_size(1_024);
    for symbol in ["eval", "import", "export", "fn", "loop", "while", "for"] {
        engine.disable_symbol(symbol);
    }
    engine
}

fn validate_value(name: &str, value: &str) -> Result<(), DomainError> {
    if value.is_empty() {
        return Err(invalid(format!("{name} must not be empty")));
    }
    validate_size(name, value, MAX_ASSERTION_VALUE_BYTES)
}

fn validate_size(name: &str, value: &str, limit: usize) -> Result<(), DomainError> {
    if value.len() > limit {
        return Err(invalid(format!("{name} exceeds {limit} bytes")));
    }
    Ok(())
}

fn invalid(message: String) -> DomainError {
    DomainError::InvalidTarget(message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_rfc_json_path_features() {
        assert!(JsonPath::parse("$.items[*].name").is_ok());
        assert!(JsonPath::parse("$..name").is_ok());
    }
}
