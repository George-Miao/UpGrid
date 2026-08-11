use std::collections::BTreeMap;

use regex::Regex;
use rhai::{Dynamic, Map, Scope};
use serde_json::Value;
use serde_json_path::JsonPath;
use upgrid_raft::domain::{HttpAssertion, MAX_SCRIPT_INPUT_BYTES, script_engine};

use crate::http::Response;

pub(super) fn evaluate(
    assertions: &[HttpAssertion],
    response: &Response,
    latency_ms: u64,
) -> Option<String> {
    for (index, assertion) in assertions.iter().enumerate() {
        if let Err(error) = evaluate_one(assertion, response, latency_ms) {
            return Some(format!("HTTP assertion {} failed: {error}", index + 1));
        }
    }
    None
}

fn evaluate_one(
    assertion: &HttpAssertion,
    response: &Response,
    latency_ms: u64,
) -> Result<(), String> {
    let body = String::from_utf8_lossy(&response.body);
    match assertion {
        HttpAssertion::BodyContains { value } => require(
            body.contains(value),
            format!("body does not contain {value:?}"),
        ),
        HttpAssertion::BodyRegex { pattern } => {
            let regex =
                Regex::new(pattern).map_err(|error| format!("body regex is invalid: {error}"))?;
            require(
                regex.is_match(&body),
                format!("body does not match /{pattern}/"),
            )
        }
        HttpAssertion::JsonPath { path, expected } => {
            let json: Value = serde_json::from_slice(&response.body)
                .map_err(|error| format!("response is not valid JSON: {error}"))?;
            let path =
                JsonPath::parse(path).map_err(|error| format!("JSONPath is invalid: {error}"))?;
            let values = path.query(&json).all();
            if values.is_empty() {
                return Err(format!("JSONPath {path} selected no values"));
            }
            if let Some(expected) = expected {
                let matches = values.iter().any(|value| match value {
                    Value::String(value) => value == expected,
                    value => value.to_string() == *expected,
                });
                require(
                    matches,
                    format!("JSONPath {path} did not select expected value {expected:?}"),
                )
            } else {
                Ok(())
            }
        }
        HttpAssertion::ResponseHeader { name, value } => {
            let actual = response.headers.get(&name.to_ascii_lowercase());
            let Some(actual) = actual else {
                return Err(format!("response header {name:?} is missing"));
            };
            if let Some(expected) = value {
                require(
                    actual == expected,
                    format!("response header {name:?} was {actual:?}, expected {expected:?}"),
                )
            } else {
                Ok(())
            }
        }
        HttpAssertion::Latency { max_ms } => require(
            latency_ms <= *max_ms,
            format!("latency {latency_ms} ms exceeds {max_ms} ms"),
        ),
        HttpAssertion::Script { source } => evaluate_script(
            source,
            response.status.as_u16(),
            latency_ms,
            &body,
            response.url.as_ref(),
            &response.headers,
        ),
    }
}

fn evaluate_script(
    source: &str,
    status: u16,
    latency_ms: u64,
    body: &str,
    final_url: &str,
    headers: &BTreeMap<String, String>,
) -> Result<(), String> {
    let mut scope = Scope::new();
    scope.push("status", i64::from(status));
    scope.push("latency_ms", as_script_int(latency_ms));
    scope.push("body", bounded_script_value(body));
    scope.push("final_url", bounded_script_value(final_url));
    let headers = headers
        .iter()
        .take(1_024)
        .map(|(name, value)| {
            (
                name.clone().into(),
                Dynamic::from(bounded_script_value(value)),
            )
        })
        .collect::<Map>();
    scope.push("headers", headers);
    let passed = script_engine()
        .eval_with_scope::<bool>(&mut scope, source)
        .map_err(|error| format!("script error: {error}"))?;
    require(passed, "script returned false".to_owned())
}

fn as_script_int(value: u64) -> i64 {
    value.try_into().unwrap_or(i64::MAX)
}

fn bounded_script_value(value: &str) -> String {
    let mut end = value.len().min(MAX_SCRIPT_INPUT_BYTES);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_owned()
}

fn require(condition: bool, diagnostic: String) -> Result<(), String> {
    condition.then_some(()).ok_or(diagnostic)
}

#[cfg(test)]
mod tests {
    use http::StatusCode;
    use url::Url;

    use super::*;

    fn response() -> Response {
        Response {
            status: StatusCode::OK,
            headers: BTreeMap::from([("content-type".to_owned(), "application/json".to_owned())]),
            body: br#"{"healthy":true,"items":[{"name":"api"}]}"#.to_vec(),
            url: Url::parse("https://example.com/health").unwrap(),
        }
    }

    #[test]
    fn evaluates_assertions_in_order() {
        let assertions = [
            HttpAssertion::ResponseHeader {
                name: "Content-Type".to_owned(),
                value: Some("application/json".to_owned()),
            },
            HttpAssertion::JsonPath {
                path: "$..name".to_owned(),
                expected: Some("api".to_owned()),
            },
            HttpAssertion::BodyRegex {
                pattern: "\\\"healthy\\\":true".to_owned(),
            },
            HttpAssertion::Latency { max_ms: 50 },
            HttpAssertion::Script {
                source: "status == 200 && latency_ms < 50 && headers[\"content-type\"] == \
                         \"application/json\""
                    .to_owned(),
            },
        ];

        assert_eq!(evaluate(&assertions, &response(), 12), None);
    }

    #[test]
    fn reports_the_first_failed_assertion() {
        let assertions = [
            HttpAssertion::Latency { max_ms: 10 },
            HttpAssertion::BodyContains {
                value: "missing".to_owned(),
            },
        ];

        assert_eq!(
            evaluate(&assertions, &response(), 12).as_deref(),
            Some("HTTP assertion 1 failed: latency 12 ms exceeds 10 ms")
        );
    }
}
