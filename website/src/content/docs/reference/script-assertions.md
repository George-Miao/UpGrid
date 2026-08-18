---
title: Script assertions
description: Reference for Boolean Rhai expressions in HTTP response assertions.
---

A script assertion evaluates one [Rhai](https://rhai.rs/) expression after an HTTP probe. The expression must return `true` for the assertion to pass. A `false` result or an evaluation error fails the assertion.

## Available values

| Name | Rhai type | Value |
| --- | --- | --- |
| `status` | Integer | Final HTTP response status code |
| `latency_ms` | Integer | Complete probe duration in milliseconds |
| `body` | String | Response body |
| `final_url` | String | Final response URL after redirects |
| `headers` | Map of strings | Response headers with lower-case names |

The body, final URL, and each header value contain at most the first 64 KiB. The headers map contains at most 1,024 entries.

## Write an expression

Use standard Rhai comparison, Boolean, string, and map operations. For example:

```text
status == 200
```

```text
status == 200 && latency_ms < 500
```

```text
status == 200 && headers["content-type"] == "application/json"
```

```text
status >= 200 && status < 300 && final_url == "https://example.com/health"
```

UpGrid checks the accepted HTTP status range before it evaluates response assertions. It then evaluates assertions in their displayed order and records the first failure as the evaluation diagnostic.

## Execution limits

UpGrid compiles a script when it validates the target. A script can contain at most 8 KiB and cannot be empty.

Each evaluation has these limits:

- 10,000 operations
- 32 expression levels
- 16 function-call levels
- 64 KiB strings
- 1,024-element arrays and maps

The engine disables generated-code evaluation, module import and export, function definitions, and all loop forms. These limits keep probe evaluation bounded.

## HTTP API shape

Use the `script` assertion kind and put the Rhai expression in `source`:

```json
{
  "kind": "script",
  "source": "status == 200 && latency_ms < 500"
}
```

A target can contain at most 32 assertions. See [Monitor services](/guides/targets/#assert-an-http-response) for assertion ordering and the other assertion kinds.
