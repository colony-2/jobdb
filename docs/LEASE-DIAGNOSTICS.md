# Remote lease failure diagnostics

Remote `ImportLease`, `Renew`, and `KeepAlive` expose failures through
`*remote.LeaseTransportError`. Import uses operation `renew`; callers can add
initial-import context. A workflow runner wraps the diagnostic in
`*jobdb.LeaseRenewalError`, retaining both its safe message and its error chain.

```go
var diagnostic *remote.LeaseTransportError
if errors.As(err, &diagnostic) {
    // Safe to display or serialize as JSON.
    log.Print(diagnostic.Error())
    // Branch on Category, Phase, ResponseReceived, and StatusCode, not text.
}
if errors.Is(err, jobdb.ErrExecutionLeaseLost) {
    // Authority was rejected or the returned lease binding/validity was invalid.
}
```

## Fields and phases

`Operation` is `renew` or `keep_alive`. `Destination` is the configured service's
scheme and host, including its port. User info, paths, queries, and fragments are
omitted, including encoded values. It describes the configured service, not a
redirect destination. Invalid destinations may be omitted entirely.

`ResponseReceived` distinguishes a received HTTP response from a failure before
one was observed. `StatusCode` preserves the actual status even if reading,
decoding, or converting that response fails. Zero means unknown and is never
rendered as an HTTP status in the message.

| Phase | Meaning |
| --- | --- |
| `request` | Building or editing the request failed. |
| `exchange` | Sending the request or receiving headers failed, or HTTP rejected it. |
| `response_read` | Reading the body failed after headers arrived. |
| `response_decode` | Content type or JSON was invalid. |
| `snapshot` | The renewal snapshot was missing, invalid, or could not be converted. |

The exchange phase includes connection, TLS, redirects, and header waits. Generic
HTTP transports cannot reliably distinguish all those stages. `Category`
provides additional classification from typed causes: DNS, connection refusal,
unreachable destination, timeout, cancellation, TLS, HTTP rejection, body read,
decode, invalid snapshot, unsupported renewal, or unknown. Unknown failures stay
unknown rather than being classified by potentially sensitive error strings.

For example, a body timeout has phase `response_read`, category `timeout`, and the
received HTTP status. A malformed HTTP 200 response is a decode failure, not an
HTTP rejection. Non-200 status classification does not depend on reading its body.

## Safe rendering and causes

Errors produced by the renewal adapter support credential-safe `Error()`,
`GoString()`, `SafeMessage()`, and `json.Marshal`, including value copies. Messages
use fixed descriptions rather than response bodies, headers, or arbitrary nested
error strings. The raw `Err` field is excluded from JSON.

`Err` and `Unwrap()` retain raw causes for `errors.Is` and `errors.As`. They are
**not** safe logging APIs: URL, transport, and conversion errors can contain
credentials or server-controlled text. These guarantees apply to the diagnostics
returned by this adapter, not caller-modified fields or arbitrary remote API errors.

`LeaseRenewalError` displays a nested cause only when it explicitly supplies
`SafeMessage() string`; custom implementations must honor that safety contract.
Otherwise its text remains generic. Use `errors.As` to obtain structured metadata
from a workflow error.

## Execution and compatibility

HTTP 400, 401, 403, 404, and 409 preserve the existing renewal authority-rejection
mapping through `errors.Is(err, jobdb.ErrExecutionLeaseLost)`, even if their bodies
are unreadable. Other HTTP failures and read/decode/conversion failures are
ambiguous: the server might already have extended the lease.

A token-only response from an older server remains supported by `KeepAlive`.
`Renew` requires a snapshot and retains `ErrLeaseRenewalUnsupported` when it is
missing. Wrong content types are decode failures, not evidence of an old server.

There are no new retries or replacement acquisitions. Both renewal entry points
use a bounded request context, and workflow execution still stops after renewal
failure. No backend schema or pgjobdb implementation change is required.

Regression tests inject failures at each boundary across import, renew, and
keepalive, and exercise local HTTP/TLS servers. Runner tests damage responses
after a successful backend renewal, checking initial admission and subsequent
heartbeat cancellation without new work acquisition or outcome chapters.
