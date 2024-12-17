#![deny(missing_docs)]
//! Kitsune2 bootstrap server is an HTTP REST server for handling bootstrapping
//! discovery of peer network reachability in p2p applications.
//!
//! Despite being in the kitsune2 repo, `bootstrap_srv` and `bootstrap_cli`
//! do not depend on any Kitsune2 crates. This is to ensure the bootstrapping
//! functionality is well-defined, self-contained, easily testable in
//! isolation, and usable for projects that don't choose to make use of
//! Kitsune2 itself.
//!
//! That being said, the bootstrap server and client are designed to transfer
//! the `AgentInfoSigned` data type defined in the `kitsune2_api` crate. Since
//! the canonical encoding of that type is JSON, we just redefine a subset
//! of the schema here, and only validate the parts required for bootstrapping.
//!
//! For additional details, please see the [spec].

/// This is a documentation module containing the kitsune2_bootstrap spec.
///
/// #### 1. Types
///
/// All base64 in this spec uses: <https://datatracker.ietf.org/doc/html/rfc4648#section-5>
///
/// The url safe alphabet (with `-` and `_`) and no padding characters (`=`)
/// added when encoding or required when decoding.
///
/// - `Base64Agent` - base64UrlSafeNoPad encoded string agent id.
/// - `Base64Space` - base64UrlSafeNoPad encoded string space id.
/// - `Base64Sig` - base64UrlSafeNoPad encoded string crypto signature.
/// - `Json` - string containing json that can be decoded.
/// - `I64` - string containing an i64 number indicating the number of
///           microseconds since the unix epoch. Since the unix epoch
///           is canonically defined in UTC, the timestamp is also in UTC.
///
/// ```text
/// AgentInfoSigned = { "agentInfo": Json, "signature": Base64Sig }
/// AgentInfo = {
///   "agent": Base64Agent,
///   "space": Base64Space,
///   "createdAt": I64,
///   "expiresAt": I64,
///   "isTombstone": boolean
/// }
/// ```
///
/// Any other properties on these objects will be ignored and pass through.
///
/// #### 2. REST API
///
/// ##### 2.1. In Brief
///
/// ```text
/// ErrResponse = { "error": string }
/// OkResponse = {}
/// ListResponse = [ AgentInfoSigned, .. ]
/// ```
///
/// - `PUT /bootstrap/<Base64Space>/<Base64Agent>`
///   - Request Body: `AgentInfoSigned`
///   - Response Body: `OkResponse | ErrResponse`
/// - `GET /bootstrap/<Base64Space>`
///   - Response Body: `ListResponse | ErrResponse`
/// - `GET /health`
///   - Response Body: `OkResponse | ErrResponse`
///
/// ##### 2.2. Publishing info to the bootstrap server.
///
/// A `PUT` on `/bootstrap/<Base64Space>/<Base64Agent>` with an
/// `AgentInfoSigned` json object as the request body.
///
/// - The server MUST reject the request if the body is > 1024 bytes.
/// - The server MUST reject the request if `createdAt` is not within
///   3 minutes in either direction of the server time.
/// - The server MUST reject the request if `expiresAt` is in the past.
/// - The server MUST reject the request if `expiresAt <= createdAt`.
/// - The server MUST reject the request if `expiresAt - createdAt` is
///   more than 30 minutes.
/// - The server MUST reject the request if `signature` is invalid vs
///   the `agentInfo`.
/// - If `isTombstone` is `true`, the server MUST delete any existing
///   info being held matching the `space` and `agent` and with a `createdAt`
///   less than the `createdAt` of the tombstone record.
/// - If `isTombstone` is `false`, the server MAY begin storing the info.
///   See section 3. on storage strategies below.
///
/// ##### 2.3. Listing data stored on the bootstrap server.
///
/// A `GET` on `/bootstrap/<Base64Space>`.
///
/// - The server MUST respond with a complete list of stored infos.
/// - If there are no infos stored at this space, the server MUST return
///   an empty list (`[]`).
/// - The only reason a server MAY return an `ErrResponse` for this request
///   is in the case of internal server error.
///
/// ##### 2.4. Health check.
///
/// A `GET` on `/health`.
///
/// - The server, in general, SHOULD return `OkResponse` to this request.
/// - The server MAY return `ErrResponse` for internal errors or some other
///   inability to continue serving correctly.
///
/// #### 3. Storage Strategies
///
/// ##### 3.1. The Future
///
/// It is the intention someday in the future to add a "trusted" strategy,
/// that will be triggerd via a new api, perhaps `/registerTrust/Base64Space`.
///
/// Even when that API is implemented, however, the default strategy defined
/// next will be used on all spaces that have not been registered with a
/// different strategy. Therefore, we can proceed for now without consideration
/// for any other future strategies.
///
/// ##### 3.2. The "Default" Storage Strategy
///
/// Assumptions:
///
/// - We don't want to store unbounded count infos in each space.
/// - We like storing long-running reliable nodes.
/// - We don't want to ONLY store long-running nodes to avoid eclipse scenarios.
///
/// The solution put forth here is to allocate half our storage space to
/// long-running nodes, and the other half to whoever has most recently
/// published an info. The strategy for accomplishing this is defined here:
///
/// Consider "the store" acting as a stack. It can be implemented in any manner,
/// but the following strategy assumes new entries are added to the end,
/// and when any entries are removed, the indexes of any items after them
/// will be decrimented.
///
/// - The server SHOULD allow setting the maximum number of infos that will be
///   allowed in a single space.
///   For the duration of this document, that value will be called MAX_INFOS.
///   It is recommended to default that value to `32` to keep the maximum
///   response size from a request to around 32KiB.
/// - The server SHOULD delete expired infos periodically.
/// - If the server is already storing an agent, and a `PUT` with a newer
///   `createdAt` arrives, the existing entry MUST be replaced.
/// - If the store count is < MAX_INFOS, the server MUST push the info
///   onto the stack.
/// - If the store count is >= MAX_INFOS, the server MUST delete the entry
///   at MAX_INFOS / 2 and then push the info onto the stack.
///
/// #### 4. Rate Limiting
///
/// Info count within a space is limited by the storage strategy above. But
/// space count within a server is unbounded. While servers will likely want
/// to limit request counts in general, we are also going to have to take some
/// special care with PUT requests that result in creation of a new space that
/// was not previously tracked in order to mitigate attacks.
///
/// - A server SHOULD delete any spaces that no longer contain agents.
/// - A server SHOULD decide a max count of spaces it intends to support
///   (based on available memory or disk space for storing agents within those
///   spaces), and then return error 429 beyond that.
/// - A server MAY make the above max space count configurable.
/// - A server SHOULD track the IP addresses of clients that make PUT requests
///   which result in space creation, and error with 429 if that frequency
///   is beyond a limit.
/// - A server MAY make this limit configurable.
///
/// #### 5. Client Recommendations
///
/// - In the case of server unreachable or 500 error, a client should use
///   an exponential backoff strategy to avoid stressing the server when
///   it does come back online.
/// - A client should poll the bootstrap server on an interval >= 5 minutes
///   to balance getting new infos to avoid partitioning without worry
///   of hitting the rate limit.
/// - A client, in general, should set expiration to 20 minutes beyond
///   the info creation time.
/// - A client, in general, should re-publish a new info on a 15 minute
///   interval.
#[cfg(doc)]
pub mod spec {}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .expect("InvalidSystemTime")
        .as_micros() as i64
}

mod config;
pub use config::*;

mod parse;
use parse::*;

mod store;
use store::*;

mod space;
use space::*;

mod http;
use http::*;

mod server;
pub use server::*;

#[cfg(test)]
mod test;

pub(crate) mod event;
