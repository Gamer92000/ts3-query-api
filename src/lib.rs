/// # TeamSpeak 3 Query Library
///
/// ## Example
/// ```no_run
/// use ts3_query_api::{QueryClient, HostKeyVerification};
/// use ts3_query_api::error::QueryError;
/// use ts3_query_api::definitions::EventType;
///
/// #[tokio::main]
/// async fn main() -> Result<(), QueryError> {
///     // Pin the server's SHA-256 fingerprint (printed on first connect), or use
///     // HostKeyVerification::InsecureAcceptAny on a trusted network.
///     let client = QueryClient::connect(
///         ("localhost", 10022),
///         "username",
///         "password",
///         HostKeyVerification::InsecureAcceptAny,
///     ).await?;
///
///     // select virtual server
///     client.use_sid(1).await?;
///     client.server_notify_register(EventType::Channel).await?;
///
///     // Wait for events
///     while let Ok(event) = client.wait_for_event().await {
///         // ...
///     }
///
///     Ok(())
/// }
/// ```
pub mod requests;

pub mod definitions;
pub mod event;
pub mod parser;

pub mod error;

mod macros;
mod protocol;

pub use protocol::*;
