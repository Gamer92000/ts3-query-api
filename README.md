# Teamspeak 3 Server Query API for Rust

## Example

```rust
use ts3_query_api::{QueryClient, HostKeyVerification};
use ts3_query_api::error::QueryError;
use ts3_query_api::definitions::Event;

#[tokio::main]
async fn main() -> Result<(), QueryError> {
    // Verify the server's SHA-256 host-key fingerprint (recommended), or pass
    // HostKeyVerification::InsecureAcceptAny to skip verification on a trusted network.
    let client = QueryClient::connect(
        ("localhost", 10022),
        "username",
        "password",
        HostKeyVerification::Fingerprint("SHA256:...".to_string()),
    ).await?;

    // select virtual server
    client.use_sid(1).await?;
    client.server_notify_register(EventType::Channel).await?;
    
    // Wait for events
    while let Ok(event) = client.wait_for_event().await {
        // ...
    }

    Ok(())
}
```
