# Teamspeak 3 Server Query API for Rust

## Example

```rust
use ts3_query_api::QueryClient;
use ts3_query_api::error::QueryError;
use ts3_query_api::definitions::Event;

#[tokio::main]
async fn main() -> Result<(), QueryError> {
    let client = QueryClient::connect(("localhost", 10022), "username", "password").await?;

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
