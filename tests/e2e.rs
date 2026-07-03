//! End-to-end tests against a live TeamSpeak 6 server.
//!
//! Skipped unless `TS_E2E_HOST` and `TS_E2E_PASS` are set, so `cargo test`
//! stays green locally without a server. The GitHub Actions `e2e` workflow
//! starts a real `tsserver` and sets these variables.
//!
//! Env vars:
//! - `TS_E2E_HOST` (required)   — server host/ip
//! - `TS_E2E_PASS` (required)   — serveradmin query password
//! - `TS_E2E_PORT` (optional)   — query SSH port, default 10022
//! - `TS_E2E_USER` (optional)   — query user, default `serveradmin`

use std::time::Duration;
use tokio::time::timeout;
use ts3_query_api::definitions::builder::BanParams;
use ts3_query_api::definitions::{
    ChannelProperty, ClientProperty, Permission, Scope, ServerProperty,
};
use ts3_query_api::error::QueryError;
use ts3_query_api::event::Event;
use ts3_query_api::{HostKeyVerification, QueryClient};

struct Config {
    host: String,
    port: u16,
    user: String,
    pass: String,
}

/// Returns the test config, or `None` if the server env vars are not set
/// (in which case the test is skipped).
fn config() -> Option<Config> {
    let host = std::env::var("TS_E2E_HOST").ok()?;
    let pass = std::env::var("TS_E2E_PASS").ok()?;
    let port = std::env::var("TS_E2E_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(10022);
    let user = std::env::var("TS_E2E_USER").unwrap_or_else(|_| "serveradmin".to_string());

    Some(Config { host, port, user, pass })
}

async fn connect(cfg: &Config, host_key: HostKeyVerification) -> Result<QueryClient, QueryError> {
    QueryClient::connect((cfg.host.as_str(), cfg.port), &cfg.user, &cfg.pass, host_key).await
}

/// Connects, retrying transient SSH errors. A rejected host-key handshake (from
/// the mismatch check below) briefly makes the server abort the next SSH auth
/// (`AuthAborted`), so the following connect needs a short backoff.
async fn connect_retry(cfg: &Config, host_key: HostKeyVerification) -> QueryClient {
    let mut last = None;
    for _ in 0..10 {
        match connect(cfg, host_key.clone()).await {
            Ok(client) => return client,
            Err(e @ QueryError::SshError(_)) => {
                last = Some(e);
                tokio::time::sleep(Duration::from_millis(400)).await;
            }
            Err(e) => panic!("connect failed: {e:?}"),
        }
    }
    panic!("connect failed after retries: {last:?}");
}

/// Waits for the next event of the expected variant, draining unrelated events,
/// up to a timeout.
async fn expect_event(client: &QueryClient, want: &str) {
    let deadline = Duration::from_secs(10);
    let found = timeout(deadline, async {
        loop {
            let event = client.wait_for_event().await.expect("event stream closed");
            let got = match &event {
                Event::ChannelCreated(_) => "channelcreated",
                Event::ChannelEdited(_) => "channeledited",
                Event::ChannelDeleted(_) => "channeldeleted",
                Event::ClientMoved(_) => "clientmoved",
                _ => "other",
            };
            if got == want {
                return;
            }
        }
    })
    .await;

    assert!(found.is_ok(), "timed out waiting for event `{want}`");
}

#[tokio::test]
async fn full_e2e() {
    let _ = env_logger::builder().is_test(true).try_init();

    let Some(cfg) = config() else {
        eprintln!("TS_E2E_HOST / TS_E2E_PASS not set — skipping e2e test");
        return;
    };

    // --- host-key verification -------------------------------------------------
    // A wrong fingerprint must be rejected; the error carries the real one, which
    // must then be accepted. Retry transient SSH aborts (server throttling).
    let mut actual = None;
    for _ in 0..10 {
        match connect(
            &cfg,
            HostKeyVerification::Fingerprint("SHA256:definitely-not-the-real-key".to_string()),
        )
        .await
        {
            Err(QueryError::HostKeyMismatch { actual: fp, .. }) => {
                actual = Some(fp);
                break;
            }
            Err(QueryError::SshError(_)) => {
                tokio::time::sleep(Duration::from_millis(400)).await;
            }
            Err(e) => panic!("expected HostKeyMismatch, got error {e:?}"),
            Ok(_) => panic!("expected HostKeyMismatch, but connect succeeded"),
        }
    }
    let actual = actual.expect("host-key mismatch was never reported");
    connect_retry(&cfg, HostKeyVerification::Fingerprint(actual))
        .await
        .version()
        .await
        .expect("version over verified connection");

    // --- main session ----------------------------------------------------------
    let client = connect_retry(&cfg, HostKeyVerification::InsecureAcceptAny).await;
    client.use_sid(1).await.expect("use_sid");

    let me = client.who_am_i().await.expect("who_am_i");
    let my_id = me.id;
    let my_nick = me.nickname.clone();

    // --- read commands ---------------------------------------------------------
    client.version().await.expect("version");
    client.server_info().await.expect("server_info");
    client.channel_list().await.expect("channel_list");
    client.channel_list_full().await.expect("channel_list_full");
    client.client_list().await.expect("client_list");
    client.client_list_full().await.expect("client_list_full");
    client.permission_list().await.expect("permission_list");
    client.ban_list(None, None).await.expect("ban_list");
    client.api_key_list(None, None, None).await.expect("api_key_list");

    client.client_info(my_id).await.expect("client_info");
    client
        .client_info_multiple(&[my_id])
        .await
        .expect("client_info_multiple");

    let channels = client.channel_list().await.expect("channel_list");
    let first_cid = channels.first().expect("server has at least one channel").id;
    client.channel_info(first_cid).await.expect("channel_info");
    client
        .channel_info_multiple(&[first_cid])
        .await
        .expect("channel_info_multiple");
    client
        .channel_perm_list(first_cid)
        .await
        .expect("channel_perm_list");

    // --- events + channel lifecycle -------------------------------------------
    client
        .server_notify_register_all()
        .await
        .expect("server_notify_register_all");

    let cid = client
        .channel_create("ci-e2e-channel", &[ChannelProperty::FlagPermanent(true)])
        .await
        .expect("channel_create");
    expect_event(&client, "channelcreated").await;

    client.channel_info(cid).await.expect("channel_info(new)");
    client
        .channel_edit(cid, &[ChannelProperty::Topic("edited by ci".into())])
        .await
        .expect("channel_edit");
    client
        .channel_add_perm(cid, &Permission::i_channel_needed_join_power(0))
        .await
        .expect("channel_add_perm");
    client
        .channel_add_perm_multiple(cid, &[Permission::i_channel_needed_join_power(10)])
        .await
        .expect("channel_add_perm_multiple");
    client
        .channel_add_perm_id(cid, 86, 0)
        .await
        .expect("channel_add_perm_id");
    client
        .channel_perm_list(cid)
        .await
        .expect("channel_perm_list(new)");

    // --- client move (query client into the new channel and back) --------------
    client
        .client_move(&[my_id], cid, None, false)
        .await
        .expect("client_move(into)");
    client
        .client_move(&[my_id], first_cid, None, false)
        .await
        .expect("client_move(back)");

    // --- client update (reversible) --------------------------------------------
    client
        .client_update(&[ClientProperty::Nickname("ci-e2e-bot".into())])
        .await
        .expect("client_update");
    client
        .client_update(&[ClientProperty::Nickname(my_nick)])
        .await
        .expect("client_update(restore)");

    // --- misc ------------------------------------------------------------------
    client.gm("ci e2e test message").await.expect("gm");
    client.help().await.expect("help");

    // Re-authenticate the serveradmin query account over the existing connection.
    client.login(&cfg.user, &cfg.pass).await.expect("login");

    // --- server edit (reversible) ----------------------------------------------
    let server = client.server_info().await.expect("server_info");
    let original_name = server.name.clone();
    client
        .server_edit(&[ServerProperty::Name("CI E2E".into())])
        .await
        .expect("server_edit");
    client
        .server_edit(&[ServerProperty::Name(original_name)])
        .await
        .expect("server_edit(restore)");

    // --- bans ------------------------------------------------------------------
    let ban = client
        .ban_add(BanParams::default().with_ip("203.0.113.7"))
        .await
        .expect("ban_add");
    client.ban_list(None, None).await.expect("ban_list(after add)");
    client.ban_delete(ban.id).await.expect("ban_delete");

    // `ban_client` needs a real online client id; the only one connected is this
    // query client, and banning it would ban the loopback address and break the
    // rest of the run. Exercise the command against a non-existent id instead:
    // the server rejects it (no ban is created), which still round-trips the
    // command and its error response.
    let ban_client_res = client
        .ban_client(&[i32::MAX], Some(1), Some("ci"), true)
        .await;
    assert!(
        ban_client_res.is_err(),
        "banning a non-existent client should be rejected, got {ban_client_res:?}"
    );

    client.ban_delete_all().await.expect("ban_delete_all");

    // --- api keys (create two so the list is multi-record) ---------------------
    let key1 = client
        .api_key_add(Scope::MANAGE, Some(1), None)
        .await
        .expect("api_key_add #1");
    let key2 = client
        .api_key_add(Scope::READ, Some(1), None)
        .await
        .expect("api_key_add #2");
    let keys = client
        .api_key_list(None, None, None)
        .await
        .expect("api_key_list(multi)");
    assert!(keys.len() >= 2, "expected at least two api keys");
    client.api_key_delete(key1.id).await.expect("api_key_delete #1");
    client.api_key_delete(key2.id).await.expect("api_key_delete #2");

    // --- misc selectors --------------------------------------------------------
    client.use_port(9987).await.expect("use_port");

    // --- cleanup ---------------------------------------------------------------
    client.use_sid(1).await.expect("use_sid(cleanup)");
    client.channel_delete(cid, true).await.expect("channel_delete");
    client.logout().await.expect("logout");

    // `quit` closes the connection; must be the last command on this client.
    // The server may drop the socket around its response, so a closed connection
    // is an expected (successful) outcome here.
    match client.quit().await {
        Ok(()) | Err(QueryError::ConnectionClosed) => {}
        Err(e) => panic!("quit: unexpected error {e:?}"),
    }
}
