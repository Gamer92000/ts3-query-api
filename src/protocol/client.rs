use crate::definitions::Status;
use crate::error::{ParseError, QueryError};
use crate::event::Event;
use crate::parser::{Command, Decode, DecodeCustomInto, DecodeInto, Decoder};
use crate::protocol::connection::Connection;
use crate::protocol::ssh::{ChannelReader, ChannelWriter};
use crate::protocol::types::{RawCommandRequest, RawCommandResponse};
use log::{info, warn};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::spawn;

/// Server error id returned by list queries when the result set is empty
/// (e.g. `banlist` with no bans). Treated as an empty result, not an error.
const EMPTY_RESULT_SET: i32 = 1281;

/// How the server's SSH host key is verified when connecting.
///
/// TeamSpeak query-over-SSH does not authenticate the server for you. Without
/// verification a man-in-the-middle can impersonate the server and capture the
/// query login. Pick a policy deliberately.
#[derive(Debug, Clone)]
pub enum HostKeyVerification {
    /// Verify the server's SHA-256 public-key fingerprint against this exact value.
    ///
    /// The expected value uses the OpenSSH format `SHA256:<base64>` (the same
    /// string logged on the first connection). A mismatch aborts the connection
    /// with [`QueryError::HostKeyMismatch`].
    Fingerprint(String),
    /// Accept any server key without verification.
    ///
    /// Vulnerable to man-in-the-middle attacks — only for trusted networks or
    /// throwaway/testing use. Logs a warning on every connect.
    InsecureAcceptAny,
}

pub struct QueryClient {
    command_tx: flume::Sender<RawCommandRequest>,
    event_rx: flume::Receiver<Event>,
    shutdown_tx: flume::Sender<()>,
}

impl QueryClient {
    pub async fn connect<A: ToSocketAddrs>(
        addr: A,
        username: &str,
        password: &str,
        host_key: HostKeyVerification,
    ) -> Result<Self, QueryError> {
        let stream = TcpStream::connect(addr)
            .await
            .map_err(QueryError::ConnectionFailed)?;

        let config = makiko::ClientConfig::default();
        let (client, mut client_rx, client_fut) = makiko::Client::open(stream, config)?;

        tokio::task::spawn(async move {
            if let Err(e) = client_fut.await {
                warn!("SSH client closed: {:?}", e);
            }
        });

        // Reports a host-key mismatch out of the detached event loop so `connect`
        // can surface the specific error instead of a generic SSH failure.
        let (hostkey_err_tx, hostkey_err_rx) = flume::bounded::<QueryError>(1);

        tokio::task::spawn(async move {
            loop {
                let event = match client_rx.recv().await {
                    Ok(Some(event)) => event,
                    Ok(None) => break,
                    Err(e) => {
                        warn!("Error while receiving client event: {:?}", e);
                        break;
                    }
                };

                if let makiko::ClientEvent::ServerPubkey(pubkey, accept) = event {
                    let actual = pubkey.fingerprint();
                    info!("Server pubkey type {}, fingerprint {}", pubkey.type_str(), actual);

                    match &host_key {
                        HostKeyVerification::Fingerprint(expected) if *expected == actual => {
                            accept.accept();
                        }
                        HostKeyVerification::Fingerprint(expected) => {
                            let _ = hostkey_err_tx.send(QueryError::HostKeyMismatch {
                                expected: expected.clone(),
                                actual: actual.clone(),
                            });
                            accept.reject(std::io::Error::new(
                                std::io::ErrorKind::PermissionDenied,
                                "host key fingerprint mismatch",
                            ));
                        }
                        HostKeyVerification::InsecureAcceptAny => {
                            warn!("Accepting server host key WITHOUT verification (insecure): {}", actual);
                            accept.accept();
                        }
                    }
                }
            }
        });

        let auth_res = match client.auth_password(username.into(), password.into()).await {
            Ok(res) => res,
            // A rejected host key aborts the SSH client, which surfaces here as a
            // generic error; prefer the specific mismatch error if we have one.
            Err(e) => return Err(hostkey_err_rx.try_recv().unwrap_or_else(|_| e.into())),
        };

        match auth_res {
            makiko::AuthPasswordResult::Success => {
                info!("We have successfully authenticated using a password");
            }
            makiko::AuthPasswordResult::ChangePassword(prompt) => {
                return Err(QueryError::AuthenticationFailed {
                    message: format!("server requires a password change: {}", prompt.prompt),
                });
            }
            makiko::AuthPasswordResult::Failure(failure) => {
                return Err(QueryError::AuthenticationFailed {
                    message: format!("server rejected authentication: {:?}", failure),
                });
            }
        }

        let (sess, sess_rx) = client
            .open_session(makiko::ChannelConfig::default())
            .await?;

        let shell = sess.shell()?;
        shell.wait().await?;

        let (command_tx, command_rx) = flume::unbounded::<RawCommandRequest>();
        let (event_tx, event_rx) = flume::unbounded::<Event>();
        let (shutdown_tx, shutdown_rx) = flume::unbounded::<()>();

        let mut connection = Connection::new(
            ChannelReader::new(sess_rx),
            ChannelWriter::new(sess),
            event_tx,
            command_rx,
            command_tx.clone(),
            shutdown_rx,
        );

        connection.read_welcome_message().await?;

        spawn(connection.run());

        Ok(Self {
            command_tx,
            event_rx,
            shutdown_tx,
        })
    }

    pub async fn send_command_no_response(&self, command: Command) -> Result<(), QueryError> {
        let command = command.into();
        self.send_command_internal(command).await?;
        Ok(())
    }

    pub async fn send_command<T: Decode>(&self, command: Command) -> Result<T, QueryError> {
        let response = self.send_command_raw(command).await?;
        let mut decoder = Decoder::new(response.content());

        decoder.decode().map_err(QueryError::ParseError)
    }

    pub async fn send_command_into<I: DecodeInto>(
        &self,
        command: Command,
        dst: I,
    ) -> Result<I, QueryError> {
        let response = match self.send_command_raw(command).await {
            Ok(response) => response,
            // A list query with no rows reports the "empty result set" error;
            // treat it as an empty result rather than a failure.
            Err(QueryError::QueryError { id: EMPTY_RESULT_SET, .. }) => return Ok(dst),
            Err(e) => return Err(e),
        };
        let mut decoder = Decoder::new(response.content());

        dst.decode_into(&mut decoder)
            .map_err(QueryError::ParseError)
    }

    pub async fn send_command_custom_into<F, T, I: DecodeCustomInto<T>>(
        &self,
        command: Command,
        dst: I,
        gen: F,
    ) -> Result<I, QueryError>
    where
        F: Fn(&mut Decoder) -> Result<T, ParseError>,
    {
        let response = match self.send_command_raw(command).await {
            Ok(response) => response,
            Err(QueryError::QueryError { id: EMPTY_RESULT_SET, .. }) => return Ok(dst),
            Err(e) => return Err(e),
        };
        let mut decoder = Decoder::new(response.content());

        dst.decode_into(&mut decoder, gen)
            .map_err(QueryError::ParseError)
    }

    pub async fn wait_for_event(&self) -> Result<Event, QueryError> {
        self.event_rx
            .recv_async()
            .await
            .map_err(|_| QueryError::ConnectionClosed)
    }

    async fn send_command_internal(
        &self,
        mut command: String,
    ) -> Result<RawCommandResponse, QueryError> {
        let (response_tx, response_rx) = flume::unbounded::<RawCommandResponse>();

        command.push_str("\n\r");

        self.command_tx
            .send_async(RawCommandRequest {
                data: command,
                response_tx,
            })
            .await
            .map_err(|_| QueryError::ConnectionClosed)?;

        let response = response_rx
            .recv_async()
            .await
            .map_err(|_| QueryError::ConnectionClosed)?;

        Ok(response)
    }

    pub async fn send_command_raw(
        &self,
        command: Command,
    ) -> Result<RawCommandResponse, QueryError> {
        let command = command.into();
        let response = self.send_command_internal(command).await?;

        let status = Decoder::new(response.status())
            .decode_with_name::<Status>()
            .map_err(QueryError::ParseError)?;

        if status.id == 0 {
            Ok(response)
        } else {
            Err(QueryError::QueryError {
                id: status.id,
                message: status.message,
            })
        }
    }
}

impl Drop for QueryClient {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(());
    }
}
