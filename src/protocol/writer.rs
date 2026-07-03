use crate::error::QueryError;
use crate::protocol::ssh::ChannelWriter;
use crate::protocol::types::{RawCommandRequest, RawCommandResponse};
use log::debug;
use std::time::Duration;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::time::timeout;

/// Maximum time to wait for a server response to a single command. The TS3
/// protocol has no request IDs, so responses are paired with commands purely by
/// order; if a response never arrives the pairing can never recover, so a
/// timeout tears the connection down rather than risk mismatching later replies.
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);

pub(super) struct Writer {
    writer: BufWriter<ChannelWriter>,
    response_rx: flume::Receiver<RawCommandResponse>,
    command_rx: flume::Receiver<RawCommandRequest>,
}

impl Writer {
    pub fn new(
        writer: ChannelWriter,
        response_rx: flume::Receiver<RawCommandResponse>,
        command_rx: flume::Receiver<RawCommandRequest>,
    ) -> Self {
        Self {
            writer: BufWriter::new(writer),
            response_rx,
            command_rx,
        }
    }

    pub async fn run(mut self) -> Result<(), QueryError> {
        loop {
            let command = self
                .command_rx
                .recv_async()
                .await
                .map_err(|_| QueryError::ConnectionClosed)?;

            self.write_command(command).await?;
        }
    }

    async fn write_command(&mut self, command: RawCommandRequest) -> Result<(), QueryError> {
        debug!("[C->S] {}", &command.data[..command.data.len() - 2]);

        self.writer
            .write_all(command.data.as_bytes())
            .await
            .map_err(QueryError::WriteError)?;

        let _ = self.writer.flush().await.map_err(QueryError::WriteError);

        let response = match timeout(RESPONSE_TIMEOUT, self.response_rx.recv_async()).await {
            Ok(Ok(response)) => response,
            Ok(Err(_)) => return Err(QueryError::ConnectionClosed),
            Err(_) => return Err(QueryError::Timeout),
        };

        command
            .response_tx
            .send_async(response)
            .await
            .map_err(|_| QueryError::ConnectionClosed)?;

        Ok(())
    }
}
