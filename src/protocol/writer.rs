use crate::error::QueryError;
use crate::protocol::ssh::ChannelWriter;
use crate::protocol::types::{RawCommandRequest, RawCommandResponse};
use log::debug;
use tokio::io::{AsyncWriteExt, BufWriter};

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

        let response = self
            .response_rx
            .recv_async()
            .await
            .map_err(|_| QueryError::ConnectionClosed)?;

        command
            .response_tx
            .send_async(response)
            .await
            .map_err(|_| QueryError::ConnectionClosed)?;

        Ok(())
    }
}
