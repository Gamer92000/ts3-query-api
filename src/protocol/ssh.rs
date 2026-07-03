use bytes::Buf;
use bytes::Bytes;
use bytes::BytesMut;
use makiko::Session;
use makiko::SessionEvent;
use makiko::SessionReceiver;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::AsyncWrite;
use tokio::io::{AsyncRead, Result as IoResult};

type SendFut = Pin<Box<dyn Future<Output = Result<(), makiko::Error>> + Send>>;

pub struct ChannelWriter {
    channel: Session,
    buffer: BytesMut,
    /// The in-flight `send_stdin` future, if a flush is currently in progress.
    send_fut: Option<SendFut>,
}

impl ChannelWriter {
    pub fn new(channel: Session) -> Self {
        Self {
            channel,
            buffer: BytesMut::with_capacity(8192),
            send_fut: None,
        }
    }
}

impl AsyncWrite for ChannelWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<IoResult<usize>> {
        self.buffer.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        let this = self.get_mut();

        loop {
            // Drive any in-flight send to completion first.
            if let Some(fut) = this.send_fut.as_mut() {
                match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(())) => this.send_fut = None,
                    Poll::Ready(Err(e)) => {
                        this.send_fut = None;
                        return Poll::Ready(Err(std::io::Error::other(e)));
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }

            if this.buffer.is_empty() {
                return Poll::Ready(Ok(()));
            }

            // Own the data in a 'static future so it can be polled across calls.
            let channel = this.channel.clone();
            let data = this.buffer.split().freeze();
            this.send_fut = Some(Box::pin(async move { channel.send_stdin(data).await }));
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        self.poll_flush(cx)
    }
}

pub struct ChannelReader {
    receiver: SessionReceiver,
    buffer: Bytes,
}

impl ChannelReader {
    pub fn new(receiver: SessionReceiver) -> Self {
        Self {
            receiver,
            buffer: Bytes::new(),
        }
    }
}

impl AsyncRead for ChannelReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out_buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<IoResult<()>> {
        loop {
            if self.buffer.has_remaining() {
                let to_copy = std::cmp::min(out_buf.remaining(), self.buffer.len());
                out_buf.put_slice(&self.buffer.split_to(to_copy));
                return Poll::Ready(Ok(()));
            }

            match self.receiver.poll_recv(cx) {
                Poll::Ready(Ok(Some(SessionEvent::StdoutData(data)))) => {
                    self.buffer = data;
                }
                Poll::Ready(Ok(Some(SessionEvent::Eof))) | Poll::Ready(Ok(None)) => {
                    return Poll::Ready(Ok(()));
                }
                Poll::Ready(Ok(_)) => {
                    // Skip non-data events.
                    continue;
                }
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Err(std::io::Error::other(e)));
                }
            }
        }
    }
}
