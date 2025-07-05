use bytes::Buf;
use bytes::Bytes;
use bytes::BytesMut;
use makiko::Session;
use makiko::SessionEvent;
use makiko::SessionReceiver;
use std::pin::Pin;
use std::task::Waker;
use std::task::{Context, Poll};
use tokio::io::AsyncWrite;
use tokio::io::{AsyncRead, Result as IoResult};

pub struct ChannelWriter {
    channel: Session,
    buffer: BytesMut,
    flushing: bool,
    waker: Option<Waker>,
}

impl ChannelWriter {
    pub fn new(channel: Session) -> Self {
        Self {
            channel,
            buffer: BytesMut::with_capacity(8192),
            flushing: false,
            waker: None,
        }
    }

    /// Flush the buffer asynchronously, returning Poll::Pending if still flushing,
    /// and Poll::Ready when done.
    fn poll_flush_inner(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        if self.buffer.is_empty() {
            // Nothing to flush
            self.flushing = false;
            return Poll::Ready(Ok(()));
        }

        if self.flushing {
            // Flush is ongoing, store the waker and return Pending
            self.waker = Some(cx.waker().clone());
            return Poll::Pending;
        }

        // Start flushing
        self.flushing = true;

        // Extract the data to send
        let data = self.buffer.split().freeze();

        // Kick off the async send - spawn a task that completes flush and wakes this task
        let channel = self.channel.clone();
        let waker = cx.waker().clone();

        tokio::spawn(async move {
            // Perform the async send
            let _ = channel.send_stdin(data).await;

            // Wake the task to resume polling flush
            waker.wake();
        });

        // Store the current waker for future wake-ups if needed
        self.waker = Some(cx.waker().clone());

        Poll::Pending
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

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        self.as_mut().poll_flush_inner(cx)
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
                    panic!("{:?}", e);
                }
            }
        }
    }
}
