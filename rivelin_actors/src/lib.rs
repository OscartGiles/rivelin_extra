//! rivelin_actors is a tokio based actor framework created for the Rivelin Project.
//!
//! ## Actor implementations
//!
//! Some reusable actor implementations are provided to get you started.
//!
//! - [EventBus](crate::event_bus): a pub/sub event bus that can be used to send messages between actors.
//!
//! ## Implement your own
//!
//! ```
//! # use tokio_test;
//! # use rivelin_actors::Actor;
//! # use rivelin_actors::Addr;

//! // Create a simple actor that prints a message
//! struct HelloActor;
//!
//! // Implement the Actor trait for HelloWorldActor
//! impl Actor for HelloActor {
//!     type Message = String;
//!     type State = ();
//!
//!     async fn handle(&self, message: Self::Message, _: &mut Self::State) {
//!         println!("Hello {}", message);
//!     }
//! }
//!
//! # tokio_test::block_on(async {
//! // Spawn an actor.
//! // The address can be cheaply cloned.
//! let (addr, handle): (Addr<HelloActor>, _) = Actor::spawn(HelloActor, ());
//!
//! // Drop addr.
//! // Because no clones exist the actor can no longer receive messages and will gracefully shutdown.
//! drop(addr);
//!
//! // Wait for the actor to finish processing messages
//! handle.await.unwrap();
//! # });
use std::{
    fmt::Display,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{Future, Stream};
use thiserror::Error;
use tokio::sync::mpsc::{self};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};

mod actors;
pub use actors::event_bus;
use tokio_util::sync::CancellationToken;
pub trait Actor
where
    Self: 'static + Sized + Send,
{
    /// The type of message the actor can receive.
    type Message: Send;

    /// The type of state the actor holds.
    type State: Send;

    /// Runs when the actor is started.
    fn on_start(&self, _state: &mut Self::State) -> impl std::future::Future<Output = ()> + Send {
        async {}
    }

    /// Runs when the actor is gracefully stopped.
    fn on_stop(self, _state: &mut Self::State) -> impl std::future::Future<Output = ()> + Send {
        async {}
    }

    /// Runs the actor. The default implementation simple iterates over a stream of messages and calls [`Actor::handle`] for each message.
    /// Override this method if you need to handle messages in a different way.
    fn run(
        self,
        mut message_stream: impl Stream<Item = Self::Message> + Send + 'static + std::marker::Unpin,
        mut state: Self::State,
        cancellation_token: CancellationToken,
    ) -> impl std::future::Future<Output = ()> + Send {
        async move {
            loop {
                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        self.on_stop(&mut state).await;
                        break;
                    },
                    message = message_stream.next() => {
                        match message {
                            Some(message) => self.handle(message, &mut state).await,
                            None => {
                                self.on_stop(&mut state).await;
                                break
                            },
                        };
                    }
                }
            }
        }
    }

    /// Handle a message sent to the actor.
    fn handle(
        &self,
        message: Self::Message,
        state: &mut Self::State,
    ) -> impl std::future::Future<Output = ()> + Send;

    /// Create an Actor instance.
    fn spawn<K>(actor: Self, state: Self::State) -> (K, ActorHandle)
    where
        K: From<Addr<Self>>,
    {
        let (sender, receiver) = mpsc::channel::<Self::Message>(1000);

        let cancellation_token = CancellationToken::new();
        let actors_cancel_token = cancellation_token.clone();

        let handle = tokio::spawn(async move {
            let mut state = state;
            actor.on_start(&mut state).await;
            actor
                .run(ReceiverStream::new(receiver), state, actors_cancel_token)
                .await;
        });

        let addr = Addr::<Self>::new(sender);

        (
            addr.into(),
            ActorHandle {
                task_handle: handle,
                cancellation_token,
            },
        )
    }
}

pub struct ActorHandle {
    task_handle: tokio::task::JoinHandle<()>,
    cancellation_token: CancellationToken,
}

impl ActorHandle {
    pub fn abort(&self) {
        self.task_handle.abort();
    }
    pub async fn graceful_shutdown(self) -> Result<(), tokio::task::JoinError> {
        self.cancellation_token.cancel();
        self.task_handle.await
    }
}

impl Future for ActorHandle {
    type Output = Result<(), tokio::task::JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let inner = unsafe { self.map_unchecked_mut(|s| &mut s.task_handle) };
        inner.poll(cx)
    }
}

/// An address to send messages to an actor.
pub struct Addr<A: Actor> {
    sender: mpsc::Sender<A::Message>,
}

impl<A: Actor> Clone for Addr<A> {
    fn clone(&self) -> Self {
        Addr {
            sender: self.sender.clone(),
        }
    }
}

#[derive(Debug, Error)]
pub struct AddrError<M>(#[source] mpsc::error::SendError<M>);

impl<M> Display for AddrError<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Failed to send message to actor")
    }
}

impl<A: Actor> Addr<A> {
    fn new(sender: mpsc::Sender<A::Message>) -> Self {
        Addr { sender }
    }

    pub async fn send(&self, message: A::Message) -> Result<(), AddrError<A::Message>> {
        self.sender.send(message).await.map_err(AddrError)
    }
}
