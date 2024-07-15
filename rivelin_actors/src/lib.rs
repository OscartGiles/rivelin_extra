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
//! let (addr, handle) = Actor::spawn(HelloActor, ());
//!
//! // Shadow addr so it goes out of scope after sending a message.
//! // Because no clones of addr exist the actor can no longer receive messages and will gracefully shutdown.
//! {
//!     let addr = addr;
//!     addr.send("World".to_string()).await.unwrap();
//!  }
//!  // Wait for the actor to finish processing messages
//!  handle.await.unwrap();
//! # });
use std::fmt::Display;

use futures::Stream;
use thiserror::Error;
use tokio::sync::mpsc::{self};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};

mod actors;
pub use actors::event_bus;
pub trait Actor
where
    Self: 'static + Sized + Sync + Send,
{
    /// The type of message the actor can receive.
    type Message: Send + Sync;

    /// The type of state the actor holds.
    type State: Send;

    /// Runs when the actor is started.
    fn on_start(&self, _state: &mut Self::State) -> impl std::future::Future<Output = ()> + Send {
        async {}
    }

    /// Runs the actor. The default implementation simple iterates over a stream of messages and calls [`Actor::handle`] for each message.
    /// Override this method if you need to handle messages in a different way.
    fn run(
        self,
        mut message_stream: impl Stream<Item = Self::Message> + Send + 'static + std::marker::Unpin,
        mut state: Self::State,
    ) -> impl std::future::Future<Output = ()> + Send {
        async move {
            while let Some(message) = message_stream.next().await {
                self.handle(message, &mut state).await;
            }
        }
    }

    /// Handle a message sent to the actor.
    ///
    /// ```
    /// async fn handle(&self, message: Self::Message, state: &mut Self::State) {
    ///     println!("Hello {}", message);
    /// }
    /// ```
    fn handle(
        &self,
        message: Self::Message,
        state: &mut Self::State,
    ) -> impl std::future::Future<Output = ()> + Send;

    /// Create an Actor instance.
    fn spawn(actor: Self, state: Self::State) -> (Addr<Self>, tokio::task::JoinHandle<()>) {
        let (sender, receiver) = mpsc::channel::<Self::Message>(1000);

        let handle = tokio::spawn(async move {
            let mut state = state;
            actor.on_start(&mut state).await;
            actor.run(ReceiverStream::new(receiver), state).await;
        });

        (Addr::<Self>::new(sender), handle)
    }

    /// Spawn an actor as per [`Actor::spawn`] but providing an extra receiver to send messages to.
    /// Messages from both the receiver and the addr be sent to the actor.
    /// Prefer [`Actor::spawn`] which provides an [Addr] to send messages to the actor.
    fn spawn_with_recv(
        actor: Self,
        state: Self::State,
        extra_receiver: mpsc::Receiver<impl Into<Self::Message> + Send + 'static>,
    ) -> (Addr<Self>, tokio::task::JoinHandle<()>) {
        let (sender, receiver) = mpsc::channel::<Self::Message>(1000);
        let addr_receiver = ReceiverStream::new(receiver);

        let custom_receiver = ReceiverStream::new(extra_receiver).map(|msg| msg.into());
        let merged_streams = addr_receiver.merge(custom_receiver);

        let handle = tokio::spawn(async move {
            let mut state = state;
            actor.on_start(&mut state).await;
            actor.run(merged_streams, state).await;
        });

        (Addr::<Self>::new(sender), handle)
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
