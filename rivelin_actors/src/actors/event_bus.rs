//! An EventBus Actor.
//!
//! A pub/sub system which allows [Producer]s and [Consumer]s to send and receive messages from a [Topic].
//!
//! The [Topic] trait can be implemented to define a topic.
//!
//! ```
//! # use tokio_test;
//! use rivelin_actors::event_bus::{EventBus, EventBusAddr, EventSinkState, Topic};
//! use rivelin_actors::Actor;
//!
//! #[derive(Hash)]
//! struct HelloTopic;
//!
//! impl Topic for HelloTopic {
//!     type MessageType = String;
//! }
//!
//! # tokio_test::block_on(async {
//! // Create an EventBus actor
//! let (addr, handle) = Actor::spawn(EventBus::new(100), EventSinkState::new());
//! let addr = EventBusAddr(addr);
//!
//! // Subscribe to the topic
//! let mut consumer = addr.consumer(HelloTopic).await.unwrap();
//!
//! // Create a producer for the topic and send a message
//! let producer = addr.producer(HelloTopic);
//! producer.send("Hello from topic a".to_string()).await.unwrap();
//!
//! // Receive a message from the topic
//! let res = consumer.recv().await.unwrap();
//! let value = res.as_ref();
//!
//! assert_eq!(value, "Hello from topic a");
//!
//! // Stop the actor
//! handle.abort();
//!
//! # })
//! ```
//! ## [Topic] fields
//!
//! Structs that implement [Topic] can have fields which define unique topics.
//!
//! ```
//! # use tokio_test;
//! # use rivelin_actors::event_bus::{EventBus, EventBusAddr, EventSinkState, Topic};
//! # use rivelin_actors::Actor;
//! #[derive(Hash, Copy, Clone)]
//! struct TopicWithValues {
//!    a: u32,
//!    b: u64,
//! };
//!
//! impl Topic for TopicWithValues {
//!     type MessageType = String;
//! }
//! # tokio_test::block_on(async {
//! # let (addr, handle) = Actor::spawn(EventBus::new(100), EventSinkState::new());
//! # let addr = EventBusAddr(addr);
//!
//! // These are two different topics
//! let topic_a = TopicWithValues { a: 1, b: 1 };
//! let topic_b = TopicWithValues { a: 1, b: 2 };
//!
//! let mut consumer_a = addr.consumer(topic_a).await.unwrap();
//! let mut consumer_b = addr.consumer(topic_b).await.unwrap();
//! # });
//! ```
//!
//! ## [Stream] [Consumer]
//!
//! [Consumer] can be converted to a [Stream] to receive messages.
//! ```
//! # use tokio_test;
//! # use rivelin_actors::event_bus::{EventBus, EventBusAddr, EventSinkState, Topic};
//! # use rivelin_actors::Actor;
//! use futures_util::StreamExt;
//!
//! # tokio_test::block_on(async {
//! # let (addr, handle) = Actor::spawn(EventBus::new(100), EventSinkState::new());
//! # let addr = EventBusAddr(addr);
//! # #[derive(Hash)]
//! # struct HelloTopic;
//! # impl Topic for HelloTopic {
//! #     type MessageType = String;
//! # }
//! let expected_values = vec!["First Message", "Second Message"];
//!
//! // Create a consumer and convert it to a stream
//! let mut consumer = addr.consumer(HelloTopic).await.unwrap().to_stream().enumerate();
//! # let producer = addr.producer(HelloTopic);
//! # producer.send("First Message".to_string()).await.unwrap();
//! # producer.send("Second Message".to_string()).await.unwrap();
//!
//! let mut recovered_values = vec![];
//! while let Some((i, Ok(value))) = consumer.next().await {
//!     recovered_values.push(value.as_ref().to_owned());
//!     if i == expected_values.len() - 1 {
//!         break;
//!     }
//! }
//!
//! assert_eq!(recovered_values, expected_values);
//! # });
//! ```
//! ## Compile time safety
//!
//! Implementations of the [Topic] trait must provide an [Topic::MessageType] associated type. This is the type of message that can be sent and received for the topic.
//! This allows for a type safe API.
//!
//! ```
//! # use tokio_test;
//! # use rivelin_actors::event_bus::{EventBus, EventBusAddr, EventSinkState, Topic};
//! # use rivelin_actors::Actor;
//! #[derive(Hash)]
//! struct TopicA;
//!
//! impl Topic for TopicA {
//!     type MessageType = u32;
//! }
//! # tokio_test::block_on(async {
//! # let (addr, handle) = Actor::spawn(EventBus::new(100), EventSinkState::new());
//! # let addr = EventBusAddr(addr);
//!
//! let producer = addr.producer(TopicA);
//! let mut consumer = addr.consumer(TopicA).await.unwrap();
//!
//! // This is Ok
//! producer.send(42).await.unwrap();
//!
//! // This is a compile time error:
//! // producer.send("Hello".to_string()).await.unwrap();
//!
//! //| producer.send("Hello".to_string()).await.unwrap();
//! //|          ---- ^^^^^^^^^^^^^^^^^^^ expected `u32`, found `String`
//! //|          |
//! //|          arguments to this method are incorrect
//! //|
//! # });
//! ```

use std::{
    any::{Any, TypeId},
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
    marker::PhantomData,
    sync::Arc,
};

use crate::{Actor, Addr, AddrError};
use futures::{Stream, TryStreamExt};
use std::fmt::Debug;
use tokio::sync::{
    broadcast::{self, error::RecvError, Receiver},
    oneshot,
};
use tokio_stream::wrappers::{errors::BroadcastStreamRecvError, BroadcastStream};

#[derive(Debug, Clone)]
pub struct Event {
    payload: Arc<dyn Any + Send + Sync>,
    topic_id: TopicId,
}

#[derive(Debug, Clone)]
pub struct RecoveredEvent<T: Topic> {
    payload: Arc<dyn Any + Send + Sync>,
    phantom: PhantomData<T>,
}

impl<T: Topic> AsRef<T::MessageType> for RecoveredEvent<T> {
    fn as_ref(&self) -> &T::MessageType {
        match self.payload.downcast_ref::<T::MessageType>() {
            Some(value) => value,
            None => panic!(
                "Could not downcast. This should never happen and is a bug. Please report it."
            ),
        }
    }
}

#[derive(Debug)]
pub enum EventMessage {
    Event(Event),
    Subscribe {
        topic_id: TopicId,
        sender: oneshot::Sender<broadcast::Receiver<Event>>,
    },
}

pub struct EventSinkState {
    listeners: HashMap<TopicId, broadcast::Sender<Event>>,
}
impl EventSinkState {
    pub fn new() -> Self {
        Self {
            listeners: HashMap::new(),
        }
    }
}
impl Default for EventSinkState {
    fn default() -> Self {
        Self::new()
    }
}

/// An EventBus Actor.
///
/// The EventBus is a pub/sub system where actors can subscribe to a topic and receive messages sent to that topic.
///
/// The EventBus allows registering [Producer]s and [Consumer]s for [Topic]s.
/// A [Topic] can have multiple [Consumer]s and multiple [Producer]s.
pub struct EventBus {
    channel_capacity: usize,
}

impl EventBus {
    /// Create a new EventBus with a channel capacity.
    /// The channel capacity is the maximum number of messages that can be buffered in the channel.
    /// Each topic has its own channel.
    /// When a [Producer] sends a message to a topic, the message is sent to the channel for that topic.
    /// If a [Consumer] that is subscribed to that topic does not receive the message in time, the message will be dropped.
    pub fn new(channel_capacity: usize) -> Self {
        Self { channel_capacity }
    }
}

impl Actor for EventBus {
    type Message = EventMessage;
    type State = EventSinkState;

    async fn handle(&self, message: Self::Message, state: &mut Self::State) {
        match message {
            EventMessage::Event(event) => {
                if let Some(sender) = state.listeners.get(&event.topic_id) {
                    let _ = sender.send(event);
                } else {
                    println!("No listeners for topic: {:?}", &event.topic_id);
                }
            }
            EventMessage::Subscribe {
                topic_id: topic_type,
                sender,
            } => {
                let project_entry = state.listeners.entry(topic_type);

                let receiver = match project_entry {
                    std::collections::hash_map::Entry::Occupied(inner) => {
                        let recv = inner.get().subscribe();
                        recv
                    }
                    std::collections::hash_map::Entry::Vacant(_) => {
                        let (send, recv) = broadcast::channel(self.channel_capacity);
                        let _ = project_entry.or_insert(send);
                        recv
                    }
                };

                if let Err(e) = sender.send(receiver) {
                    println!("Failed to send receiver: {:?}", e);
                }
            }
        }
    }
}

/// A Consumer for a topic.
pub struct Consumer<T: Topic> {
    receiver: Receiver<Event>,
    phantom: PhantomData<T>,
}

impl<T: Topic> Consumer<T> {
    /// Receive a message from the topic.
    pub async fn recv(&mut self) -> Result<RecoveredEvent<T>, RecvError> {
        let value = self.receiver.recv().await?;
        Ok(RecoveredEvent {
            payload: value.payload,
            phantom: PhantomData,
        })
    }

    /// Convert the consumer to a [Stream].
    pub fn to_stream(
        self,
    ) -> impl Stream<Item = Result<RecoveredEvent<T>, BroadcastStreamRecvError>> {
        BroadcastStream::new(self.receiver).map_ok(|elem| RecoveredEvent {
            payload: elem.payload,
            phantom: PhantomData,
        })
    }
}

// /// A Consumer for a topic.
// pub struct ConsumerStream<T: Topic> {
//     stream: BroadcastStream<Event>,
//     phantom: PhantomData<T>,
// }

// impl<T: Topic> From<Consumer<T>> for ConsumerStream<T> {
//     fn from(consumer: Consumer<T>) -> Self {
//         Self {
//             stream: BroadcastStream::new(consumer.receiver),
//             phantom: PhantomData,
//         }
//     }
// }

/// An EventBusAddr is a handle to an EventBus actor.
/// Use to create [Producer]s and [Consumer]s for topics.
pub struct EventBusAddr(pub Addr<EventBus>);

impl EventBusAddr {
    /// Create a new consumer for a topic.
    pub async fn consumer<T: Topic>(
        &self,
        topic: T,
    ) -> Result<Consumer<T>, AddrError<EventMessage>> {
        let (tx, rx) = oneshot::channel();

        self.0
            .send(EventMessage::Subscribe {
                topic_id: topic.topic_id(),
                sender: tx,
            })
            .await?;

        Ok(Consumer {
            receiver: rx.await.unwrap(),
            phantom: PhantomData,
        })
    }

    /// Create a new producer for a topic.
    pub fn producer<T>(&self, topic: T) -> Producer<T>
    where
        T: Topic,
    {
        Producer {
            addr: self.0.clone(),
            topic_id: topic.topic_id(),
            phantom: PhantomData,
        }
    }
}

/// A Producer for a topic.
pub struct Producer<T> {
    addr: Addr<EventBus>,
    topic_id: TopicId,
    phantom: PhantomData<T>,
}

impl<T: Topic> Producer<T> {
    /// Send a message to the topic.
    pub async fn send(&self, message: T::MessageType) -> Result<(), AddrError<EventMessage>> {
        let message = Event {
            payload: Arc::new(message),
            topic_id: self.topic_id.clone(),
        };
        self.addr.send(EventMessage::Event(message)).await?;

        Ok(())
    }
}

#[derive(Hash, Debug, PartialEq, Eq, Clone)]
pub struct TopicId {
    hash: u64,
}

/// A [Topic] is a type which defines a topic that [Producer]s can send messages to and [Consumer]s can receive messages from.
/// Implementations of [Topic] must:
///  1. Implement [Hash]. Required to uniquely identify the topic.
///  2. Implement [Sized]. Required to be able to create a [TopicId] for the topic.
///  3. Be `'static`` (i.e. Cannot contain non-static references).
pub trait Topic: std::hash::Hash
where
    Self: 'static + Sized,
{
    /// The type of message that can be sent and received for the topic.
    type MessageType: Send + Sync + 'static;

    /// The topic_id is a unique identifier for a topic.
    /// It is based on both the type of the topic and the hash of the implementing type.
    fn topic_id(self) -> TopicId {
        let mut state = DefaultHasher::new();
        let type_id = TypeId::of::<Self>();
        type_id.hash(&mut state);
        self.hash(&mut state);

        let topic_hash = state.finish();
        TopicId { hash: topic_hash }
    }
}
