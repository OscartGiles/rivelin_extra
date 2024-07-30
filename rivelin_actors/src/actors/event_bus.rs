//! An EventBus Actor.
//!
//! A pub/sub system which allows [Producer]s and [Consumer]s to send and receive messages from a [Topic].
//!
//! The [Topic] trait can be implemented to define a topic.
//!
//! ```rust
//! # use tokio_test;
//! use rivelin_actors::event_bus::{EventBus, EventBusAddr, EventSinkState, Topic, Filter};
//! use rivelin_actors::Actor;
//!
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
//! let mut consumer = addr
//!     .consumer::<HelloTopic, _>(Filter::all_subtopics)
//!     .await
//!     .unwrap();
//!
//! // Create a producer for the topic and send a message
//! let producer = addr.producer(HelloTopic);
//! producer
//!     .send("Hello from topic a".to_string())
//!     .await
//!     .unwrap();
//!
//! // Receive a message from the topic
//! let res = consumer.recv().await.unwrap();
//! let value = res.as_ref();
//!
//! assert_eq!(value, &"Hello from topic a".to_string());
//! // Stop the actor
//! handle.graceful_shutdown().await.unwrap();
//!
//! # })
//! ```
//!
//! ## Compile time safety
//!
//! Implementations of the [Topic] trait must provide an [Topic::MessageType] associated type. This is the type of message that can be sent and received for the topic.
//! This allows for a type safe API.
//! However, [EventBus] supports multiple topics with different message types. This is achieves using the [Any] trait internally.
//!
//! ```
//! # use tokio_test;
//! # use rivelin_actors::event_bus::{EventBus, EventBusAddr, EventSinkState, Topic, Filter};
//! # use rivelin_actors::Actor;
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
//! let mut consumer = addr.consumer::<TopicA, _>(Filter::all_subtopics).await.unwrap();
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
//!
//! ## Subtopics
//!
//! Structs that implement [Topic] can have fields. The value of these fields define "subtopics".
//!
//! Subtopics allow for more fine grained control over which messages a [Consumer] receives. A consumer could receive
//! all messages for a topic, or filter messages by subtopic.
//! When creating a consumer, a function or closure is passed which defines the rule for filtering subtopics.
//!
//! This is similar to the concept of a [`routing_keys`](https://www.rabbitmq.com/tutorials/tutorial-five-python#topic-exchange) in RabbitMQ,
//! except rather than using a string to filter the consumers messages, we use create arbitrary filtering rules passed as closures.
//! This provides both type safety and flexibility.
//!
//! ```rust
//! # use tokio_test;
//! # use rivelin_actors::event_bus::{EventBus, EventBusAddr, EventSinkState, Topic, Filter};
//! # use rivelin_actors::Actor;
//! struct SubTopics {
//!    a: u32,
//!    b: u32,
//! };
//!
//! impl Topic for SubTopics {
//!     type MessageType = &'static str;
//! }
//!
//! # tokio_test::block_on(async {
//! let (addr, handle) = Actor::spawn(EventBus::new(100), EventSinkState::new());
//! let addr = EventBusAddr(addr);
//!
//! // These are two different topics
//! let topic_a = SubTopics { a: 1, b: 1 };
//! let topic_b = SubTopics { a: 1, b: 2 };
//!
//! let producer_1 = addr.producer(topic_a);
//! let producer_2 = addr.producer(topic_b);
//!
//! // consumer_a will receive all messages
//! let mut consumer_a = addr
//!     .consumer::<SubTopics, _>(Filter::all_subtopics)
//!     .await
//!     .unwrap();
//!
//! // consumer_b will receive all messages where the topic.b == 2
//! let mut consumer_b = addr.consumer(|t: &SubTopics| t.b == 2).await.unwrap();
//!
//! producer_1.send("This is subtopic_a").await.unwrap();
//! producer_2.send("This is subtopic_b").await.unwrap();
//!
//! assert_eq!(
//!     *consumer_a.recv().await.unwrap().as_ref(),
//!     "This is subtopic_a"
//! );
//! assert_eq!(
//!     *consumer_a.recv().await.unwrap().as_ref(),
//!     "This is subtopic_b"
//! );
//!
//! assert_eq!(
//!     *consumer_b.recv().await.unwrap().as_ref(),
//!     "This is subtopic_b"
//! );
//!
//! handle.graceful_shutdown().await.unwrap();
//! # })
//! ```
//!
//! ## Enum Subtopics
//!
//! You can also use enums to define subtopics. In this example there is a single type called `EnumSubTopic` which defines
//! subtopics as enum variants and associated data. Remember that while enum variants and data define subtopics, only the type
//! (in this case `EnumSubTopic`) defines the topic.
//!
//! ```rust
//! # use tokio_test;
//! # use rivelin_actors::event_bus::{EventBus, EventBusAddr, EventSinkState, Topic, Filter};
//! # use rivelin_actors::Actor;
//! # #[allow(dead_code)]
//! enum EnumSubTopic {
//!     A,
//!     B { a: &'static str },
//! }
//!
//! impl Topic for EnumSubTopic {
//!     type MessageType = u32;
//! }
//! # tokio_test::block_on(async {
//! # let (addr, _handle) = Actor::spawn(EventBus::new(100), EventSinkState::new());
//! # let addr = EventBusAddr(addr);
//!
//! let producer_a = addr.producer(EnumSubTopic::B {
//!     a: "subtopic of subtopic B",
//! });
//!
//! let mut consumer_topic_b = addr
//!     .consumer(
//!         |t: &EnumSubTopic| matches!(t, EnumSubTopic::B { a } if a == &"subtopic of subtopic B"),
//!     )
//!     .await.unwrap();
//!
//! # let mut _consumer_topic_a = addr.consumer(|t| matches!(t, EnumSubTopic::A)).await.unwrap();
//! producer_a.send(3).await.unwrap();
//! let event = consumer_topic_b.recv().await.unwrap();
//!
//! assert_eq!(*event.as_ref(), 3);
//! # });
//!
//! ```
//! ## [Stream] [Consumer]
//!
//! [Consumer] can be converted to a [Stream] to receive messages.
//! ```
//! # use tokio_test;
//! # use rivelin_actors::event_bus::{EventBus, EventBusAddr, EventSinkState, Topic, Filter};
//! # use rivelin_actors::Actor;
//! # use futures_util::StreamExt;
//! # tokio_test::block_on(async {
//! # let (addr, handle) = Actor::spawn(EventBus::new(100), EventSinkState::new());
//! # let addr = EventBusAddr(addr);
//! # struct HelloTopic;
//! # impl Topic for HelloTopic {
//! #     type MessageType = String;
//! # }
//! let expected_values = vec!["First Message", "Second Message"];
//!
//! // Create a consumer and convert it to a stream
//! let mut consumer = addr.consumer::<HelloTopic, _>(Filter::all_subtopics).await
//!     .unwrap()
//!     .to_stream()
//!     .enumerate();
//! # let producer = addr.producer(HelloTopic);
//! # producer.send("First Message".to_string()).await.unwrap();
//! # producer.send("Second Message".to_string()).await.unwrap();
//!
//! let mut recovered_values = vec![];
//! while let Some((i, value)) = consumer.next().await {
//!     recovered_values.push(value.as_ref().to_owned());
//!     if i == expected_values.len() - 1 {
//!         break;
//!     }
//! }
//!
//! assert_eq!(recovered_values, expected_values);
//! # });
//! ```

use std::{
    any::{Any, TypeId},
    collections::HashMap,
    marker::PhantomData,
    sync::Arc,
};

use crate::{Actor, Addr, AddrError};
use futures::{Stream, StreamExt};
use std::fmt::Debug;
use tokio::sync::{
    mpsc::{self, Receiver},
    oneshot,
};
use tokio_stream::wrappers::ReceiverStream;

/// A [Topic] is a type which defines a topic that [Producer]s can send messages to and [Consumer]s can receive messages from.
pub trait Topic: 'static + Send + Sync {
    /// The type of message that can be sent and received for the topic.
    type MessageType: Send + Sync;
}

#[derive(Clone, Debug)]
pub struct Event {
    payload: Arc<dyn Any + Send + Sync>,
    topic: Arc<dyn Any + Send + Sync>,
}

pub enum EventMessage {
    Event(Event),
    Subscribe {
        topic_id: TypeId,
        sender: oneshot::Sender<mpsc::Receiver<Event>>,
        filter: OptionalBoxedFilter,
    },
}

impl Debug for EventMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Event(arg0) => f.debug_tuple("Event").field(arg0).finish(),
            Self::Subscribe {
                topic_id,
                sender,
                filter: _,
            } => f
                .debug_struct("Subscribe")
                .field("topic_id", topic_id)
                .field("sender", sender)
                .finish(),
        }
    }
}

type OptionalBoxedFilter = Option<Box<dyn Fn(&Arc<dyn Any + Send + Sync>) -> bool + Send + Sync>>;

struct EventSender {
    sender: mpsc::Sender<Event>,
    filter: OptionalBoxedFilter,
}

impl EventSender {
    fn new(sender: mpsc::Sender<Event>, filter: OptionalBoxedFilter) -> Self {
        Self { sender, filter }
    }
}

pub struct EventSinkState {
    listeners: HashMap<TypeId, Vec<EventSender>>,
}

impl EventSinkState {
    pub fn new() -> Self {
        Self {
            listeners: HashMap::new(),
        }
    }

    fn get_topic_listeners(
        &self,
        topic_any: Arc<dyn Any + Send + Sync>,
    ) -> Option<impl Iterator<Item = &EventSender>> {
        // Get the topic id of dyn TopicAny, not Arc<dyn TopicAny>. So dereference before calling.
        let id = (*topic_any).type_id();

        self.listeners.get(&id).map(|topic_listeners| {
            topic_listeners.iter().filter(move |s| {
                if let Some(filter) = &s.filter {
                    (filter)(&topic_any)
                } else {
                    true
                }
            })
        })
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
                let listeners = state.get_topic_listeners(event.topic.clone());

                if let Some(listeners) = listeners {
                    for l in listeners.into_iter() {
                        l.sender.send(event.clone()).await.unwrap();
                    }
                }
            }
            EventMessage::Subscribe {
                topic_id,
                sender,
                filter,
            } => {
                let topic_listerners = state.listeners.entry(topic_id).or_default();
                let (send, receiver) = mpsc::channel(self.channel_capacity);
                topic_listerners.push(EventSender::new(send, filter));

                if let Err(e) = sender.send(receiver) {
                    println!("Failed to send receiver: {:?}", e);
                }
            }
        }
    }
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

/// A Consumer for a topic.
pub struct Consumer<T: Topic> {
    receiver: Receiver<Event>,
    phantom: PhantomData<T>,
}

impl<T: Topic> Consumer<T> {
    /// Receive a message from the topic.
    pub async fn recv(&mut self) -> Option<RecoveredEvent<T>> {
        self.receiver.recv().await.map(|e| RecoveredEvent {
            payload: e.payload,
            phantom: PhantomData,
        })
    }

    /// Receive a message from the topic.
    pub fn try_recv(
        &mut self,
    ) -> Result<RecoveredEvent<T>, tokio::sync::mpsc::error::TryRecvError> {
        self.receiver.try_recv().map(|e| RecoveredEvent {
            payload: e.payload,
            phantom: PhantomData,
        })
    }

    /// Convert the consumer to a [Stream].
    pub fn to_stream(self) -> impl Stream<Item = RecoveredEvent<T>> {
        ReceiverStream::new(self.receiver).map(|elem| RecoveredEvent {
            payload: elem.payload,
            phantom: PhantomData,
        })
    }
}

// /// A Consumer for a topic.
// pub struct ConsumerStream<T: Topic> {
//     stream: ReceiverStream<Event>,
//     phantom: PhantomData<T>,
// }

// impl<T: Topic> From<Consumer<T>> for ConsumerStream<T> {
//     fn from(consumer: Consumer<T>) -> Self {
//         Self {
//             stream: ReceiverStream::new(consumer.receiver),
//             phantom: PhantomData,
//         }
//     }
// }

/// An EventBusAddr is a handle to an EventBus actor.
/// Use to create [Producer]s and [Consumer]s for topics.
pub struct EventBusAddr(pub Addr<EventBus>);

impl From<Addr<EventBus>> for EventBusAddr {
    fn from(addr: Addr<EventBus>) -> Self {
        Self(addr)
    }
}

impl EventBusAddr {
    /// Create a new consumer for a topic.
    pub async fn consumer<T: Topic, F>(&self, f: F) -> Result<Consumer<T>, AddrError<EventMessage>>
    where
        F: Fn(&T) -> bool + Send + Sync + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let topic_id = TypeId::of::<T>();

        let f = move |item: &Arc<dyn Any + Send + Sync>| {
            // Downcast
            let value = match item.downcast_ref::<T>() {
                Some(value) => value,
                None => panic!(
                    "Could not downcast. This should never happen and is a bug. Please report it."
                ),
            };

            f(value)
        };

        self.0
            .send(EventMessage::Subscribe {
                topic_id,
                sender: tx,
                filter: Some(Box::new(f)),
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
            topic: Arc::new(topic),
        }
    }
}

/// A Producer for a topic.
pub struct Producer<T: Topic> {
    addr: Addr<EventBus>,
    topic: Arc<T>,
    // phantom: PhantomData<T>,
}

impl<T: Topic> Producer<T> {
    /// Send a message to the topic.
    pub async fn send(&self, message: T::MessageType) -> Result<(), AddrError<EventMessage>> {
        let t = self.topic.clone();

        let message = Event {
            payload: Arc::new(message),
            topic: t,
        };
        self.addr.send(EventMessage::Event(message)).await?;

        Ok(())
    }
}

pub struct Filter;

impl Filter {
    pub fn all_subtopics<T: Topic>(_: &T) -> bool {
        true
    }
}
