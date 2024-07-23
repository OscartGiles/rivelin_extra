use std::any::Any;
use std::hash::Hash;
use std::sync::Arc;

use futures_util::StreamExt;
use rivelin_actors::event_bus::{EventBus, EventBusAddr, EventSinkState, Topic, TopicAny};
use rivelin_actors::Actor;

#[tokio::test]
async fn event_bus_string() -> anyhow::Result<()> {
    let (addr, _handle) = Actor::spawn(EventBus::new(100), EventSinkState::new());
    let addr = EventBusAddr(addr);

    #[derive(Hash)]
    struct HelloTopic;

    impl Topic for HelloTopic {
        type MessageType = String;
    }

    let t1 = HelloTopic::id();
    let t2 = HelloTopic.topic_id();

    println!("t1 = {:?}, t2 = {:?}", t1, t2);

    let mut consumer = addr.consumer::<HelloTopic, _>(|_| true).await?;
    let producer = addr.producer(HelloTopic);
    producer.send("Hello from topic a".to_string()).await?;

    let res = consumer.recv().await.unwrap();
    let value = res.as_ref();
    assert_eq!(value, "Hello from topic a");

    Ok(())
}

#[tokio::test]
async fn event_bus_consumer_stream() -> anyhow::Result<()> {
    let (addr, _handle) = Actor::spawn(EventBus::new(100), EventSinkState::new());
    let addr = EventBusAddr(addr);

    #[derive(Hash)]
    struct HelloTopic;

    impl Topic for HelloTopic {
        type MessageType = String;
    }

    let mut consumer = addr
        .consumer::<HelloTopic, _>(|e| true)
        .await?
        .to_stream()
        .enumerate();
    let producer = addr.producer(HelloTopic);
    producer.send("Hello from topic a".to_string()).await?;
    producer.send("Hello from topic b".to_string()).await?;

    let expected_values = vec!["Hello from topic a", "Hello from topic b"];

    let mut recovered_values = vec![];

    while let Some((i, value)) = consumer.next().await {
        recovered_values.push(value.as_ref().to_owned());

        if i == expected_values.len() - 1 {
            break;
        }
    }

    assert_eq!(recovered_values, expected_values);

    Ok(())
}

#[tokio::test]
async fn event_bus_struct() -> anyhow::Result<()> {
    let (addr, _handle) = Actor::spawn(EventBus::new(100), EventSinkState::new());
    let addr = EventBusAddr(addr);

    #[derive(PartialEq, Eq, Debug, Clone)]
    struct ComplexMessage {
        name: String,
        age: u8,
    }

    #[derive(Hash)]
    struct ComplexTopic;

    impl Topic for ComplexTopic {
        type MessageType = ComplexMessage;
    }

    let mut consumer = addr.consumer::<ComplexTopic, _>(|e| true).await?;
    let producer = addr.producer(ComplexTopic);

    let message = ComplexMessage {
        name: "Oscar".to_string(),
        age: 33,
    };

    producer.send(message.clone()).await?;

    let res = consumer.recv().await.unwrap();
    assert_eq!(res.as_ref().to_owned(), message);

    Ok(())
}

#[tokio::test]
async fn event_bus_topic_values() -> anyhow::Result<()> {
    let (addr, _handle): (EventBusAddr, _) =
        Actor::spawn(EventBus::new(100), EventSinkState::new());

    #[derive(Hash, Clone, Copy)]
    struct TopicWithValues {
        a: u32,
        b: u64,
    }

    impl Topic for TopicWithValues {
        type MessageType = String;
    }

    // These are the same topics, but different subtopics
    let topic_a = TopicWithValues { a: 1, b: 1 };
    let topic_b = TopicWithValues { a: 1, b: 2 };

    let mut consumer_a = addr.consumer::<TopicWithValues, _>(|_| true).await?;
    let producer_a = addr.producer(topic_a);

    let mut consumer_b = addr.consumer::<TopicWithValues, _>(|_| true).await?;
    let producer_b = addr.producer(topic_b);

    producer_a.send("Message A".into()).await?;
    producer_b.send("Message B".into()).await?;

    let res = consumer_a.recv().await.unwrap();
    assert_eq!(res.as_ref(), "Message A");

    let res = consumer_a.recv().await.unwrap();
    assert_eq!(res.as_ref(), "Message B");

    let res = consumer_b.recv().await.unwrap();
    assert_eq!(res.as_ref(), "Message A");

    let res = consumer_b.recv().await.unwrap();
    assert_eq!(res.as_ref(), "Message B");

    Ok(())
}

#[tokio::test]
async fn event_bus_subtopics() -> anyhow::Result<()> {
    let (addr, _handle) = Actor::spawn(EventBus::new(100), EventSinkState::new());
    let addr = EventBusAddr(addr);

    #[derive(Hash, Clone, Copy)]
    struct SubTopicA {
        a: u32,
        b: u64,
    }

    impl Topic for SubTopicA {
        type MessageType = u32;
    }

    #[derive(Hash, Clone)]
    struct SubTopicB {
        name: String,
    }

    impl Topic for SubTopicB {
        type MessageType = f64;
    }

    // These are two different topics
    let topic_a = SubTopicA { a: 1, b: 1 };
    let topic_b = SubTopicA { a: 1, b: 2 };
    let topic_c = SubTopicA { a: 1, b: 3 };

    let topic_d = SubTopicB {
        name: String::from("Oscar"),
    };

    let producer_a = addr.producer(topic_a);
    let producer_b = addr.producer(topic_b);
    let producer_c = addr.producer(topic_c);

    let producer_d = addr.producer(topic_d);

    // We have to give a type hint somewhere here (one of two ways)
    let mut consumer = addr.consumer(|a: &SubTopicA| a.b > 2).await?;
    let mut consumer2 = addr.consumer::<SubTopicB, _>(|a| a.name == "Oscar").await?;

    producer_a.send(1).await?;
    producer_b.send(2).await?;
    producer_c.send(3).await?;
    producer_d.send(99.0).await?;

    let res = consumer.recv().await.unwrap();
    assert_eq!(*res.as_ref(), 3);

    let res = consumer2.recv().await.unwrap();
    assert_eq!(*res.as_ref(), 99.0);

    Ok(())
}
