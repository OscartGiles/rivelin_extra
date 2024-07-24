use std::time::Duration;

use futures_util::StreamExt;
use rivelin_actors::event_bus::{EventBus, EventBusAddr, EventSinkState, Filter, Topic};
use rivelin_actors::Actor;

#[tokio::test]
async fn event_bus_string() -> anyhow::Result<()> {
    let (addr, _handle) = Actor::spawn(EventBus::new(100), EventSinkState::new());
    let addr = EventBusAddr(addr);

    struct HelloTopic;

    impl Topic for HelloTopic {
        type MessageType = String;
    }

    let mut consumer = addr
        .consumer::<HelloTopic, _>(Filter::all_subtopics)
        .await?;

    let producer = addr.producer(HelloTopic);
    producer
        .send("Hello from topic a".to_string())
        .await
        .unwrap();

    let res = consumer.recv().await.unwrap();
    let value = res.as_ref();
    assert_eq!(value, "Hello from topic a");

    Ok(())
}

#[tokio::test]
async fn event_bus_consumer_stream() -> anyhow::Result<()> {
    let (addr, _handle) = Actor::spawn(EventBus::new(100), EventSinkState::new());
    let addr = EventBusAddr(addr);

    struct HelloTopic;

    impl Topic for HelloTopic {
        type MessageType = String;
    }

    let mut consumer = addr
        .consumer::<HelloTopic, _>(Filter::all_subtopics)
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
    struct StructMessage {
        name: String,
        age: u8,
    }

    struct ComplexTopic;

    impl Topic for ComplexTopic {
        type MessageType = StructMessage;
    }

    let mut consumer = addr
        .consumer::<ComplexTopic, _>(Filter::all_subtopics)
        .await?;

    let producer = addr.producer(ComplexTopic);

    let message = StructMessage {
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

    let mut consumer_a = addr
        .consumer::<TopicWithValues, _>(Filter::all_subtopics)
        .await?;
    let producer_a = addr.producer(topic_a);

    let mut consumer_b = addr
        .consumer::<TopicWithValues, _>(Filter::all_subtopics)
        .await?;
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

    #[allow(dead_code)]
    struct SubTopic {
        a: u32,
        b: u64,
    }

    impl Topic for SubTopic {
        type MessageType = u32;
    }

    // These are two different topics
    let subtopic_a = SubTopic { a: 1, b: 1 };
    let subtopic_b = SubTopic { a: 1, b: 2 };
    let subtopic_c = SubTopic { a: 1, b: 3 };

    let producer_a = addr.producer(subtopic_a);
    let producer_b = addr.producer(subtopic_b);
    let producer_c = addr.producer(subtopic_c);

    // We have to give a type hint somewhere here (one of two ways)
    let mut consumer = addr.consumer(|t: &SubTopic| t.b > 2).await?;

    producer_a.send(1).await?;
    producer_b.send(2).await?;
    producer_c.send(3).await?;

    let res = consumer.recv().await.unwrap();

    // Should only receive the last value, even though sent last
    assert_eq!(*res.as_ref(), 3);

    // No more values should be available. If we haven't received on in 500ms will assume there are no more values
    let res = tokio::time::timeout(Duration::from_millis(500), consumer.recv()).await;
    assert!(res.is_err());

    Ok(())
}

#[tokio::test]
async fn event_bus_multiple_subtopics() -> anyhow::Result<()> {
    let (addr, _handle) = Actor::spawn(EventBus::new(100), EventSinkState::new());
    let addr = EventBusAddr(addr);

    #[allow(dead_code)]
    struct SubTopicA {
        a: u32,
        b: u64,
    }

    impl Topic for SubTopicA {
        type MessageType = u32;
    }

    struct SubTopicB {
        name: String,
    }

    impl Topic for SubTopicB {
        type MessageType = &'static str;
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

    let mut consumer_topic_a = addr.consumer(|t: &SubTopicA| t.b < 3).await?;
    let mut consumer_topic_b = addr.consumer(|t: &SubTopicB| t.name == "Oscar").await?;

    producer_a.send(1).await?;
    producer_b.send(2).await?;
    producer_c.send(3).await?;
    producer_d.send("hello").await?;

    // Consumer for topic a can get first two values from SubtopicA, an then no more values available
    let res = consumer_topic_a.recv().await.unwrap();
    assert_eq!(*res.as_ref(), 1);
    let res = consumer_topic_a.recv().await.unwrap();
    assert_eq!(*res.as_ref(), 2);

    // No more values should be available. If we haven't received on in 500ms will assume there are no more values
    let res = tokio::time::timeout(Duration::from_millis(500), consumer_topic_a.recv()).await;
    assert!(res.is_err());

    // Consumer for topic b can get the value from SubtopicB
    let res = consumer_topic_b.recv().await.unwrap();
    assert_eq!(*res.as_ref(), "hello");

    let res = tokio::time::timeout(Duration::from_millis(500), consumer_topic_b.recv()).await;
    assert!(res.is_err());

    Ok(())
}
