use std::hash::Hash;
use std::time::Duration;

use futures_util::StreamExt;
use rivelin_actors::event_bus::{EventBus, EventBusAddr, EventSinkState, Topic};
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

    let mut consumer = addr.consumer(HelloTopic).await?;
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

    let mut consumer = addr.consumer(HelloTopic).await?.to_stream().enumerate();
    let producer = addr.producer(HelloTopic);
    producer.send("Hello from topic a".to_string()).await?;
    producer.send("Hello from topic b".to_string()).await?;

    let expected_values = vec!["Hello from topic a", "Hello from topic b"];

    let mut recovered_values = vec![];

    while let Some((i, Ok(value))) = consumer.next().await {
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

    let mut consumer = addr.consumer(ComplexTopic).await?;
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
    let (addr, _handle) = Actor::spawn(EventBus::new(100), EventSinkState::new());
    let addr = EventBusAddr(addr);

    #[derive(Hash, Clone, Copy)]
    struct TopicWithValues {
        a: u32,
        b: u64,
    }

    impl Topic for TopicWithValues {
        type MessageType = String;
    }

    // These are two different topics
    let topic_a = TopicWithValues { a: 1, b: 1 };
    let topic_b = TopicWithValues { a: 1, b: 2 };

    let mut consumer_a = addr.consumer(topic_a).await?;
    let producer_a = addr.producer(topic_a);

    let mut consumer_b = addr.consumer(topic_b).await?;
    let producer_b = addr.producer(topic_b);

    producer_a.send("Message from topic_a".into()).await?;

    producer_b.send("Message from topic_b".into()).await?;

    let res = consumer_a.recv().await.unwrap();
    assert_eq!(res.as_ref(), "Message from topic_a");

    let res = consumer_b.recv().await.unwrap();
    assert_eq!(res.as_ref(), "Message from topic_b");

    // Nothing more to read from topic_a
    assert!(
        tokio::time::timeout(Duration::from_millis(50), consumer_a.recv())
            .await
            .is_err()
    );

    Ok(())
}

// #[tokio::test]
// async fn event_bus_subtopics() -> anyhow::Result<()> {
//     let (addr, _handle) = Actor::spawn(EventBus::new(100), EventSinkState::new());
//     let addr = EventBusAddr(addr);

//     #[derive(Hash, Clone, Copy)]
//     struct TopicA {
//         a: u32,
//         b: u64,
//     }

//     struct TopicAFilter {
//         a: Option<fn(u32) -> bool>,
//         b: Option<fn(u64) -> bool>,
//     }

//     impl TopicAFilter {
//         fn a(mut self, filter: fn(u32) -> bool) -> Self {
//             self.a = Some(filter);
//             self
//         }
//         fn b(mut self, filter: fn(u64) -> bool) -> Self {
//             self.b = Some(filter);
//             self
//         }
//     }

//     impl TopicA {
//         fn filter() -> TopicAFilter {
//             TopicAFilter { a: None, b: None }
//         }
//     }

//     impl Topic for TopicA {
//         type MessageType = String;
//     }

//     // These are two different topics
//     let topic_a = TopicA { a: 1, b: 1 };
//     let topic_b = TopicA { a: 1, b: 2 };
//     let topic_c = TopicA { a: 1, b: 3 };

//     // Listen to TopicA but only if a == 1 and b < 3
//     let filter_a = TopicA::filter().a(|val| val == 1).b(|val| val < 3);

//     let producer_a = addr.producer(topic_a);
//     let producer_b = addr.producer(topic_b);
//     let producer_c = addr.producer(topic_c);

//     // let mut consumer = addr.consumer_filter(filter_a).await?;

//     producer_a.send("Message from topic_a".into()).await?;
//     producer_b.send("Message from topic_b".into()).await?;
//     producer_c.send("Message from topic_c".into()).await?;

//     // let res = consumer.recv().await.unwrap();
//     // assert_eq!(res.as_ref(), "Message from topic_a");

//     Ok(())
// }

// #[test]
// fn test_hash() {
//     // let mut map: HashMap<TopicId, u32> = HashMap::new();

//     #[derive(Hash, Clone, Copy)]
//     struct TopicA {
//         a: u32,
//         b: u64,
//     }

//     impl Topic for TopicA {
//         type MessageType = String;
//     }

//     struct TopicAFilter {
//         a: Option<fn(u32) -> bool>,
//         b: Option<fn(u64) -> bool>,
//     }

//     impl TopicAFilter {
//         fn a(mut self, filter: fn(u32) -> bool) -> Self {
//             self.a = Some(filter);
//             self
//         }
//         fn b(mut self, filter: fn(u64) -> bool) -> Self {
//             self.b = Some(filter);
//             self
//         }
//     }

//     impl TopicA {
//         fn filter() -> TopicAFilter {
//             TopicAFilter { a: None, b: None }
//         }
//     }

//     let topic_a = TopicA { a: 1, b: 1 };
//     let topic_b = TopicA { a: 1, b: 2 };
//     let topic_c = TopicA { a: 1, b: 3 };

//     let filter_a = TopicA::filter().a(|val| val == 1).b(|val| val < 3);

//     impl TopicA {
//         fn check_filter(&self, filter: &TopicAFilter) -> bool {
//             if let Some(filter) = filter.a {
//                 if !filter(self.a) {
//                     return false;
//                 }
//             }

//             if let Some(filter) = filter.b {
//                 if !filter(self.b) {
//                     return false;
//                 }
//             }

//             true
//         }
//     }

//     println!("{}", topic_a.check_filter(&filter_a));
//     println!("{}", topic_b.check_filter(&filter_a));
//     println!("{}", topic_c.check_filter(&filter_a));

//     // map.insert(topic_a.topic_id(), 1);
//     // map.insert(topic_b.topic_id(), 2);
//     // map.insert(topic_c.topic_id(), 3);

//     // println!("{:#?}", map);
// }
