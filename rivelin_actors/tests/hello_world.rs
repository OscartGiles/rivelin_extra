use rivelin_actors::{Actor, Addr};
use tokio::sync::oneshot;

#[derive(Debug)]
struct HelloMessage {
    content: String,
    channel: oneshot::Sender<String>,
}

struct HelloWorldActor;

impl Actor for HelloWorldActor {
    type Message = HelloMessage;
    type State = u8;

    async fn handle(&self, message: Self::Message, n_messages: &mut Self::State) {
        *n_messages += 1;

        let response = format!(
            "Hello {}! I have received {} messages",
            message.content, n_messages
        );

        message
            .channel
            .send(response)
            .expect("Channel must be open");
    }
}

struct HelloWorldAddr(Addr<HelloWorldActor>);

impl From<Addr<HelloWorldActor>> for HelloWorldAddr {
    fn from(addr: Addr<HelloWorldActor>) -> Self {
        Self(addr)
    }
}

impl HelloWorldAddr {
    async fn message(&self, message: impl Into<String>) -> anyhow::Result<String> {
        let (tx, rx) = oneshot::channel();

        self.0
            .send(HelloMessage {
                content: message.into(),
                channel: tx,
            })
            .await?;

        Ok(rx.await?)
    }
}

#[tokio::test]
async fn hello_world() -> anyhow::Result<()> {
    let (addr, _handle): (HelloWorldAddr, _) = Actor::spawn(HelloWorldActor, 0);

    let response = addr.message("World").await?;
    assert_eq!(response, "Hello World! I have received 1 messages");

    let response = addr.message("Rivelin").await?;
    assert_eq!(response, "Hello Rivelin! I have received 2 messages");

    Ok(())
}
