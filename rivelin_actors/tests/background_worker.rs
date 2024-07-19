use rivelin_actors::{Actor, Addr};
use uuid::Uuid;

use std::collections::HashMap;

use futures::Stream;
use tokio::{
    sync::oneshot,
    task::{AbortHandle, JoinSet},
};
use tokio_stream::StreamExt;

#[derive(Debug)]
pub enum Message {
    Build {
        build_id: Uuid,
    },
    Cancel {
        build_id: Uuid,
        msg: oneshot::Sender<bool>,
    },
}

pub struct BackgroundActorState {
    build_tracker: HashMap<Uuid, AbortHandle>,
    tasks: JoinSet<Uuid>,
}

impl BackgroundActorState {
    pub fn new() -> Self {
        Self {
            build_tracker: HashMap::new(),
            tasks: JoinSet::new(),
        }
    }
}

impl Default for BackgroundActorState {
    fn default() -> Self {
        Self::new()
    }
}

pub struct BackgroundActor;

impl Actor for BackgroundActor {
    type Message = Message;
    type State = BackgroundActorState;

    async fn handle(&self, message: Self::Message, state: &mut Self::State) {
        match message {
            Message::Build { build_id } => {
                println!("Building: {}", build_id);

                let handle = state.tasks.spawn(async move {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    build_id
                });
                state.build_tracker.insert(build_id, handle);
            }
            Message::Cancel { build_id, msg } => {
                println!("Cancelling: build: {}", build_id);
                if let Some(handle) = state.build_tracker.remove(&build_id) {
                    handle.abort();
                    msg.send(true).unwrap();
                } else {
                    eprintln!("Task not found for cancellation: {:?}", build_id);
                    msg.send(false).unwrap();
                }
            }
        }
    }

    async fn run(
        self,
        mut message_stream: impl Stream<Item = Self::Message> + Send + 'static + std::marker::Unpin,
        mut state: Self::State,
    ) {
        loop {
            tokio::select! {
                res = state.tasks.join_next(), if !state.tasks.is_empty() => {
                    if let Some(Ok(build_id)) = res {
                        if let Some(_handle) = state.build_tracker.remove(&build_id) {
                            println!("Task completed: {}", build_id);
                        } else {
                            eprintln!("Task not found for cancellation: {}", build_id);
                        }
                    }
                    else if let Some(Err(e)) = res {
                        if e.is_cancelled() {
                            eprintln!("A task was cancelled.");
                        } else {
                        eprintln!("Task failed: {:?}", e);
                        }
                    }
                }
                Some(message) = message_stream.next() => {
                    self.handle(message, &mut state).await;
                },
                else => break,
            }
        }
    }
}

pub struct BackgroundActorAddr(pub Addr<BackgroundActor>);

impl From<Addr<BackgroundActor>> for BackgroundActorAddr {
    fn from(addr: Addr<BackgroundActor>) -> Self {
        Self(addr)
    }
}
impl BackgroundActorAddr {
    pub async fn build(&self, build_id: Uuid) {
        self.0.send(Message::Build { build_id }).await.unwrap();
    }
    pub async fn cancel(&self, build_id: Uuid) -> bool {
        let (tx, rx) = oneshot::channel();
        self.0
            .send(Message::Cancel { build_id, msg: tx })
            .await
            .unwrap();

        rx.await.unwrap()
    }
}

#[tokio::test]
async fn test_background_worker() {
    let (addr, handle): (BackgroundActorAddr, _) =
        Actor::spawn(BackgroundActor, BackgroundActorState::new());

    let first_build = Uuid::new_v4();
    let second_build = Uuid::new_v4();

    addr.build(first_build).await;
    addr.build(second_build).await;

    let cancel_success = addr.cancel(first_build).await;
    assert!(cancel_success);

    drop(addr); // Drop addr so that the actor can shut down. It will process any remaining tasks before shutting down.
    handle.await.unwrap();
}
