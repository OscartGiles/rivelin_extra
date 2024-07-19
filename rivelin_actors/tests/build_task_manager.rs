use rivelin_actors::{Actor, Addr};

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
        resp_chan: oneshot::Sender<tokio::task::Id>,
    },
    Cancel {
        build_id: tokio::task::Id,
        resp_chan: oneshot::Sender<bool>,
    },
}

pub struct TaskManagerState {
    abort_handles: HashMap<tokio::task::Id, AbortHandle>,
    tasks: JoinSet<()>,
}

impl TaskManagerState {
    pub fn new() -> Self {
        Self {
            abort_handles: HashMap::new(),
            tasks: JoinSet::new(),
        }
    }
}

impl Default for TaskManagerState {
    fn default() -> Self {
        Self::new()
    }
}

pub struct BuildTaskManager;

impl Actor for BuildTaskManager {
    type Message = Message;
    type State = TaskManagerState;

    async fn handle(&self, message: Self::Message, state: &mut Self::State) {
        match message {
            Message::Build { resp_chan } => {
                let handle = state.tasks.spawn(async move {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                });

                let task_id = handle.id();
                println!("Building: {}", task_id);
                // Store the handle in the build_tracker
                state.abort_handles.insert(task_id, handle);
                // Respond with the task id to allow cancellation
                resp_chan.send(task_id).unwrap();
            }
            Message::Cancel {
                build_id,
                resp_chan: msg,
            } => {
                println!("Cancelling: build: {}", build_id);
                if let Some(handle) = state.abort_handles.remove(&build_id) {
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
                res = state.tasks.join_next_with_id(), if !state.tasks.is_empty() => {
                    if let Some(Ok((build_id, _))) = res {
                        if let Some(_handle) = state.abort_handles.remove(&build_id) {
                            println!("Task completed: {:?}", build_id);
                        } else {
                            eprintln!("Task not found for cancellation: {:?}", build_id);
                        }
                    }
                    else if let Some(Err(e)) = res {
                        // Get the build_id from the error and remove it from the build_tracker
                        let build_id = e.id();
                        state.abort_handles.remove(&build_id);
                        if e.is_cancelled() {
                            eprintln!("A task was cancelled.");
                        } else {
                            eprintln!("Task failed. But the actor is still alive, honest");

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

pub struct BackgroundActorAddr(pub Addr<BuildTaskManager>);

impl From<Addr<BuildTaskManager>> for BackgroundActorAddr {
    fn from(addr: Addr<BuildTaskManager>) -> Self {
        Self(addr)
    }
}
impl BackgroundActorAddr {
    pub async fn build(&self) -> tokio::task::Id {
        let (tx, rx) = oneshot::channel();

        self.0.send(Message::Build { resp_chan: tx }).await.unwrap();

        rx.await.unwrap()
    }
    pub async fn cancel(&self, build_id: tokio::task::Id) -> bool {
        let (tx, rx) = oneshot::channel();
        self.0
            .send(Message::Cancel {
                build_id,
                resp_chan: tx,
            })
            .await
            .unwrap();

        rx.await.unwrap()
    }
}

#[tokio::test]
async fn test_background_worker() {
    let (addr, handle): (BackgroundActorAddr, _) =
        Actor::spawn(BuildTaskManager, TaskManagerState::new());

    let first_build_id = addr.build().await;
    let _second_build_id = addr.build().await;

    println!("Cancelling: {:?}", first_build_id);
    let cancel_success = addr.cancel(first_build_id).await;
    assert!(cancel_success);

    drop(addr); // Drop addr so that the actor can shut down. It will process any remaining tasks before shutting down.
    handle.await.unwrap();
}
