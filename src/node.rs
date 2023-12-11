use std::{error::Error, sync::Arc};
use tokio::sync::{mpsc, oneshot};
use log::*;

use crate::ring::*;
use tonic::Request;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum NodeMessage {
    Join(String),
    JoinExisting(String),
    Leave,
    SetNext(String),
    SetPrev(String),
}

#[derive(Debug)]
enum WorkerMessage {
    Join(String, oneshot::Sender<Arc<Result<(), Box<dyn Error + Send + Sync>>>>),
    JoinExisting(String, oneshot::Sender<Arc<Result<(), Box<dyn Error + Send + Sync>>>>),
    Leave(oneshot::Sender<Arc<Result<(), Box<dyn Error + Send + Sync>>>>),
    SetNext(String),
    SetPrev(String),
}

unsafe impl Send for WorkerMessage {}
unsafe impl Sync for WorkerMessage {}

#[derive(Debug, Clone)]
pub struct Node {
    sender: mpsc::Sender<WorkerMessage>,
}

#[derive(Debug)]
struct NodeState {
    lock: tokio::sync::Mutex<()>,
    address: String,
    prev: String,
    next: String,
}


impl Node {
    pub fn new(address: std::net::SocketAddr) -> Node {
        let address = address.to_string();
        let mut node = NodeState {
            lock: tokio::sync::Mutex::new(()),
            address: address.clone(),
            prev: address.clone(),
            next: address,
        };
        let (sender, mut reciever) = mpsc::channel::<WorkerMessage>(8);

        let _worker = tokio::spawn(async move {
            while let Some(message) = reciever.recv().await {
                match message {
                    WorkerMessage::Join(address, tx) => {
                        debug!("Appending new node: {}", address.clone());
                        let _ = tx.send(Arc::new(node.join(address, String::from("http")).await));
                    },
                    WorkerMessage::JoinExisting(address, tx) => {
                        debug!("Joining to ring at {}", address.clone());
                        let result = node.join_existing(address, String::from("http")).await;
                        match result {
                            Ok(_) => {
                                debug!("Joined to ring");
                            },
                            Err(ref e) => {
                                warn!("Joining to ring failed: {}", e);
                            },
                        }

                        let _ = tx.send(Arc::new(result));
                    },
                    WorkerMessage::Leave(tx) => {
                        println!("Leaving");
                        let result = node.leave().await;
                        let _ = tx.send(Arc::new(result));
                        break;
                    },
                    WorkerMessage::SetNext(address) => {
                        debug!("Setting next to {}", address);
                        node.set_next(address).await;
                    },
                    WorkerMessage::SetPrev(address) => {
                        debug!("Setting prev to {}", address);
                        node.set_prev(address).await;
                    },
                }
            }

            reciever.close();
        });


        Node {
            sender,
        }
    }

    pub async fn send_message(&self, message: NodeMessage) -> Result<(), Box<dyn Error>> {
        let (tx, rx) = oneshot::channel::<Arc<Result<(), Box<dyn Error + Send + Sync>>>>();
        match message {
            NodeMessage::Join(address) => {
                self.sender.send(WorkerMessage::Join(address, tx)).await?;
            },
            NodeMessage::JoinExisting(address) => {
                self.sender.send(WorkerMessage::JoinExisting(address, tx)).await?;
            },
            NodeMessage::Leave => {
                self.sender.send(WorkerMessage::Leave(tx)).await?;
            },
            NodeMessage::SetNext(address) => {
                self.sender.send(WorkerMessage::SetNext(address)).await?;
            },
            NodeMessage::SetPrev(address) => {
                self.sender.send(WorkerMessage::SetPrev(address)).await?;
            },
        };

        match Arc::try_unwrap(rx.await?) {
            Ok(Ok(())) => Ok(()),
            _ => Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "failed to join"))),
        }
    }
}

impl NodeState {
    pub async fn join(&mut self, address: String, protocol: String) -> Result<(), Box<dyn Error + Send + Sync>> {
        let _lock = self.lock.lock().await;
        let mut joinner_client = ring_client::RingClient::connect(format!("{}://{}", protocol, address.clone())).await.unwrap();
        let mut next_client = ring_client::RingClient::connect(format!("{}://{}", protocol, self.next.clone())).await.unwrap();
        let _ = next_client.set_prev(Request::new(SetPrevRequest { address: address.clone() })).await;
        let _ = joinner_client.set_next(Request::new(SetNextRequest { address: self.next.clone() })).await;
        let _ = joinner_client.set_prev(Request::new(SetPrevRequest { address: self.address.clone() })).await;

        self.next = address;
        Ok(())
    }

    pub async fn join_existing(&mut self, address: String, protocol: String) -> Result<(), Box<dyn Error + Send + Sync>> {
        let client = ring_client::RingClient::connect(format!("{}://{}", protocol, address)).await;
        match client {
            Ok(mut client) => {
                let _lock = self.lock.lock().await;
                let res = client.join(Request::new(JoinRequest {
                    address: self.address.clone(),
                })).await;

                match res {
                    Ok(_) => Ok(()),
                    Err(e) => Err(Box::new(e)),
                }
            },
            Err(e) => {
                warn!("join: failed to connect {}, {}", address, e);
                Err(Box::new(e))
            }
        }
    }

    pub async fn set_next(&mut self, address: String) {
        let _lock = self.lock.lock().await;
        self.next = address;
    }

    pub async fn set_prev(&mut self, address: String) {
        let _lock = self.lock.lock().await;
        self.prev = address;
    }

    pub async fn leave(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        unimplemented!();
    }
}
