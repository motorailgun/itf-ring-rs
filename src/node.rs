use std::error::Error;
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
    Join(String, oneshot::Sender<Result<(), Box<dyn Error + Send + Sync>>>),
    JoinExisting(String, oneshot::Sender<Result<(), Box<dyn Error + Send + Sync>>>),
    Leave(oneshot::Sender<Result<(), Box<dyn Error + Send + Sync>>>),
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
    // should wrap all members by Mutex, but too lazy to do it
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
                let res = match message {
                    WorkerMessage::Join(address, tx) => {
                        debug!("Appending new node: {}", &address);
                        let _ = tx.send(node.join(address, String::from("http")).await);
                    },
                    WorkerMessage::JoinExisting(address, tx) => {
                        debug!("Joining to ring at {}", &address);
                        let result = node.join_existing(address, "http".into()).await;
                        match result {
                            Ok(_) => {
                                debug!("Joined to ring");
                            },
                            Err(ref e) => {
                                warn!("Joining to ring failed: {}", e);
                            },
                        }

                        let _ = tx.send(result);
                    },
                    WorkerMessage::Leave(tx) => {
                        info!("Leaving");
                        let result = node.leave("http".into()).await;
                        let _ = tx.send(result);
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
                };

                node.show_state().await;
                res
            }

            reciever.close();
        });


        Node {
            sender,
        }
    }

    pub async fn send_message(&self, message: NodeMessage) -> Result<(), Box<dyn Error>> {
        let (tx, rx) = oneshot::channel::<Result<(), Box<dyn Error + Send + Sync>>>();
        let check_rx = || async move {
            match rx.await {
                Ok(p) => p.map_err(|e| e as Box<dyn Error>),
                _ => Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "failed to receive channel output")) as Box<dyn Error>),
            }
        };


        match message {
            NodeMessage::Join(address) => {
                self.sender.send(WorkerMessage::Join(address, tx)).await?;
                check_rx().await
            },
            NodeMessage::JoinExisting(address) => {
                self.sender.send(WorkerMessage::JoinExisting(address, tx)).await?;
                check_rx().await
            },
            NodeMessage::Leave => {
                self.sender.send(WorkerMessage::Leave(tx)).await?;
                check_rx().await as Result<(), Box<dyn Error>>
            },
            NodeMessage::SetNext(address) => {
                self.sender.send(WorkerMessage::SetNext(address)).await.map_err(|e| Box::new(e).into())
            },
            NodeMessage::SetPrev(address) => {
                self.sender.send(WorkerMessage::SetPrev(address)).await.map_err(|e| Box::new(e).into())
            },
        }
    }
}

impl NodeState {
    pub async fn join(&mut self, address: String, protocol: String) -> Result<(), Box<dyn Error + Send + Sync>> {
        let _lock = self.lock.lock().await;
        debug!("join message: {}", &address);
        let mut joinner_client = ring_client::RingClient::connect(format!("{}://{}", protocol, address.clone())).await?;
        let mut next_client = ring_client::RingClient::connect(format!("{}://{}", protocol, self.next.clone())).await?;
        let _ = next_client.set_prev(Request::new(SetPrevRequest { address: address.clone() })).await;
        let _ = joinner_client.set_next(Request::new(SetNextRequest { address: self.next.clone() })).await;
        let _ = joinner_client.set_prev(Request::new(SetPrevRequest { address: self.address.clone() })).await;

        self.next = address;
        Ok(())
    }

    pub async fn join_existing(&mut self, address: String, protocol: String) -> Result<(), Box<dyn Error + Send + Sync>> {
        debug!("join_existing message: {}", &address);
        let mut client = ring_client::RingClient::connect(format!("{}://{}", protocol, address)).await?;
        let _lock = self.lock.lock().await;
        client.join(Request::new(JoinRequest {address: self.address.clone() })).await?;
        Ok(())
    }

    pub async fn set_next(&mut self, address: String) {
        let _lock = self.lock.lock().await;
        debug!("set_next message: {}", &address);
        self.next = address;
    }

    pub async fn set_prev(&mut self, address: String) {
        let _lock = self.lock.lock().await;
        debug!("set_prev message: {}", &address);
        self.prev = address;
    }

    pub async fn leave(&self, protocol: String) -> Result<(), Box<dyn Error + Send + Sync>> {
        let _lock = self.lock.lock().await;
        debug!("leave message");
        
        let mut next_client = ring_client::RingClient::connect(format!("{}://{}", protocol, self.next.clone())).await?;
        let mut prev_client = ring_client::RingClient::connect(format!("{}://{}", protocol, self.prev.clone())).await?;

        let _ = next_client.set_prev(Request::new(SetPrevRequest { address: self.prev.clone() })).await?;
        let _ = prev_client.set_next(Request::new(SetNextRequest { address: self.next.clone() })).await?;

        Ok(())
    }

    pub async fn show_state(&self) {
        let _lock = self.lock.lock().await;
        debug!("Node state:");
        debug!("  address: {}", &self.address);
        debug!("  prev: {}", &self.prev);
        debug!("  next: {}", &self.next);
    }
}
