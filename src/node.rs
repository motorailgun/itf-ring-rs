use std::{error::Error, time::Duration};
use tokio::sync::{mpsc, oneshot};
use log::*;

use crate::ring::*;
use tonic::Request;
use rand::Rng;

#[derive(Debug, Clone, PartialEq)]
pub enum NodeMessage {
    Join(String),
    JoinExisting(String),
    Leave,
    SetNext(String),
    SetPrev(String),
    ListNodes(Vec<NodeInfo>),
    ShareNodes(Vec<NodeInfo>)
}

#[derive(Debug)]
enum WorkerMessage {
    Join(String, oneshot::Sender<Result<(), Box<dyn Error + Send + Sync>>>),
    JoinExisting(String, oneshot::Sender<Result<(), Box<dyn Error + Send + Sync>>>),
    Leave(oneshot::Sender<Result<(), Box<dyn Error + Send + Sync>>>),
    SetNext(String),
    SetPrev(String),
    ListNodes(Vec<NodeInfo>, oneshot::Sender<Result<(), Box<dyn Error + Send + Sync>>>),
    ShareNodes(Vec<NodeInfo>, oneshot::Sender<Result<(), Box<dyn Error + Send + Sync>>>),
}

unsafe impl Send for WorkerMessage {}
unsafe impl Sync for WorkerMessage {}

#[derive(Debug, Clone)]
pub struct Node {
    sender: mpsc::Sender<WorkerMessage>,
}

#[derive(Debug, Clone)]
struct NodeInternal {
    address: String,
    prev: String,
    next: String,
    is_leader: bool,
    last_heartbeat: std::time::Instant,
}

#[derive(Debug, Clone)]
struct NodeState {
    lock: std::sync::Arc<tokio::sync::Mutex<NodeInternal>>,
}


impl Node {
    pub fn new(address: std::net::SocketAddr) -> (Node, Vec<tokio::task::JoinHandle<()>>) {
        let address = address.to_string();
        let mut node = NodeState {
            lock: std::sync::Arc::new(tokio::sync::Mutex::new(
                NodeInternal {
                    address: address.clone(),
                    prev: address.clone(),
                    next: address,
                    is_leader: false,
                    last_heartbeat: std::time::Instant::now(),
                }
            )),
        };

        let (sender, mut reciever) = mpsc::channel::<WorkerMessage>(8);
        let node2 = node.clone();

        let worker = tokio::spawn(async move {
            while let Some(message) = reciever.recv().await {
                match message {
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
                    WorkerMessage::ListNodes(list, tx) => {
                        debug!("Listing nodes");
                        let _ = tx.send(node.list_nodes(list, "http".into()).await);
                    },
                    WorkerMessage::ShareNodes(list, tx) => {
                        debug!("Sharing nodes");
                        let _ = tx.send(node.share_nodes(list, "http".into()).await);
                    }
                };

                node.show_state().await;
            }

            reciever.close();
        });

        let wait_time = Duration::from_millis(rand::thread_rng().gen_range(100..900) + 1000);
        let heartbeat = tokio::spawn(async move {
            loop {
                tokio::time::sleep(wait_time).await;
                let mut node = node2.lock.lock().await;

                if node.is_leader {
                    let mut list = Vec::new();
                    list.push(NodeInfo { number: 0, address: node.address.clone() });
                    let mut client = ring_client::RingClient::connect(format!("http://{}", node.address.clone())).await.unwrap();
                    let _ = client.list_nodes(Request::new(NodeList { nodes: list })).await;
                } else { 
                    let now = std::time::Instant::now();
                    if now.duration_since(node.last_heartbeat).as_millis() > 3000 {
                        info!("Heartbeat timeout, becoming leader");
                        node.is_leader = true;
                    }
                }
            }
        });

        (Node {
            sender,
        }, vec![worker, heartbeat])
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
                check_rx().await
            },
            NodeMessage::SetNext(address) => {
                self.sender.send(WorkerMessage::SetNext(address)).await.map_err(|e| Box::new(e).into())
            },
            NodeMessage::SetPrev(address) => {
                self.sender.send(WorkerMessage::SetPrev(address)).await.map_err(|e| Box::new(e).into())
            },
            NodeMessage::ListNodes(list) => {
                self.sender.send(WorkerMessage::ListNodes(list, tx)).await?;
                check_rx().await
            },
            NodeMessage::ShareNodes(list) => {
                self.sender.send(WorkerMessage::ShareNodes(list, tx)).await.map_err(|e| Box::new(e).into())
            },
        }
    }
}

impl NodeState {
    pub async fn join(&mut self, address: String, protocol: String) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut lock = self.lock.lock().await;
        debug!("join message: {}", &address);
        let mut joinner_client = ring_client::RingClient::connect(format!("{}://{}", protocol, &address)).await?;
        let mut next_client = ring_client::RingClient::connect(format!("{}://{}", protocol, &lock.next)).await?;
        let _ = next_client.set_prev(Request::new(SetPrevRequest { address: address.clone() })).await;
        let _ = joinner_client.set_next(Request::new(SetNextRequest { address: lock.next.clone() })).await;
        let _ = joinner_client.set_prev(Request::new(SetPrevRequest { address: lock.address.clone() })).await;

        lock.next = address;
        Ok(())
    }

    pub async fn join_existing(&mut self, address: String, protocol: String) -> Result<(), Box<dyn Error + Send + Sync>> {
        debug!("join_existing message: {}", &address);
        let mut client = ring_client::RingClient::connect(format!("{}://{}", protocol, address)).await?;
        let lock = self.lock.lock().await;
        client.join(Request::new(JoinRequest {address: lock.address.clone() })).await?;
        Ok(())
    }

    pub async fn set_next(&mut self, address: String) {
        let mut  lock = self.lock.lock().await;
        debug!("set_next message: {}", &address);
        lock.next = address;
    }

    pub async fn set_prev(&mut self, address: String) {
        let mut lock = self.lock.lock().await;
        debug!("set_prev message: {}", &address);
        lock.prev = address;
    }

    pub async fn leave(&self, protocol: String) -> Result<(), Box<dyn Error + Send + Sync>> {
        let lock = self.lock.lock().await;
        debug!("leave message");
        
        let mut next_client = ring_client::RingClient::connect(format!("{}://{}", protocol, &lock.next)).await?;
        let mut prev_client = ring_client::RingClient::connect(format!("{}://{}", protocol, &lock.prev)).await?;

        let _ = next_client.set_prev(Request::new(SetPrevRequest { address: lock.prev.clone() })).await?;
        let _ = prev_client.set_next(Request::new(SetNextRequest { address: lock.next.clone() })).await?;

        Ok(())
    }

    pub async fn show_state(&self) {
        let lock = self.lock.lock().await;
        debug!("Node state:");
        debug!("  address: {}", &lock.address);
        debug!("  prev: {}", &lock.prev);
        debug!("  next: {}", &lock.next);
        debug!("  is_leader: {}", &lock.is_leader);
    }

    pub async fn list_nodes(&self, mut list: Vec<NodeInfo>, protocol: String) -> Result<(), Box<dyn Error + Send + Sync>> {
        let lock = self.lock.lock().await;
        debug!("list_nodes message");

        list.push(NodeInfo { number: list.first().unwrap().number + 1, address: lock.address.clone() });
        let mut client = ring_client::RingClient::connect(format!("{}://{}", protocol, &lock.next)).await?;
        let _ = client.list_nodes(Request::new(NodeList { nodes: list })).await?;
        Ok(())
    }

    pub async fn share_nodes(&self, list: Vec<NodeInfo>, protocol: String) -> Result<(), Box<dyn Error + Send + Sync>> {
        debug!("share_nodes message: {:?}", &list);

        if list.len() == 0 {
            return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "empty list given")) as Box<dyn Error + Send + Sync>);
        }

        let mut list = list;
        list.sort_unstable_by(|a, b| a.number.cmp(&b.number));
        let first = list.first().unwrap().address.clone();

        let mut lock = self.lock.lock().await;
        lock.last_heartbeat = std::time::Instant::now();
    
        if first == lock.address {
            debug!("share_nodes: head is self, I AM LEADER");
            lock.is_leader = true;
        } else {
            debug!("share_nodes: becoming follower");
            lock.is_leader = false;

            let last = list.last().unwrap().address.clone();
            if last != lock.address {
                let address = format!("{}://{}", protocol, &lock.next);
                tokio::spawn(async move {
                    let client = ring_client::RingClient::connect(address).await;
                    if client.is_err() {
                        return;
                    }
                    let mut client = client.unwrap();
                    let _ = client.share_nodes(Request::new(NodeList { nodes: list })).await;
                });
            }
        }

        Ok(())
    }
}
