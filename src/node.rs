use tokio::sync::mpsc;
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

#[derive(Debug, Clone)]
pub struct Node {
    sender: mpsc::Sender<NodeMessage>,
}

#[derive(Debug)]
struct NodeState {
    lock: tokio::sync::Mutex<()>,
    address: String,
    prev: String,
    next: String,
}


impl Node {
    pub fn get_sender(&self) -> mpsc::Sender<NodeMessage> {
        self.sender.clone()
    }

    pub fn new(address: std::net::SocketAddr) -> Node {
        let address = address.to_string();
        let mut node = NodeState {
            lock: tokio::sync::Mutex::new(()),
            address: address.clone(),
            prev: address.clone(),
            next: address,
        };
        let (sender, mut reciever) = mpsc::channel::<NodeMessage>(16);

        let worker = tokio::spawn(async move {
            while let Some(message) = reciever.recv().await {
                match message {
                    NodeMessage::Join(address) => {
                        debug!("Appending new node: {}", address.clone());
                        node.join(address, String::from("http")).await;
                    },
                    NodeMessage::JoinExisting(address) => {
                        debug!("Joining to ring at {}", address.clone());
                        let result = node.join_existing(address, String::from("http")).await;
                        match result {
                            Ok(_) => {
                                debug!("Joined to ring");
                            },
                            Err(e) => {
                                warn!("Joining to ring failed: {}", e);
                            },
                        }
                    },
                    NodeMessage::Leave => {
                        println!("Leaving");
                        let _result = node.leave().await;
                        break;
                    },
                    NodeMessage::SetNext(address) => {
                        debug!("Setting next to {}", address);
                        node.set_next(address).await;
                    },
                    NodeMessage::SetPrev(address) => {
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
}

impl NodeState {
    pub async fn join(&mut self, address: String, protocol: String) {
        let _lock = self.lock.lock().await;
        let mut joinner_client = ring_client::RingClient::connect(format!("{}://{}", protocol, address.clone())).await.unwrap();
        let mut next_client = ring_client::RingClient::connect(format!("{}://{}", protocol, self.next.clone())).await.unwrap();
        let _ = next_client.set_prev(Request::new(SetPrevRequest { address: address.clone() })).await;
        let _ = joinner_client.set_next(Request::new(SetNextRequest { address: self.next.clone() })).await;
        let _ = joinner_client.set_prev(Request::new(SetPrevRequest { address: self.address.clone() })).await;

        self.next = address;
    }

    pub async fn join_existing(&mut self, address: String, protocol: String) -> Result<(), Box<dyn std::error::Error>> {
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

    pub async fn leave(&self) -> Result<(), Box<dyn std::error::Error>> {
        unimplemented!();
    }
}
