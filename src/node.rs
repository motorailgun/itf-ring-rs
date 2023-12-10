use tokio::{sync::mpsc, task::JoinHandle};
use log::*;

use crate::ring::*;
use tonic::Request;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum NodeMessage {
    Join(String),
    Leave,
    SetNext(String),
    SetPrev(String),
}

pub struct Node {
    sender: mpsc::Sender<NodeMessage>,
    #[allow(dead_code)]
    worker: JoinHandle<()>,
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
                        debug!("Joining to ring at {}", address.clone());
                        let result = node.join(address, String::from("http")).await;
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
        });


        Node {
            sender,
            worker,
        }
    }
}

impl NodeState {
    pub async fn join(&mut self, address: String, protocol: String) -> Result<(), Box<dyn std::error::Error>> {
        let client = ring_client::RingClient::connect(format!("{}://{}", protocol, address)).await;
        match client {
            Ok(mut client) => {
                let _lock = self.lock.lock().await;
                let res = client.join(Request::new(JoinRequest {
                    address: self.address.clone(),
                })).await;

                match res {
                    Ok(res) => {
                        let JoinReply{prev, next} = res.into_inner();
                        self.prev = prev;
                        self.next = next;
                    },
                    Err(e) => return Err(Box::new(e)),
                };

                debug!("{:?}", self);
                Ok(())
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
