use tokio::{sync::mpsc, task::JoinHandle};
use log::*;

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
struct NodeInfo {
    #[allow(dead_code)]
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
        let mut node = NodeInfo {
            address: address.clone(),
            prev: address.clone(),
            next: address,
        };
        let (sender, mut reciever) = mpsc::channel::<NodeMessage>(16);

        let worker = tokio::spawn(async move {
            while let Some(message) = reciever.recv().await {
                match message {
                    NodeMessage::Join(_address) => {
                        println!("Joining");
                    },
                    NodeMessage::Leave => {
                        println!("Leaving");
                    },
                    NodeMessage::SetNext(address) => {
                        debug!("Setting next to {}", address);
                        node.next = address;
                    },
                    NodeMessage::SetPrev(address) => {
                        debug!("Setting prev to {}", address);
                        node.prev = address;
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
