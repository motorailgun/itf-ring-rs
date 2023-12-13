use log::*;
use std::net::SocketAddr;
use tonic::{Request, Response, Status};

use crate::ring::*;
use crate::node::{Node, NodeMessage};

#[derive(Debug)]
pub struct Handler {
    address: String,
    sender: Node,
}

impl Handler {
    pub fn new(address: SocketAddr, sender: Node) -> Self {
        Handler { address: address.to_string(), sender }
    }
}

#[tonic::async_trait]
impl ring_server::Ring for Handler {
    async fn join(&self, request: Request<JoinRequest>) -> Result<Response<()>, Status> {
        info!("received join request");
        let address = request.into_inner().address;

        match address.parse::<SocketAddr>() {
            Ok(_) => {
                let res = self.sender.send_message(NodeMessage::Join(address)).await;
                match res {
                    Ok(_) => Ok(Response::new(())),
                    Err(e) => {
                        error!("join: failed to process join request: {}", e);
                        Err(Status::internal("failed to send message"))
                    }
                }
            }
            Err(e) => {
                error!("join: failed to parse address {}: {}", address, e);
                Err(Status::invalid_argument("invalid address"))
            }
        }
    }

    async fn set_next(&self, request: Request<SetNextRequest>) -> Result<Response<()>, Status> {
        info!("received set_next request");
        let address = request.into_inner().address;
        match address.parse::<SocketAddr>() {
            Ok(_) => {
                self.sender.send_message(NodeMessage::SetNext(address)).await.unwrap();
                Ok(Response::new(()))
            }
            Err(e) => {
                error!("set_next: failed to parse address {}: {}", address, e);
                Err(Status::invalid_argument("invalid address"))
            }
        }
    }

    async fn set_prev(&self, request: Request<SetPrevRequest>) -> Result<Response<()>, Status> {
        info!("received set_prev request");
        let address = request.into_inner().address;
        match address.parse::<SocketAddr>() {
            Ok(_) => {
                self.sender.send_message(NodeMessage::SetPrev(address)).await.unwrap();
                Ok(Response::new(()))
            }
            Err(e) => {
                error!("set_prev: failed to parse address {}: {}", address, e);
                Err(Status::invalid_argument("invalid address"))
            }
        }
    }

    async fn list_nodes(&self, request: Request<NodeList>) -> Result<Response<()>, Status> {
        info!("received list_nodes request");
        let mut list = request.into_inner().nodes;
        list.sort_unstable_by(|a, b| a.number.cmp(&b.number));
        
        if list.len() == 0 {
            return Err(Status::invalid_argument("empty list given"));
        } else if  list[0].address == self.address {
            debug!("list_nodes: head is self, invoking share");
            let res = self.sender.send_message(NodeMessage::ShareNodes(list)).await;
            match res {
                Ok(_) => (),
                Err(e) => {
                    error!("list_nodes: failed to process list_nodes request: {}", e);
                    return Err(Status::internal("failed to send message"));
                }
            }
        } else {
            let res = self.sender.send_message(NodeMessage::ListNodes(list)).await;
            match res {
                Ok(_) => (),
                Err(e) => {
                    error!("list_nodes: failed to process list_nodes request: {}", e);
                    return Err(Status::internal("failed to send message"));
                }
            }
        }

        Ok(Response::new(()))
    }

    async fn share_nodes(&self, request: Request<NodeList>) -> Result<Response<()>, Status> {
        info!("received share_nodes request");
        let list = request.into_inner().nodes;
        match self.sender.send_message(NodeMessage::ShareNodes(list)).await {
            Ok(_) => Ok(Response::new(())),
            Err(e) => {
                error!("share_nodes: failed to process share_nodes request: {}", e);
                Err(Status::internal("failed to send message"))
            },
        }
    }
}
