use log::*;
use std::net::SocketAddr;
use tonic::{Request, Response, Status};

use crate::ring::*;
use crate::node::NodeMessage;
use tokio::sync::mpsc;

#[derive(Debug)]
pub struct Handler {
    address: String,
    sender: mpsc::Sender<NodeMessage>,
}

impl Handler {
    pub fn new(address: SocketAddr, sender: mpsc::Sender<NodeMessage>) -> Self {
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
                self.sender.send(NodeMessage::Join(address)).await.unwrap();
                Ok(Response::new(()))
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
                self.sender.send(NodeMessage::SetNext(address)).await.unwrap();
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
                self.sender.send(NodeMessage::SetPrev(address)).await.unwrap();
                Ok(Response::new(()))
            }
            Err(e) => {
                error!("set_prev: failed to parse address {}: {}", address, e);
                Err(Status::invalid_argument("invalid address"))
            }
        }
    }
}
