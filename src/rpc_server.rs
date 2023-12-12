use log::*;
use std::net::SocketAddr;
use tonic::{Request, Response, Status};

use crate::ring::*;
use crate::node::{Node, NodeMessage, LeaveNodeMessage};

#[derive(Debug)]
pub struct Handler {
    _address: String,
    sender: Node,
}

impl Handler {
    pub fn new(address: SocketAddr, sender: Node) -> Self {
        Handler { _address: address.to_string(), sender }
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

    async fn leave(&self, request: Request<LeaveRequest>) -> Result<Response<()>, Status> {
        info!("received leave request");
        let message = request.into_inner();
        let leave_message = LeaveNodeMessage{leaver: message.self_address, next: message.next_address};
        let both_parsable = leave_message.leaver.parse::<SocketAddr>().is_ok() && leave_message.next.parse::<SocketAddr>().is_ok();

        if both_parsable {
            let leave_res = self.sender.send_message(NodeMessage::LeaveNode(leave_message)).await;
            match leave_res {
                Ok(_) => Ok(Response::new(())),
                Err(e) => {
                    error!("leave: send_message: {}", &e);
                    Err(Status::internal("failed to send message"))
                }
            }
        } else {
            error!("leave: address parse error: imvalid addres(s): {:?}", leave_message);
            Err(Status::invalid_argument("invalid address(s)"))
        }
    }
}
