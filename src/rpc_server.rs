use log::*;
use std::error::Error;
use std::net::{AddrParseError, SocketAddr};
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use crate::ring::*;


#[derive(Debug)]
pub struct Handler {
    address: String,
    prev: Mutex<String>,
    next: Mutex<String>,
}

impl Handler {
    pub fn new(address: String) -> Result<Self, AddrParseError> {
        // assume legit address
        let res = address.parse::<SocketAddr>();
        match res {
            Ok(_) => Ok(Handler {
                address: address.clone(),
                prev: Mutex::new(address.clone()),
                next: Mutex::new(address),
            }),
            Err(e) => Err(e),
        }
    }

    pub async fn join_ex(&mut self, address: &SocketAddr) -> Result<(), Box<dyn Error>> {
        let client = ring_client::RingClient::connect(format!("http://{}", address.to_string())).await;
        match client {
            Ok(mut client) => {
                let res = client.join(Request::new(JoinRequest {
                    address: self.address.clone(),
                })).await;

                match res {
                    Ok(res) => {
                        let JoinReply{prev, next} = res.into_inner();
                        let mut my_prev = self.prev.lock().await;
                        let mut my_next = self.next.lock().await;
                        *my_prev = prev;
                        *my_next = next;
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

    pub async fn leave(&self) -> Result<(), Box<dyn Error>> {
        unimplemented!();
    }
}

#[tonic::async_trait]
impl ring_server::Ring for Handler {
    async fn join(&self, request: Request<JoinRequest>) -> Result<Response<JoinReply>, Status> {
        info!("received join request");
        let address = request.into_inner().address;

        match address.parse::<SocketAddr>() {
            Ok(_) => {
                let old_next = self.next.lock().await.clone();
                let mut next_client = ring_client::RingClient::connect(format!("http://{}", old_next.clone())).await.unwrap();
                let _ = next_client.set_prev(Request::new(SetPrevRequest { address: address.clone() })).await;
                let mut my_next = self.next.lock().await;
                *my_next = address;

                Ok(Response::new(JoinReply {
                    prev: self.address.clone(),
                    next: old_next,
                }))
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
                let mut my_next = self.next.lock().await;
                *my_next = address;
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
                let mut my_prev = self.prev.lock().await;
                *my_prev = address;
                Ok(Response::new(()))
            }
            Err(e) => {
                error!("set_prev: failed to parse address {}: {}", address, e);
                Err(Status::invalid_argument("invalid address"))
            }
        }
    }
}
