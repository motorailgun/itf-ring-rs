use log::*;
use std::error::Error;
use std::net::{AddrParseError, SocketAddr};
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use crate::ring::*;


#[derive(Debug)]
pub struct RingNode {
    address: String,
    prev: Mutex<String>,
    next: Mutex<String>,
}

impl RingNode {
    pub fn new(address: String) -> Result<Self, AddrParseError> {
        // assume legit address
        let res = address.parse::<SocketAddr>();
        match res {
            Ok(_) => Ok(RingNode {
                address: address.clone(),
                prev: Mutex::new(address.clone()),
                next: Mutex::new(address),
            }),
            Err(e) => Err(e),
        }
    }

    pub async fn join(&self, address: &SocketAddr) -> Result<(), Box<dyn Error>> {
        let client = ring_client::RingClient::connect(address.to_string()).await;
        match client {
            Ok(mut client) => {
                let res = client.join(Request::new(JoinRequest {
                    address: self.address.clone(),
                })).await;

                match res {
                    Err(e) => return Err(Box::new(e)),
                    _ => (),
                };
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
impl ring_server::Ring for RingNode {
    async fn join(&self, request: Request<JoinRequest>) -> Result<Response<()>, Status> {
        info!("received join request");
        let address = request.into_inner().address;

        match address.parse::<SocketAddr>() {
            Ok(_) => {
                let my_next = self.next.lock().await.clone();
                let mut next_client = ring_client::RingClient::connect(my_next.clone()).await.unwrap();
                let mut joinner_client = ring_client::RingClient::connect(address.clone()).await.unwrap();
                let _ = next_client.set_prev(Request::new(SetPrevRequest { address: address.clone() })).await;
                let _ = joinner_client.set_prev(Request::new(SetPrevRequest { address: self.address.clone() })).await;
                let _ = joinner_client.set_next(Request::new(SetNextRequest { address: my_next.clone() })).await;
                let mut my_next = self.next.lock().await;
                *my_next = address;

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
