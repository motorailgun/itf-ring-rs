pub mod rpc_server;
pub mod node;

use std::net::SocketAddr;

use log::info;
use rpc_server::Handler;
use rand::Rng;
use std::error::Error;

pub mod ring {
    tonic::include_proto!("ring");
}

enum GetAddressError {
    NoAddress,
    InvalidAddress(String),
}

fn join_address() -> Result<String, GetAddressError> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() >= 2 {
        let s = args[1].clone();
        if s.parse::<SocketAddr>().is_ok() {
            Ok(s)
        } else {
            Err(GetAddressError::InvalidAddress(s))
        }
    } else {
        Err(GetAddressError::NoAddress)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    let my_port: u16 = rand::thread_rng().gen_range(10000..(2u32.pow(16) - 1).try_into()?);
    let my_address: SocketAddr = format!("127.0.0.1:{}", my_port).parse().unwrap();
    let (node, joinhandlers) = node::Node::new(my_address);
    let handler = Handler::new(my_address, node.clone());
    let server = tokio::spawn( async move { 
        tonic::transport::Server::builder()
            .add_service(ring::ring_server::RingServer::new(handler))
            .serve(my_address).await.unwrap();
    });

    match join_address() {
        Ok(address) => {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await; // ensure gRPC server to start
            info!("my address: {}", my_address);
            info!("joining ring {}", address);
            node.send_message(node::NodeMessage::JoinExisting(address)).await?;
        },
        Err(e) => {
            match e {
                GetAddressError::NoAddress => {
                    info!("no address provided, starting new ring");
                    info!("my address: {}", my_address)
                },
                GetAddressError::InvalidAddress(addr) => {
                    panic!("invalid address {}", addr);
                },
            }
        }
    };


    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigint = signal(SignalKind::interrupt())?;
        let _ = sigint.recv().await;
        tokio::task::spawn( async move { node.send_message(node::NodeMessage::Leave).await.expect("failed to leave from node"); } ).await?;
        joinhandlers.into_iter().for_each(|h| h.abort());
        server.abort();
    };
    
    Ok(())
}
