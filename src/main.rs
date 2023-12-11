pub mod rpc_server;
pub mod node;

use std::net::SocketAddr;

use log::info;
use rpc_server::Handler;
use rand::Rng;

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
        if let Ok(_) = s.parse::<SocketAddr>() {
            Ok(s)
        } else {
            Err(GetAddressError::InvalidAddress(s))
        }
    } else {
        Err(GetAddressError::NoAddress)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let my_port: u16 = rand::thread_rng().gen_range(10000..(2u32.pow(16) - 1).try_into()?);
    let my_address: SocketAddr = format!("127.0.0.1:{}", my_port).parse().unwrap();
    let node = node::Node::new(my_address.clone());
    let handler = Handler::new(my_address.clone(), node.clone());
    let server = tonic::transport::Server::builder()
        .add_service(ring::ring_server::RingServer::new(handler))
        .serve(my_address);


    match join_address() {
        Ok(address) => {
            info!("my address: {}", my_address);
            info!("joining ring {}", address);
            node.send_message(node::NodeMessage::JoinExisting(address)).await.unwrap();
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

    
    server.await?;
    Ok(())
}
