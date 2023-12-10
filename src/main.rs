pub mod ring_node;

use std::net::SocketAddr;

use log::info;
use ring_node::RingNode;
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
    let my_address: String = format!("127.0.0.1:{}", my_port);
    let mut node = RingNode::new(my_address.clone()).unwrap();

    match join_address() {
        Ok(address) => {
            info!("my address: {}", my_address);
            info!("joining ring {}", address);
            node.join_ex(&address.parse().unwrap()).await?;
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

    tonic::transport::Server::builder()
        .add_service(ring::ring_server::RingServer::new(node))
        .serve(my_address.parse().unwrap())
        .await?;

    Ok(())
}
