use log::{debug, info};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::str;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();

    let mut db: HashMap<String, String> = HashMap::new();

    let socket = UdpSocket::bind("0.0.0.0:8080".parse::<SocketAddr>().unwrap()).await?;
    info!("Listening on port 8080");
    let r = Arc::new(socket);
    let s = r.clone();
    let (tx, mut rx) = mpsc::channel::<(Vec<u8>, SocketAddr)>(1000);

    tokio::spawn(async move {
        while let Some((bytes, addr)) = rx.recv().await {
            let len = s.send_to(&bytes, &addr).await.unwrap();
            debug!("{:?} bytes sent", len);
        }
    });

    let mut buf = [0; 1024];
    loop {
        let (len, addr) = r.recv_from(&mut buf).await?;
        debug!("{:?} bytes received from {:?}", len, addr);
        let request = str::from_utf8(&buf[..len]).unwrap().to_string();
        let request = request.trim();
        if request.contains('=') {
            debug!("Received insert: {}", request);
            let parts: Vec<&str> = request.trim().split('=').collect();
            debug!("parts: {:?}", parts);
            db.insert(parts[0].to_string(), parts[1].to_string());
            debug!("db: {:?}", db);
        } else if request == "version" {
            debug!("get version");
            let msg = String::from("version=Dumb Key-Value Store 1.0\n");
            tx.send((msg.into_bytes(), addr)).await.unwrap();
        } else {
            debug!("Received retrieve: {}", request);
            let value = db.get(request);
            if value.is_some() {
                let msg = format!("{}={}\n", request, value.unwrap());
                tx.send((msg.into_bytes(), addr)).await.unwrap();
            }
        }
    }
}
