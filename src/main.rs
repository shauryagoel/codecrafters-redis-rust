use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

async fn process(mut stream: TcpStream) {
    let mut buf = [0; 1024];

    while let Ok(bytes_read) = stream.read(&mut buf).await {
        if bytes_read == 0 {
            break;
        }
        stream.write_all(b"+PONG\r\n").await.unwrap();
    }
}

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        match listener.accept().await {
            // The second item contains the IP and port of the new connection.
            Ok((stream, _)) => {
                // A new task is spawned for each inbound socket. The socket is
                // moved to the new task and processed there.
                tokio::spawn(async move {
                    process(stream).await;
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
