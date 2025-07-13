use core::panic;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

// A very basic parser for RESP
// Currently only handles non-nested arrays
// Returns the parsed output in human readable form
fn parse_command(input: &str) -> Vec<String> {
    let mut command_list: Vec<String> = vec![];

    let mut input_it = input.trim().chars().enumerate();
    // for (ind, char) in input_it {
    while let Some((ind, char)) = input_it.next() {
        match char {
            '*' => {
                let (ind2, _) = input_it.find(|&x| x.1 == '\r').unwrap();
                command_list.push(String::from(&input[ind..ind2]));
                input_it.next();
            }
            _ => continue,
        }
    }

    // Currently, always guaranteed to contain a value
    let _vector_length = command_list.pop().unwrap();

    // Extract only the valid strings for now
    // The previous code is useless for now, but, might become useful later on
    for string in input.trim().split("\r\n") {
        if string.starts_with(['*', '$']) {
            continue;
        }
        command_list.push(String::from(string));
    }

    command_list
}

async fn process(mut stream: TcpStream) {
    let mut buf = [0; 1024];

    while let Ok(bytes_read) = stream.read(&mut buf).await {
        if bytes_read == 0 {
            break;
        }
        let parsed_command = parse_command(std::str::from_utf8(&buf[..bytes_read]).unwrap());

        // Main Redis Server functioning
        // Redis commands are case insensitive
        let redis_output = match parsed_command[0].to_lowercase().as_str() {
            "ping" => "+PONG\r\n",
            "echo" => &format!("+{}\r\n", parsed_command[1].as_str()),
            _ => panic!("Command not supported"),
        };

        stream.write_all(redis_output.as_bytes()).await.unwrap();
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
