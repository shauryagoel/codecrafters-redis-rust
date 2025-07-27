use std::{
    env,
    net::TcpListener,
    process::{Child, Command},
    thread,
    time::Duration,
};

pub struct ChildGuard(Child);

impl Drop for ChildGuard {
    // Uses RAII to delete the child process when the guard goes out of scope,
    // regardeless of whether the scope is exited normally or due to a panic
    fn drop(&mut self) {
        match self.0.kill() {
            Err(e) => eprintln!("Could not kill child process: {}", e),
            // Ok(_) => println!("Successfully killed child process"),
            Ok(_) => (),
        }
    }
}

// Find a free port on the machine
pub fn find_free_tcp_port() -> u16 {
    let socket = TcpListener::bind("127.0.0.1:0").unwrap();
    socket.local_addr().unwrap().port()
}

// Start the redis server on the specified port
pub fn start_server(port: &str) -> ChildGuard {
    let binary_path = env::var("CARGO_MANIFEST_DIR").unwrap()
        + "/target/debug/"
        + env::var("CARGO_PKG_NAME").unwrap().as_ref();
    // println!("{binary_path}");

    // Adjust path if your binary is named differently
    let child = Command::new(binary_path)
        .arg(port)
        // .stdout(std::process::Stdio::null())
        .spawn()
        .expect("Failed to spawn server");

    // Give the server a moment to bind to port
    thread::sleep(Duration::from_millis(1000));
    ChildGuard(child)
}

// This struct is needed to hold the server object so that the `Drop` trait doesn't get called
#[allow(dead_code)]
pub struct TestServer {
    server: ChildGuard,
    pub connection: redis::Connection,
}

pub fn start_server_and_get_connection() -> TestServer {
    let port = &find_free_tcp_port().to_string();
    let server = start_server(port);
    let client = redis::Client::open(format!("redis://127.0.0.1:{port}/")).unwrap();

    TestServer {
        server,
        connection: client.get_connection().unwrap(),
    }
}
