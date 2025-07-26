use redis::Commands;
use std::env;
use std::net::TcpListener;
use std::process::{Child, Command};
use std::thread;
use std::time::Duration;

struct ChildGuard(Child);

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

fn find_free_tcp_port() -> u16 {
    let socket = TcpListener::bind("127.0.0.1:0").unwrap();
    socket.local_addr().unwrap().port()
}

// Start the redis server on the specified port
fn start_server(port: &str) -> ChildGuard {
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
    thread::sleep(Duration::from_millis(500));
    ChildGuard(child)
}

#[test]
fn test_ping() {
    let port = &find_free_tcp_port().to_string();
    let _server = start_server(port);
    let client = redis::Client::open(format!("redis://127.0.0.1:{port}/")).unwrap();
    let mut con = client.get_connection().unwrap();

    let ping_result: String = con.ping().unwrap();
    assert_eq!(ping_result, "PONG");
}

#[test]
fn test_echo() {
    let port = &find_free_tcp_port().to_string();
    let _server = start_server(port);
    let client = redis::Client::open(format!("redis://127.0.0.1:{port}/")).unwrap();
    let mut con = client.get_connection().unwrap();

    let message = "hey";
    let echo_result: String = redis::cmd("ECHO").arg(message).query(&mut con).unwrap();
    assert_eq!(echo_result, message);
}

#[test]
fn test_set_get_ttl() {
    // 1. Start the server
    let port = &find_free_tcp_port().to_string();
    // Need to assign it to a variable to ensure it is not dropped
    let _server = start_server(port);

    // 2. Connect a Redis client
    let client = redis::Client::open(format!("redis://127.0.0.1:{port}/")).unwrap();
    let mut con = client.get_connection().unwrap();

    // 3. SET foo bar
    // let set_result: String = con.set("foo", "bar").unwrap();
    let set_result: String = redis::cmd("SET")
        .arg(&["foo", "bar", "px", "1000"])
        .query(&mut con)
        .unwrap();
    assert_eq!(set_result, "OK");

    // 4. GET foo
    let val: Option<String> = con.get("foo").unwrap();
    assert_eq!(val, Some("bar".into()));

    // 4.1 Wait for ttl to expire
    thread::sleep(Duration::from_millis(1000));
    let val: Option<String> = con.get("foo").unwrap();
    assert_eq!(val, None);

    // 5. Clean up happens using the `Drop` trait of `ChildGuard`
}

#[test]
fn test_array() {
    let port = &find_free_tcp_port().to_string();
    let _server = start_server(port);
    let client = redis::Client::open(format!("redis://127.0.0.1:{port}/")).unwrap();
    let mut con = client.get_connection().unwrap();

    // (R)push the data
    let rpush_result: usize = con.rpush("list", "foo").unwrap();
    assert_eq!(rpush_result, 1);
    let rpush_result: usize = con.rpush("list", &["bar", "baz"]).unwrap();
    assert_eq!(rpush_result, 3);
    let rpush_result: usize = con.rpush("list", &["foo", "bar", "baz"]).unwrap();
    assert_eq!(rpush_result, 6);

    // Fetch the data (for +ve indices)
    let lrange_result: Vec<String> = con.lrange("list", 0, 1).unwrap();
    assert_eq!(lrange_result, vec!["foo".to_string(), "bar".to_string()]);
    let lrange_result: Vec<String> = con.lrange("list", 4, 4).unwrap();
    assert_eq!(lrange_result, vec!["bar".to_string()]);
    // Edge cases-
    // Non-existent list
    let lrange_result: Vec<String> = con.lrange("non_existent_list", 4, 4).unwrap();
    assert_eq!(lrange_result, Vec::<String>::new());
    // start index > list length
    let lrange_result: Vec<String> = con.lrange("list", 8, 4).unwrap();
    assert_eq!(lrange_result, Vec::<String>::new());
    // stop index > list length
    let lrange_result: Vec<String> = con.lrange("list", 4, 8).unwrap();
    assert_eq!(lrange_result, vec!["bar".to_string(), "baz".to_string()]);
    // start index > stop index
    let lrange_result: Vec<String> = con.lrange("list", 2, 0).unwrap();
    assert_eq!(lrange_result, Vec::<String>::new());

    // Fetch the data (for -ve indices)
    let lrange_result: Vec<String> = con.lrange("list", -2, -1).unwrap();
    assert_eq!(lrange_result, vec!["bar".to_string(), "baz".to_string()]);
    let lrange_result: Vec<String> = con.lrange("list", 0, -5).unwrap();
    assert_eq!(lrange_result, vec!["foo".to_string(), "bar".to_string()]);
}
