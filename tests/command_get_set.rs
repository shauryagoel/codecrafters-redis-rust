use redis::Commands;
mod utils;
use std::thread;
use std::time::Duration;

#[test]
fn test_set_get_ttl() {
    // 1. Start the server
    let mut test_server = utils::start_server_and_get_connection();
    let con = &mut test_server.connection;

    // 2. SET foo bar
    // let set_result: String = con.set("foo", "bar").unwrap();
    let set_result: String = redis::cmd("SET")
        .arg(&["foo", "bar", "px", "1000"])
        .query(con)
        .unwrap();
    assert_eq!(set_result, "OK");

    // 3. GET foo
    let val: Option<String> = con.get("foo").unwrap();
    assert_eq!(val, Some("bar".into()));

    // 3.1 Wait for ttl to expire
    thread::sleep(Duration::from_millis(1000));
    let val: Option<String> = con.get("foo").unwrap();
    assert_eq!(val, None);

    // 4. Clean up happens using the `Drop` trait of `ChildGuard`
}
