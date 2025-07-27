use redis::Commands;

mod utils;

#[test]
fn test_ping() {
    let mut test_server = utils::start_server_and_get_connection();
    let con = &mut test_server.connection;

    let ping_result: String = con.ping().unwrap();
    assert_eq!(ping_result, "PONG");
}
