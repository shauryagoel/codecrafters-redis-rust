mod utils;

#[test]
fn test_echo() {
    let mut test_server = utils::start_server_and_get_connection();
    let con = &mut test_server.connection;

    let message = "hey";
    let echo_result: String = redis::cmd("ECHO").arg(message).query(con).unwrap();
    assert_eq!(echo_result, message);
}
