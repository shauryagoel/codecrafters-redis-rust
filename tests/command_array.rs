use redis::Commands;
use std::num::NonZero;

mod utils;

#[test]
fn test_rpush_lrange() {
    let mut test_server = utils::start_server_and_get_connection();
    let con = &mut test_server.connection;

    // (R)push the data
    let rpush_result: usize = con.rpush("list", "foo").unwrap();
    assert_eq!(rpush_result, 1);
    let rpush_result: usize = con.rpush("list", &["bar", "baz"]).unwrap();
    assert_eq!(rpush_result, 3);
    let list_size: usize = con.llen("list").unwrap();
    assert_eq!(list_size, 3);
    let rpush_result: usize = con.rpush("list", &["foo", "bar", "baz"]).unwrap();
    assert_eq!(rpush_result, 6);
    let list_size: usize = con.llen("list").unwrap();
    assert_eq!(list_size, 6);

    let list_size: usize = con.llen("non_existent").unwrap();
    assert_eq!(list_size, 0);

    // Fetch the data (for +ve indices)
    let lrange_result: Vec<String> = con.lrange("list", 0, 1).unwrap();
    assert_eq!(lrange_result, ["foo", "bar"].to_vec());
    let lrange_result: Vec<String> = con.lrange("list", 4, 4).unwrap();
    assert_eq!(lrange_result, ["bar"].to_vec());
    // Edge cases-
    // Non-existent list
    let lrange_result: Vec<String> = con.lrange("non_existent_list", 4, 4).unwrap();
    assert_eq!(lrange_result, Vec::<String>::new());
    // start index > list length
    let lrange_result: Vec<String> = con.lrange("list", 8, 4).unwrap();
    assert_eq!(lrange_result, Vec::<String>::new());
    // stop index > list length
    let lrange_result: Vec<String> = con.lrange("list", 4, 8).unwrap();
    assert_eq!(lrange_result, ["bar", "baz"].to_vec());
    // start index > stop index
    let lrange_result: Vec<String> = con.lrange("list", 2, 0).unwrap();
    assert_eq!(lrange_result, Vec::<String>::new());

    // Fetch the data (for -ve indices)
    let lrange_result: Vec<String> = con.lrange("list", -2, -1).unwrap();
    assert_eq!(lrange_result, ["bar", "baz"].to_vec());
    let lrange_result: Vec<String> = con.lrange("list", 0, -5).unwrap();
    assert_eq!(lrange_result, ["foo", "bar"].to_vec());
}

#[test]
fn test_lpush() {
    let mut test_server = utils::start_server_and_get_connection();
    let con = &mut test_server.connection;

    // (L)push the data
    let lpush_result: usize = con.lpush("llist", "c").unwrap();
    assert_eq!(lpush_result, 1);
    let lpush_result: usize = con.lpush("llist", &["b", "a"]).unwrap();
    assert_eq!(lpush_result, 3);

    let list_size: usize = con.llen("llist").unwrap();
    assert_eq!(list_size, 3);

    // Fetch the data
    let lrange_result: Vec<String> = con.lrange("llist", 0, -1).unwrap();
    assert_eq!(lrange_result, ["a", "b", "c"].to_vec());
}

#[test]
fn test_lpop() {
    let mut test_server = utils::start_server_and_get_connection();
    let con = &mut test_server.connection;

    let rpush_result: usize = con.rpush("list", &["a", "b", "c", "d"]).unwrap();
    assert_eq!(rpush_result, 4);

    let list_size: usize = con.llen("list").unwrap();
    assert_eq!(list_size, 4);

    // Lpop first element only
    let lpop_result: Vec<String> = con.lpop("list", None).unwrap();
    assert_eq!(lpop_result, ["a"].to_vec());
    let list_size: usize = con.llen("list").unwrap();
    assert_eq!(list_size, 3);

    // Lpop 2 elements
    let lpop_result: Vec<String> = con.lpop("list", NonZero::new(2)).unwrap();
    assert_eq!(lpop_result, ["b", "c"].to_vec());
    let list_size: usize = con.llen("list").unwrap();
    assert_eq!(list_size, 1);
}
