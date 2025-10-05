//! Redis server clone in Rust
#![warn(
    clippy::all,
    clippy::pedantic,
    clippy::correctness,
    clippy::suspicious,
    clippy::style,
    clippy::complexity,
    clippy::perf,
    clippy::nursery
)]
// These are from clippy::restriction
#![warn(
    clippy::missing_docs_in_private_items,
    clippy::infinite_loop,
    clippy::clone_on_ref_ptr,
    clippy::allow_attributes,
    clippy::allow_attributes_without_reason,
    clippy::absolute_paths,
    clippy::pattern_type_mismatch,
    clippy::dbg_macro
)]

use std::{
    borrow::Cow,
    collections::{HashMap, VecDeque},
    env, str,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::{TcpListener, TcpStream},
    sync::oneshot::{Receiver, Sender},
    sync::Mutex as TMutex,
};

/// Represent different types of possible values for a key.
enum RedisType {
    /// Array/list data type.
    List(VecDeque<String>),
    /// String data type.
    Val(String),
}

/// Represent all the data for a key
struct RedisValue {
    /// Creation time of the key
    creation_time: SystemTime,
    /// Actual data
    data: RedisType,
    /// TTL of the key
    ttl: Option<u64>, // in ms; it is optional as it may not be present for every key and thus will be infinite
}

// Periodically remove the expired keys
// Procedure-
// 1) Randomly sample the HashMap keys and check its TTL.
// 2) If TTL is expired, then remove it from the HashMap. (TODO: need to do this efficiently, maybe by storing the keys in a Vec also)
// 3) Sleep for some time.
// 4) Repeat from step 1.
// async fn delete_expired_keys(redis_key_val_store: Arc<Mutex<HashMap<String, RedisValue>>>) {
//     loop {
//         // Sleep for 1 second before checking for expired keys
//         time::sleep(Duration::from_secs(1)).await;
//         let mut store = redis_key_val_store.lock().unwrap();
//         let current_time = SystemTime::now();
//         // let dbsize = store.len();
//
//         // Iterate through the keys and remove expired ones
//         store.retain(|_, value| {
//             if let Some(ttl) = value.ttl {
//                 current_time < value.creation_time + Duration::from_millis(ttl)
//             } else {
//                 true // Keep keys with no TTL
//             }
//         });
//     }
// }

/// Compute output of the LRANGE command in human readable form, or an error
fn lrange(
    redis_key_val_store: &Arc<Mutex<HashMap<String, RedisValue>>>,
    parsed_command: &[String],
) -> Result<Vec<String>, &'static str> {
    let mut output_array: Vec<String> = Vec::new();

    if let Some(redis_val) = redis_key_val_store
        .lock()
        .unwrap()
        .get(parsed_command[1].as_str())
    {
        let RedisType::List(ref list_at_key) = redis_val.data else {
            return Err("WRONGTYPE Operation against a key holding the wrong kind of value");
        };
        let (start_index, stop_index) = (parsed_command[2].clone(), parsed_command[3].clone());

        // Crash if aren't able to go from usize to isize
        let list_length = isize::try_from(list_at_key.len()).unwrap();
        let mut start_index = start_index.parse::<isize>().unwrap_or(list_length);
        let mut stop_index = stop_index.parse::<isize>().unwrap_or(list_length);

        if start_index < 0 {
            start_index += list_length;
        }
        if stop_index < 0 {
            stop_index += list_length;
        }
        start_index = start_index.max(0);
        stop_index = stop_index.min(list_length - 1);

        if start_index > stop_index {
            return Ok(output_array);
        }

        #[expect(clippy::cast_sign_loss, reason = "Negative value is clipped above")]
        let start_index = start_index as usize;
        #[expect(clippy::cast_sign_loss, reason = "Negative value causes early return")]
        let stop_index = stop_index as usize;

        output_array = (start_index..=stop_index)
            .map(|x| list_at_key.get(x).unwrap().clone())
            .collect();
    }
    Ok(output_array)
}

/// A very basic parser for RESP
/// Currently only handles non-nested arrays
/// Returns the parsed output in human readable form
fn parse_command(input: &str) -> Vec<String> {
    #[expect(
        clippy::collection_is_never_read,
        reason = "Will solve this in future when writing proper parser"
    )]
    let mut command_list: Vec<String> = Vec::new();

    let mut input_it = input.trim().chars().enumerate();
    // for (ind, char) in input_it {
    while let Some((ind, char)) = input_it.next() {
        #[expect(
            clippy::single_match,
            reason = "Will solve this in future when writing proper parser"
        )]
        match char {
            '*' => {
                let (ind2, _) = input_it.find(|&x| x.1 == '\r').unwrap();
                command_list.push(String::from(&input[ind..ind2]));
                input_it.next();
            }
            _ => (),
        }
    }

    // Currently, always guaranteed to contain a value
    // let _vector_length = command_list.pop().unwrap();

    // Extract only the valid strings for now
    // The previous code is useless for now, but, might become useful later on
    let mut command_list: Vec<String> = vec![];
    for string in input.lines() {
        if string.starts_with(['*', '$']) {
            continue;
        }
        command_list.push(String::from(string));
    }

    command_list
}

#[expect(
    clippy::too_many_lines,
    reason = "Will handle this later by creating a Redis class"
)]
/// Process a client connection
/// This function handles multiple requests from a single client
async fn process(
    mut stream: TcpStream,
    redis_key_val_store: Arc<Mutex<HashMap<String, RedisValue>>>,
    // oneshot_store: Arc<TMutex<HashMap<String, VecDeque<Sender<()>>>>>,
    oneshot_store: Arc<Mutex<HashMap<String, VecDeque<Sender<()>>>>>,
) {
    // Can handle input string of 1024 bytes
    let mut buf = [0; 1024];

    while let Ok(bytes_read) = stream.read(&mut buf).await {
        if bytes_read == 0 {
            break;
        }
        let parsed_command = parse_command(str::from_utf8(&buf[..bytes_read]).unwrap());

        // Main Redis Server functioning

        // Redis commands are case insensitive
        let redis_output = match parsed_command[0].to_lowercase().as_str() {
            "ping" => "+PONG\r\n",
            "echo" => &format!("+{}\r\n", parsed_command[1].as_str()), // TODO: why .as_str() doesn't work here???
            "client" => "+OK\r\n+OK\r\n", // Default redis-rs client sends 2 arrays when creating connection
            "set" => {
                // If expiry time parameters are passed then set TTL (in ms) else TTL is infinite
                let ttl_ms = if parsed_command.len() >= 5 {
                    Some(parsed_command[4].parse::<u64>().unwrap())
                } else {
                    None // Infinite TTL
                };

                // This overwrites the value if the key already exists
                redis_key_val_store.lock().unwrap().insert(
                    parsed_command[1].clone(),
                    RedisValue {
                        data: RedisType::Val(parsed_command[2].clone()),
                        creation_time: SystemTime::now(),
                        ttl: ttl_ms,
                    },
                );
                "+OK\r\n"
            }
            "get" => {
                let mut store = redis_key_val_store.lock().unwrap();
                #[expect(
                    clippy::option_if_let_else,
                    reason = "Difficult to handle this case as `store` is causing borrow checker issues inside closure"
                )]
                let returned_value = match store.get(parsed_command[1].as_str()) {
                    Some(x) => {
                        let key_expired = x.ttl.is_some()
                            && SystemTime::now()
                                > x.creation_time + Duration::from_millis(x.ttl.unwrap());
                        if key_expired {
                            // Remove the key as it has expired
                            // This is called "PASSIVE EXPIRY" in Redis
                            store.remove(&parsed_command[1]);
                            drop(store);
                            "$-1" // Return "Null bulk string" if the input key has expired and consequently does not exist
                        } else {
                            #[expect(clippy::match_wildcard_for_single_variants, reason="As only RedisType::Val is allowed for 'GET' operations")]
                            match x.data {
                                // Only accept string values
                                RedisType::Val(ref val) => &format!("${}\r\n{}", val.len(), val),
                                _ => "-WRONGTYPE Operation against a key holding the wrong kind of value",
                            }
                        }
                    }
                    None => "$-1", // Return "Null bulk string" if the input key does not exist
                };
                &format!("{returned_value}\r\n")
            }
            // In milliseconds
            "ttl" => {
                let returned_value = redis_key_val_store
                    .lock()
                    .unwrap()
                    .get(parsed_command[1].as_str())
                    .map_or_else(
                        || String::from("-1"), // Key does not exist
                        |x| {
                            x.ttl.map_or_else(
                                || String::from("-1"), // Key with no TTL set
                                |ttl| {
                                    let ttl_left = (x.creation_time + Duration::from_millis(ttl))
                                        .duration_since(SystemTime::now())
                                        .unwrap_or_default(); // When the key has expired, set the duration to default of 0
                                    ttl_left.as_millis().to_string()
                                },
                            )
                        },
                    );
                &format!("+{returned_value}\r\n")
            }
            "dbsize" => {
                let dbsize = redis_key_val_store.lock().unwrap().len();
                &format!(":{dbsize}\r\n")
            }
            "rpush" => {
                let mut store = redis_key_val_store.lock().unwrap();

                // Get the reference to the value; if the key doesn't exist then create it
                let redis_val =
                    store
                        .entry(parsed_command[1].clone())
                        .or_insert_with(|| RedisValue {
                            data: RedisType::List(VecDeque::new()),
                            creation_time: SystemTime::now(),
                            ttl: None,
                        });

                #[expect(
                    clippy::match_wildcard_for_single_variants,
                    reason = "rpush command works only on List"
                )]
                // Insert the desired data to the referenced value, taking care of errors
                let insertion_result: Result<usize, &str> = match redis_val.data {
                    RedisType::List(ref mut list) => {
                        if parsed_command.len() <= 2 {
                            Err("ERR wrong number of arguments for command")
                        } else {
                            // // for x in parsed_command[2..].to_vec() {  TODO: why not ???
                            // for x in parsed_command[2..].iter().cloned() {
                            //     list.push_back(x);
                            // }
                            // TODO: Time this with above 3 lines
                            // list.append(&mut parsed_command[2..].to_vec().into());
                            // TODO: Time this with above line
                            list.extend(parsed_command[2..].iter().cloned());
                            Ok(list.len())
                        }
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                };
                drop(store);

                // Convert to RESP and return the result
                match insertion_result {
                    Ok(len) => &format!(":{len}\r\n"),
                    Err(err) => &format!("-{err}\r\n"),
                }
            }
            "lpush" => {
                let mut store = redis_key_val_store.lock().unwrap();

                // Get the reference to the value; if the key doesn't exist then create it
                let redis_val =
                    store
                        .entry(parsed_command[1].clone())
                        .or_insert_with(|| RedisValue {
                            data: RedisType::List(VecDeque::new()),
                            creation_time: SystemTime::now(),
                            ttl: None,
                        });

                #[expect(
                    clippy::match_wildcard_for_single_variants,
                    reason = "lpush command works only on List"
                )]
                // Insert the desired data to the referenced value, taking care of errors
                let insertion_result: Result<usize, &str> = match redis_val.data {
                    RedisType::List(ref mut list) => {
                        if parsed_command.len() <= 2 {
                            Err("ERR wrong number of arguments for command")
                        } else {
                            for x in parsed_command[2..].iter().cloned() {
                                list.push_front(x);
                            }

                            // Send trigger to the channel for the specified list
                            // See `lbpop`
                            // let _a = oneshot_store.lock().unwrap();
                            if let Some(key) =
                                oneshot_store.lock().unwrap().get_mut(&parsed_command[1])
                            {
                                if let Some(sender) = key.pop_front() {
                                    sender.send(()).unwrap();
                                }
                            }

                            Ok(list.len())
                        }
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                };
                drop(store);

                // Convert to RESP and return the result
                match insertion_result {
                    Ok(len) => &format!(":{len}\r\n"),
                    Err(err) => &format!("-{err}\r\n"),
                }
            }
            "lrange" => {
                let lrange_output = lrange(&redis_key_val_store, &parsed_command);

                // Convert to RESP and return the result
                match lrange_output {
                    Ok(output_array) => {
                        let output_string = format!("*{}\r\n", output_array.len());
                        &output_array.iter().fold(output_string.clone(), |acc, x| {
                            acc + "$" + x.len().to_string().as_str() + "\r\n" + x + "\r\n"
                        })
                    }
                    Err(err) => &format!("-{err}\r\n"),
                }
            }
            "llen" => {
                let store = redis_key_val_store.lock().unwrap();

                #[expect(clippy::match_wildcard_for_single_variants, reason = "llen command works only on List")]
                &store
                    .get(parsed_command[1].as_str())
                    .map_or(Cow::Borrowed(":0\r\n"), |redis_val| match redis_val.data {
                        RedisType::List(ref list) => Cow::Owned(format!(":{}\r\n", list.len())),
                        _ => {
                            Cow::Borrowed("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
                        }
                    })
            }
            "lpop" => {
                let mut store = redis_key_val_store.lock().unwrap();
                let times_to_pop = parsed_command
                    .get(2)
                    .map_or(1, |x| x.parse::<u32>().unwrap());

                if let Some(redis_val) = store.get_mut(parsed_command[1].as_str()) {
                    #[expect(
                        clippy::match_wildcard_for_single_variants,
                        reason = "lpop command works only on List"
                    )]
                    match redis_val.data {
                        RedisType::List(ref mut list) => {
                            let output_array: Vec<String> =
                                (1..=times_to_pop).map_while(|_| list.pop_front()).collect();

                            // Remove the key from the store if its list has become empty
                            if list.is_empty() {
                                store.remove(&parsed_command[1]);
                            }
                            drop(store);

                            if output_array.is_empty() {
                                "$-1\r\n"
                            } else if output_array.len() == 1 {
                                &format!("${}\r\n{}\r\n", output_array[0].len(), output_array[0])
                            } else {
                                let output_string = format!("*{}\r\n", output_array.len());
                                &output_array.iter().fold(output_string.clone(), |acc, x| {
                                    acc + "$" + x.len().to_string().as_str() + "\r\n" + x + "\r\n"
                                })
                            }
                        }
                        _ => {
                            "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
                        }
                    }
                } else {
                    "$-1\r\n"
                }
            }
            "lbpop" => {
                let db_list_name = &parsed_command[1];
                // let mut store = redis_key_val_store.lock().unwrap();

                if let Some(redis_val) = redis_key_val_store.lock().unwrap().get_mut(db_list_name) {
                    #[expect(
                        clippy::match_wildcard_for_single_variants,
                        reason = "lbpop command works only on List"
                    )]
                    match redis_val.data {
                        RedisType::List(ref mut list) => {
                            let val = list.pop_front().unwrap();

                            // Remove the key from the store if its list has become empty
                            // if list.is_empty() {
                            //     store.remove(db_list_name);
                            // }
                            // drop(store);
                            &format!("${}\r\n{}\r\n", val.len(), val)
                        }
                        _ => {
                            "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
                        }
                    }
                } else {
                    // let mut oneshot_store = oneshot_store.lock().await;
                    let mut oneshot_store = oneshot_store.lock().unwrap();
                    let channel = tokio::sync::oneshot::channel();

                    let oneshot_val = oneshot_store.entry(db_list_name.clone()).or_default();
                    oneshot_val.push_back(channel.0);
                    drop(oneshot_store);

                    channel.1.await;

                    // if let Some(redis_val) = store.get_mut(db_list_name) {
                    //     match redis_val.data {
                    //         RedisType::List(ref mut list) => {
                    //             let val = list.pop_front().unwrap();
                    //
                    //                 // Remove the key from the store if its list has become empty
                    //                 if list.is_empty() {
                    //                     store.remove(db_list_name);
                    //                 }
                    //                 drop(store);
                    //             &format!("${}\r\n{}\r\n", val.len(), val)
                    //         }
                    //         _ => {
                    //             "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
                    //         }
                    //     };
                    // }

                    "$1\r\n"
                }
            }
            _ => {
                // Handle case of unknown command
                let args = parsed_command
                    .iter()
                    .skip(1) // First element is the command so skip it
                    .fold(String::new(), |acc, x| acc + "`" + x + "`, ");
                &format!(
                    "-ERR unknown command `{}`, with args beginning with: {}\r\n",
                    parsed_command[0], args,
                )
            }
        };

        stream.write_all(redis_output.as_bytes()).await.unwrap();
    }
}

#[tokio::main]
async fn main() -> ! {
    // Second argument must be `port`
    let mut args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        args.push("6379".to_string());
    }
    let port = args[1].as_str();

    let listener = TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .unwrap();

    // Actual Redis key-val store
    let redis_key_val_store: Arc<Mutex<HashMap<String, RedisValue>>> =
        Arc::new(Mutex::new(HashMap::new()));

    // let oneshot_store: Arc<Mutex<HashMap<String, VecDeque<(Sender<()>, Receiver<()>)>>>> =
    //     Arc::new(Mutex::new(HashMap::new()));
    // let oneshot_store: Arc<TMutex<HashMap<String, VecDeque<Sender<()>>>>> =
    let oneshot_store: Arc<Mutex<HashMap<String, VecDeque<Sender<()>>>>> =
        Arc::new(Mutex::new(HashMap::new()));

    // // Handle "ACTIVE EXPIRY" of keys
    // let store = redis_key_val_store.clone();
    // tokio::spawn(async move {
    //     delete_expired_keys(store).await;
    // });

    loop {
        match listener.accept().await {
            // The second item contains the IP and port of the new connection.
            Ok((stream, _)) => {
                let redis_key_val_store = Arc::clone(&redis_key_val_store); // Same as .clone()
                                                                            // let oneshot_store = oneshot_store.clone();
                let oneshot_store = Arc::clone(&oneshot_store);

                // A new task is spawned for each inbound socket. The socket is
                // moved to the new task and processed there.
                tokio::spawn(async move {
                    process(stream, redis_key_val_store, oneshot_store).await;
                });
            }
            Err(e) => {
                eprintln!("error: {e}");
            }
        }
    }
}
