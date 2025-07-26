use std::{
    collections::HashMap,
    env,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

// Handle different types of possible values for a key
enum RedisType {
    Val(String),
    List(Vec<String>),
}

struct RedisValue {
    value: RedisType,
    creation_time: SystemTime,
    ttl: Option<u64>, // in ms; it is optional as it may not be present for every key and represents infinite TTL
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

// Compute output of the LRANGE command in human readable form, or an error
fn lrange(
    redis_key_val_store: Arc<Mutex<HashMap<String, RedisValue>>>,
    parsed_command: Vec<String>,
) -> Result<Vec<String>, &'static str> {
    let store = redis_key_val_store.lock().unwrap();

    let mut output_array: Vec<String> = Vec::new();

    if let Some(redis_val) = store.get(parsed_command[1].as_str()) {
        let list_at_key = match redis_val.value {
            RedisType::List(ref list) => list,
            _ => return Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
        };
        let (start_index, stop_index) = (parsed_command[2].clone(), parsed_command[3].clone());

        let list_length = list_at_key.len() as i32;
        let mut start_index = start_index.parse::<i32>().unwrap_or(list_length);
        let mut stop_index = stop_index.parse::<i32>().unwrap_or(list_length);

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

        let start_index = start_index as usize;
        let stop_index = stop_index as usize;
        output_array.append(&mut list_at_key[start_index..=stop_index].to_vec());
        Ok(output_array)
    } else {
        // Return empty array if the key doesn't exist
        Ok(output_array)
    }
}

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
    let mut command_list: Vec<String> = vec![];
    for string in input.trim().split("\r\n") {
        if string.starts_with(['*', '$']) {
            continue;
        }
        command_list.push(String::from(string));
    }

    command_list
}

async fn process(
    mut stream: TcpStream,
    redis_key_val_store: Arc<Mutex<HashMap<String, RedisValue>>>,
) {
    // Can handle input string of 1024 bytes
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
            "echo" => &format!("+{}\r\n", parsed_command[1].as_str()), // TODO: why .as_str() doesn't work here???
            "set" => {
                // Infinite TTL
                let mut ttl_ms: Option<u64> = None;

                // If expiry time parameters are passed then set TTL (in ms) else TTL is infinite
                if parsed_command.len() >= 5 {
                    // let (_time_format, ttl_ms) = (parsed_command[3].clone(), parsed_command[4].clone();
                    ttl_ms = Some(parsed_command[4].parse::<u64>().unwrap());
                }

                // This overwrites the value if the key already exists
                redis_key_val_store.lock().unwrap().insert(
                    parsed_command[1].clone(),
                    RedisValue {
                        value: RedisType::Val(parsed_command[2].clone()),
                        creation_time: SystemTime::now(),
                        ttl: ttl_ms,
                    },
                );
                "+OK\r\n"
            }
            "get" => {
                let mut store = redis_key_val_store.lock().unwrap();
                let returned_value = match store.get(parsed_command[1].as_str()) {
                    Some(x) => {
                        let key_expired = x.ttl.is_some()
                            && SystemTime::now()
                                > x.creation_time + Duration::from_millis(x.ttl.unwrap());
                        if key_expired {
                            // Remove the key as it has expired
                            // This is called "PASSIVE EXPIRY" in Redis
                            store.remove(&parsed_command[1]);
                            "$-1" // Return "Null bulk string" if the input key has expired and consequently does not exist
                        } else {
                            match x.value {
                                // Only accept string values
                                RedisType::Val(ref val) => &format!("${}\r\n{}", val.len(), val),
                                _ => "-WRONGTYPE Operation against a key holding the wrong kind of value",
                            }
                        }
                    }
                    None => "$-1", // Return "Null bulk string" if the input key does not exist
                };
                &format!("{}\r\n", returned_value)
            }
            // In milliseconds
            "ttl" => {
                let store = redis_key_val_store.lock().unwrap();
                let returned_value = match store.get(parsed_command[1].as_str()) {
                    Some(x) => {
                        match x.ttl {
                            Some(ttl) => {
                                let ttl_left = (x.creation_time + Duration::from_millis(ttl))
                                    .duration_since(SystemTime::now())
                                    .unwrap_or_default(); // When the key has expired, set the duration to default of 0
                                &(ttl_left.as_millis()).to_string()
                            }
                            None => "-1", // Key with no TTL set
                        }
                    }
                    None => "-1", // Key does not exist
                };
                &format!("+{}\r\n", returned_value)
            }
            "dbsize" => {
                let dbsize = redis_key_val_store.lock().unwrap().len();
                &format!(":{}\r\n", dbsize)
            }
            "rpush" => {
                let mut store = redis_key_val_store.lock().unwrap();

                // Get the reference to the value; if the key doesn't exist then create it
                let redis_val =
                    store
                        .entry(parsed_command[1].clone())
                        .or_insert_with(|| RedisValue {
                            value: RedisType::List(vec![]),
                            creation_time: SystemTime::now(),
                            ttl: None,
                        });

                // Insert the desired data to the referenced value, taking care of errors
                let insertion_result: Result<usize, &str> = match &mut redis_val.value {
                    RedisType::List(list) => {
                        if parsed_command.len() <= 2 {
                            Err("ERR wrong number of arguments for command")
                        } else {
                            list.append(&mut parsed_command[2..].to_vec()); // Move occurs here
                            Ok(list.len())
                        }
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                };

                // Convert to RESP and return the result
                match insertion_result {
                    Ok(len) => &format!(":{len}\r\n"),
                    Err(err) => &format!("-{err}\r\n"),
                }
            }
            "lrange" => {
                let lrange_output = lrange(redis_key_val_store.clone(), parsed_command);

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
async fn main() {
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

    // // Handle "ACTIVE EXPIRY" of keys
    // let store = redis_key_val_store.clone();
    // tokio::spawn(async move {
    //     delete_expired_keys(store).await;
    // });

    loop {
        match listener.accept().await {
            // The second item contains the IP and port of the new connection.
            Ok((stream, _)) => {
                let redis_key_val_store = redis_key_val_store.clone();
                // let redis_key_val_store = Arc::clone(&redis_key_val_store); // Same as .clone()

                // A new task is spawned for each inbound socket. The socket is
                // moved to the new task and processed there.
                tokio::spawn(async move {
                    process(stream, redis_key_val_store).await;
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
