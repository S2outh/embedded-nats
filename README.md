
# Async NATS client for embedded devices

The `embedded-nats` crate implements the most relevant subset of the [NATS client protocol](https://docs.nats.io/reference/protocols/client)
for embedded devices. It can be used to connect `no_std` targets to NATS message servers and publish as well as subscribe to topics.

The crate uses `embedded-nal-async` traits in order to communicate with a TCP/Networking crate,
however it should be noted that it expects `read().await` methods to be cancel-safe, something the trait system does not guarantee.

As networking implementation for this crate I recommend `embassy-net`, as this crate has been tested extensively using that backend.

## Buffers

This crate supports two types of collections to hold data/topics. The user specifies the collections using generics.

* The `Heapless` collections use the `heapless` crate with the user provided constants specifying maximum message/topic size.
* The `Alloc` collections use `alloc::string::String` and `alloc::vec::Vec` types. This requires the `alloc` feature to be active as well a global allocator.

## Sync

To pass data between tasks, `embassy-sync` types are used. Therefore as part of the collections the user needs to specify a RawMutex using generics.

## Authenticators

This crate supports three ways to authenticate yourself to the server. The user specifies the authentication method using generics.
For convenience the crate provides a separate `new()` function for each authentication method.

* The `NoopAuthenticator` uses no authentication:
```rust,ignore
let (client, runner) = embedded_nats::new_no_auth(address, tcp_client, storage);
```
* The `UserPwdAuthenticator` tries to authenticate using a provided user/password
```rust,ignore
let (client, runner) = embedded_nats::new_with_user_pwd(user, password, address, tcp_client, storage).unwrap();
```
* The `TokenAuthenticator` tries to authenticate using a provided authentication token
```rust,ignore
let (client, runner) = embedded_nats::new_with_auth_token(auth_token, address, tcp_client, storage).unwrap();
```

## Examples

### Heapless

```rust,ignore
const NATS_MAX_TOPIC_SIZE: usize = 64;
const NATS_MAX_MESSAGE_SIZE: usize = 256;
const NATS_MAX_NUM_SUBS: usize = 1;
type NatsCollections =
    embedded_nats::Heapless<
        ThreadModeRawMutex,
        NATS_MAX_TOPIC_SIZE,
        NATS_MAX_MESSAGE_SIZE,
    >;

static NATS_STORAGE: embedded_nats::Storage<NatsCollections> = embedded_nats::Storage::new();

const NATS_MSG_CHANNEL_LENGTH = 5;
static SUBSCRIBE_CHANNEL: embedded_nats::MsgChannel<NatsCollections, NATS_MSG_CHANNEL_LENGTH> =
    embedded_nats::MsgChannel::new();


#[embassy_executor::task]
async fn nats_task(
    mut runner: embassy_nats::Runner<'static, NatsCollections, UserPwdAuthenticator, NATS_MAX_NUM_SUBS>,
) -> ! {
    runner.run().await
}

fn main() {
    let (mut client, runner) = embedded_nats::new_with_user_pwd(
        "admin",
        "passw0rd",
        socket_addr,
        tcp_client,
        &NATS_STORAGE,
    )
    .unwrap();

    client.subscribe(
        heapless::String::try_from("receive.topic").unwrap(),
        &SUBSCRIBE_CHANNEL,
    ).await.unwrap();
    
    let nats_msg = client.receive().await;

    // The body of the message: heapless::Vec<u8, NATS_MAX_MESSAGE_SIZE>
    let data = nats_msg.data;

    client.publish(
        heapless::String::try_from("send.topic").unwrap(),
        data,
    )
}
```

### Alloc

```rust,ignore
const NATS_MAX_NUM_SUBS: usize = 1;
type NatsCollections =
    embedded_nats::Alloc<ThreadModeRawMutex>;

static NATS_STORAGE: embedded_nats::Storage<NatsCollections> = embedded_nats::Storage::new();

const NATS_MSG_CHANNEL_LENGTH = 5;
static SUBSCRIBE_CHANNEL: embedded_nats::MsgChannel<NatsCollections, NATS_MSG_CHANNEL_LENGTH> =
    embedded_nats::MsgChannel::new();


#[embassy_executor::task]
async fn nats_task(
    mut runner: embassy_nats::Runner<'static, NatsCollections, UserPwdAuthenticator, NATS_MAX_NUM_SUBS>,
) -> ! {
    runner.run().await
}

fn main() {
    let (mut client, runner) = embedded_nats::new_with_user_pwd(
        "admin",
        "passw0rd",
        socket_addr,
        tcp_client,
        &NATS_STORAGE,
    )
    .unwrap();

    client.subscribe(
        alloc::string::String::from("receive.topic").unwrap(),
        &SUBSCRIBE_CHANNEL,
    ).await.unwrap();
    
    let nats_msg = client.receive().await;

    // The body of the message: alloc::vec::Vec<u8>
    let data = nats_msg.data;

    client.publish(
        alloc::string::String::from("send.topic").unwrap(),
        data,
    )
}
```

## Contributing

If you want to contribute feel free to open a PR, however note that there is no guarantee that it will be accepted,
especially if they contain a large amount of changes to the architecture/functionality.

If you want to make a larger change (such as adding a new NATS feature) I would recommend you first open an issue to discuss it.
