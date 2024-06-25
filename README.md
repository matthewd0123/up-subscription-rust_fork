# up-subscription-rust

uSubscription service written in Rust

## Implementation Status

This codebase is heavily work in progress - among the next things to do and investigate:

- [] extend test coverage, especially for the backend subscription management and notification handler
- [] handling of startup and proper shutdown for usubscription service
- [] look into recent up-rust changes around Rpc and UTransport implementations, and whether we can use something from there
- [] add github CI pipeline setup
- [] create a usubscription-cli module, a simple command-line frontend for running up-subscription
- [] create a little demo application for interacting with up-subscription
- [] set up a devcontainer
- [] feed back learnings and clarifications into up-spec usubscription documentation

## Implementation questions

- Is it expected that a uEntity can register more than one custom topic receiving update notifications?
- Is it supposed to be possible to register remote uuris as notification topics?

## Getting Started

### Working with the library

`up-subscription-rust` is pluggable regarding which uTransport and RpcClient implementation it uses; you can provide any implementation of these up-rust traits for up-subscription to work with.

### Usage

At the moment, running up-subscription involves the following steps:

1. Instantiate the UTransport and RpcClient implementations you want to use
   - the UTransport is used for sending of subscription change notifications, as well as for returning service responses via the command listeners
   - the RpcClient is used for interacting with remote usubscription instances, when dealing with subscriptions for remote topics
2. Create a `USubscriptionServce::new()`, providing the UTransport and RpcClient implementations
   - this will directly spawn two tasks for managing subscriptions and dealing with the sending of subscription update notifications, respectively
3. Instantiate `UListener`s for all the usubscription commands you want to used (e.g. `SubscribeListener::new()`), passing the usubscription object from step #2
4. Register these listeners with your UTransport implementation
5. Vóila, you should have a working usubscription service up and running
