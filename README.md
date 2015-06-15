# KDB

[![Join the chat at https://gitter.im/meteorhacks/kdb](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/meteorhacks/kdb?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

ACID High Performance Time Series DB for any kind of storage - No it isn't

## Notes:

 * KDB uses memory mapping to increase write performance therefore for KDB to work the `IPC_LOCK` linux capability must be enabled when running inside docker. This can be done easily by adding `--cap-add=IPC_LOCK` when starting the container. Checkout KMDB for an example.
