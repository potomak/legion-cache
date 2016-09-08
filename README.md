# Legion-Cache

Legion-Cache is an example program and test bed for the
[Legion](https://github.com/taphu/legion) framework.

It implements a replicated, in-memeory, distributed key/value store. It
is still very much experimental (as is the Legion framework) and is not
suitable for production use at this time.

## Getting started

The easiest way to get started is to configure two nodes and run them locally.
The configuration for node 1 might look like this:

    port: 8010
    adminPort: 8011
    adminHost: "*"

    logging:
        level: DEBUG

    peerAddr: ipv4:localhost:8012
    joinAddr: ipv4:localhost:8013

    ekgPort: 8014

The configuration for node 2 might look like this:

    port: 8020
    adminPort: 8021
    adminHost: "*"

    logging:
        level: DEBUG

    peerAddr: ipv4:localhost:8022
    joinAddr: ipv4:localhost:8023

    ekgPort: 8024


To start up the first node, run:

    $ stack build && stack exec legion-cache -- -c <config-file-1>

The default behavior when starting up is to create a new cluster, which is what
this command will do.

To start up the second node, run:

    $ stack build && stack exec legion-cache -- -c <config-file-2> -j ipv4:localhost:8013

The `-j` option tells legion-cache to try and join an existing Legion cluster.

Now you should have two nodes running, one on port 8010, and one on port 8020.

## Usage example

Try to store a new cache value by sending a PUT request to the first nodes:

    $ curl -v -X PUT \
        -H 'Content-Type: text/plain' \
        -d 'foo' \
        http://localhost:8010/cache/0

Try to retrieve the cache value you just stored from the second node:

    $ curl -v http://localhost:8020/cache/0
