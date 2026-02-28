# Barge
Barge is a distributed key value store that uses raft consensus protocol underneath.

## Running
`make build` builds the server and cli binaries and puts them in `bin/` directory.

Start the server using `./bin/server -id <node_id> -peers <comma_separated_peers>`. For example to start a 3 node cluster you can do:
```
./bin/server -id node1 -port 50051 -peers node2=localhost:50052,node3=localhost:50053
./bin/server -id node2 -port 50052 -peers node1=localhost:50051,node3=localhost:50053
./bin/server -id node3 -port 50053 -peers node1=localhost:50051,node2=localhost:50052
```

Use the cli to interact with the cluster:
```
./bin/cli -server localhost:50051 put key1 value1
./bin/cli -server localhost:50051 get key1
```
