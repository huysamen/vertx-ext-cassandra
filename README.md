vertx-ext-cassandra
===================

### Apache Cassandra integration for Vert.x 3.x

### Intro
This extension provides functionality to access a Cassandra database with Vert.x 3.x using a Verticle Service Factory. The code is heavily burrowed from the folks at [englishtown](https://github.com/englishtown/vertx-mod-cassandra) and [nea](https://github.com/nea/vertx-mod-cassandra-persistor), as they both have done awesome work for Vert.x 2.x.

### Configuration
The configuration follows mostly from **vertx-mod-cassandra** from *englishtown*, with elements from *nea*. The following options are available:

```json
{
    "cassandra": {
        "seeds": [<seeds>],
        
        "policies": {
            "load_balancing": {
                "name": "<lb_policy_name>",
            },
            "reconnect_policy": {
                "name": "<reconnect_policy_name>"
            }
        },
        
        "pooling": {
            "core_connections_per_host_local": <int>,
            "core_connections_per_host_remote": <int>,
            "max_connections_per_host_local": <int>,
            "max_connections_per_host_remote": <int>,
            "min_simultaneous_requests_local": <int>,
            "min_simultaneous_requests_remote": <int>,
            "max_simultaneous_requests_local": <int>,
            "max_simultaneous_requests_remote": <int>
        },
        
        "socket": {
            "connect_timeout_millis": <int>,
            "read_timeout_millis": <int>,
            "keep_alive": <boolean>,
            "reuse_address": <boolean>,
            "receive_buffer_size": <int>,
            "send_buffer_size": <int>,
            "so_linger": <int>,
            "tcp_no_delay": <boolean>
        }
    }
}
```

* `seeds` - an array of string seed IP or host names.  At least one seed must be provided.
* `lb_policy_name` - (optional) the load balancing policy name.  The following values are accepted:
    * "DCAwareRoundRobinPolicy" - requires string field `local_dc` and optional numeric field `used_hosts_per_remote_dc`
    * Any FQCN such of a class that implements `LoadBalancingPolicy`
* `reconnect_policy_name` - (optional) the reconnect policy name.  The following values are accepted:
    * "constant"|"ConstantReconnectionPolicy" - creates a `ConstantReconnectionPolicy` policy.  Expects additional numeric       field `delay` in ms.
    * "exponential"|"ExponentialReconnectionPolicy" - creates an `ExponentialReconnectionPolicy` policy.  Expects               additional numeric fields `base_delay` and `max_delay` in ms.

Refer to the [Cassandra Java driver documentation](http://www.datastax.com/documentation/developer/java-driver/2.0/index.html) for a description of the remaining configuration options.


A sample config looks like:

```json
{
    "cassandra": {
        "seeds": ["10.0.0.1", "10.0.0.2"],
        
        "policies": {
            "load_balancing": {
                "name": "DCAwareRoundRobinPolicy",
                "local_dc": "LOCAL1",
                "used_hosts_per_remote_dc": 1
            },
            "reconnect": {
                "name": "exponential",
                "base_delay": 1000,
                "max_delay": 10000
            }
        },
    }
}
```

### Overriding with Environment Variables
This is not yet supported in **ext-cassandra***.
    
### Defaults
If there is no configuration, neither JSON nor environment variables, the module will default to looking for cassandra at 127.0.0.1 with the Cassandra driver defaults for everything.
