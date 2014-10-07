package org.huysamen.vertx.ext.cassandra.impl;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.Policies;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.huysamen.vertx.ext.cassandra.config.CassandraConfiguration;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Metrics container
 */
class Metrics implements AutoCloseable {

    private final CassandraServiceImpl service;
    private final MetricRegistry registry = new MetricRegistry();
    private JmxReporter reporter;
    private GaugeStateListener listener;

    protected Metrics(final CassandraServiceImpl service) {
        this.service = service;
    }

    protected void afterReconnect() {
        close();

        final Cluster cluster = service.getCluster();
        final Configuration configuration = cluster.getConfiguration();
        final String config = getConfiguration(service.getConfig(), configuration).encodePrettily();

        String name = "config";
        registry.remove(name);
        registry.register(name, (Gauge<String>) () -> config);

        name = "closed";
        registry.remove(name);
        registry.register(name, (Gauge<Boolean>) service::isClosed);

        listener = new GaugeStateListener();
        cluster.register(listener);

        if (configuration.getMetricsOptions().isJMXReportingEnabled()) {
            final String domain = "et.cass." + cluster.getClusterName() + "-metrics";

            reporter = JmxReporter
                    .forRegistry(registry)
                    .inDomain(domain)
                    .build();

            reporter.start();
        }
    }

    private JsonObject getConfiguration(final CassandraConfiguration configurator, final Configuration configuration) {
        final JsonObject json = new JsonObject();

        // Add seeds
        final List<String> seeds = configurator.getSeeds();
        final JsonArray arr = new JsonArray();

        json.putArray("seeds", arr);

        if (seeds != null) {
            seeds.forEach(arr::addString);
        }

        final Policies policies = configuration.getPolicies();
        final JsonObject policiesJson = new JsonObject();

        json.putObject("policies", policiesJson);

        if (policies != null) {
            final LoadBalancingPolicy lbPolicy = policies.getLoadBalancingPolicy();
            policiesJson.putString("load_balancing", lbPolicy == null ? null : lbPolicy.getClass().getSimpleName());

            final ReconnectionPolicy reconnectionPolicy = policies.getReconnectionPolicy();
            policiesJson.putString("reconnection", reconnectionPolicy == null ? null : reconnectionPolicy.getClass().getSimpleName());

            final RetryPolicy retryPolicy = policies.getRetryPolicy();
            policiesJson.putString("retry", retryPolicy == null ? null : retryPolicy.getClass().getSimpleName());
        }

        final PoolingOptions poolingOptions = configuration.getPoolingOptions();
        final JsonObject pooling = new JsonObject();

        json.putObject("pooling", pooling);

        if (poolingOptions != null) {
            pooling.putNumber("core_connections_per_host_local", poolingOptions.getCoreConnectionsPerHost(HostDistance.LOCAL));
            pooling.putNumber("core_connections_per_host_remote", poolingOptions.getCoreConnectionsPerHost(HostDistance.REMOTE));
            pooling.putNumber("max_connections_per_host_local", poolingOptions.getMaxConnectionsPerHost(HostDistance.LOCAL));
            pooling.putNumber("max_connections_per_host_remote", poolingOptions.getMaxConnectionsPerHost(HostDistance.REMOTE));

            pooling.putNumber("min_simultaneous_requests_local", poolingOptions.getMinSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL));
            pooling.putNumber("min_simultaneous_requests_remote", poolingOptions.getMinSimultaneousRequestsPerConnectionThreshold(HostDistance.REMOTE));
            pooling.putNumber("max_simultaneous_requests_local", poolingOptions.getMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL));
            pooling.putNumber("max_simultaneous_requests_remote", poolingOptions.getMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.REMOTE));
        }

        final SocketOptions socketOptions = configuration.getSocketOptions();
        final JsonObject socket = new JsonObject();

        json.putObject("socket", socket);

        if (socketOptions != null) {
            socket.putNumber("connect_timeout_millis", socketOptions.getConnectTimeoutMillis());
            socket.putNumber("read_timeout_millis", socketOptions.getReadTimeoutMillis());
            socket.putNumber("receive_buffer_size", socketOptions.getReceiveBufferSize());
            socket.putNumber("send_buffer_size", socketOptions.getSendBufferSize());
            socket.putNumber("so_linger", socketOptions.getSoLinger());
            socket.putBoolean("keep_alive", socketOptions.getKeepAlive());
            socket.putBoolean("reuse_address", socketOptions.getReuseAddress());
            socket.putBoolean("tcp_no_delay", socketOptions.getTcpNoDelay());
        }

        final QueryOptions queryOptions = configuration.getQueryOptions();
        final JsonObject query = new JsonObject();

        json.putObject("query", query);

        if (queryOptions != null) {
            ConsistencyLevel consistency = queryOptions.getConsistencyLevel();
            query.putString("consistency", consistency == null ? null : consistency.name());

            consistency = queryOptions.getSerialConsistencyLevel();
            query.putString("serial_consistency", consistency == null ? null : consistency.name());
            query.putNumber("fetch_size", queryOptions.getFetchSize());
        }

        return json;
    }

    @Override
    public void close() {
        if (listener != null) {
            service.getCluster().unregister(listener);
            listener = null;
        }

        if (reporter != null) {
            reporter.stop();
            reporter = null;
        }
    }

    private class GaugeStateListener implements Host.StateListener {
        private final ConcurrentMap<String, Host> addedHosts = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, Host> upHosts = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, Host> removedHosts = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, Host> downHosts = new ConcurrentHashMap<>();

        public GaugeStateListener() {

            String name;

            name = "added-hosts";
            registry.remove(name);
            registry.register(name, new Gauge<String>() {
                @Override
                public String getValue() {
                    return stringify(addedHosts);
                }
            });

            name = "up-hosts";
            registry.remove(name);
            registry.register(name, new Gauge<String>() {
                @Override
                public String getValue() {
                    return stringify(upHosts);
                }
            });

            name = "down-hosts";
            registry.remove(name);
            registry.register(name, new Gauge<String>() {
                @Override
                public String getValue() {
                    return stringify(downHosts);
                }
            });

            name = "removed-hosts";
            registry.remove(name);
            registry.register(name, new Gauge<String>() {
                @Override
                public String getValue() {
                    return stringify(removedHosts);
                }
            });
        }

        private String stringify(ConcurrentMap<String, Host> hosts) {

            final StringBuilder sb = new StringBuilder();
            String delimiter = "";

            for (String key : hosts.keySet()) {
                Host host = hosts.get(key);
                if (host != null) {
                    sb.append(delimiter)
                            .append(host.toString())
                            .append(" (dc=")
                            .append(host.getDatacenter())
                            .append(" up=")
                            .append(host.isUp())
                            .append(")");

                    delimiter = "\n";
                }
            }

            return sb.toString();
        }

        private String getKey(final Host host) {
            return host.getAddress().toString();
        }

        /**
         * Called when a new node is added to the cluster.
         * <p>
         * The newly added node should be considered up.
         *
         * @param host the host that has been newly added.
         */
        @Override
        public void onAdd(final Host host) {
            String key = getKey(host);
            addedHosts.put(key, host);
            removedHosts.remove(key);
        }

        /**
         * Called when a node is determined to be up.
         *
         * @param host the host that has been detected up.
         */
        @Override
        public void onUp(final Host host) {
            String key = getKey(host);
            upHosts.put(key, host);
            downHosts.remove(key);
        }

        @Override
        public void onSuspected(final Host host) {}

        /**
         * Called when a node is determined to be down.
         *
         * @param host the host that has been detected down.
         */
        @Override
        public void onDown(final Host host) {
            String key = getKey(host);
            downHosts.put(key, host);
            upHosts.remove(key);
        }

        /**
         * Called when a node is removed from the cluster.
         *
         * @param host the removed host.
         */
        @Override
        public void onRemove(final Host host) {
            String key = getKey(host);
            removedHosts.put(key, host);
            addedHosts.remove(key);
        }
    }
}