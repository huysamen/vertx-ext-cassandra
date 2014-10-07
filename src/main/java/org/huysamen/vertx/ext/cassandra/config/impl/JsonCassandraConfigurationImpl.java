package org.huysamen.vertx.ext.cassandra.config.impl;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.*;
import com.google.common.base.Strings;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.huysamen.vertx.ext.cassandra.config.CassandraConfiguration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Implementation of the Cassandra Configuration {@link org.huysamen.vertx.ext.cassandra.config.CassandraConfiguration}
 * contract.
 *
 * @author <a href="http://nico.huysamen.org">Nicolaas Frederick Huysamen</a>
 * @since 1.0
 * @version 1.0
 */
public class JsonCassandraConfigurationImpl implements CassandraConfiguration {

    public static final String CONFIG_SEEDS = "seeds";
    public static final String CONFIG_CONSISTENCY_LEVEL = "consistency_level";

    public static final String CONSISTENCY_ANY = "ANY";
    public static final String CONSISTENCY_ONE = "ONE";
    public static final String CONSISTENCY_TWO = "TWO";
    public static final String CONSISTENCY_THREE = "THREE";
    public static final String CONSISTENCY_QUORUM = "QUORUM";
    public static final String CONSISTENCY_ALL = "ALL";
    public static final String CONSISTENCY_LOCAL_ONE = "LOCAL_ONE";
    public static final String CONSISTENCY_LOCAL_QUORUM = "LOCAL_QUORUM";
    public static final String CONSISTENCY_EACH_QUORUM = "EACH_QUORUM";

    protected List<String> seeds;
    protected LoadBalancingPolicy loadBalancingPolicy;
    protected ReconnectionPolicy reconnectionPolicy;
    protected PoolingOptions poolingOptions;
    protected SocketOptions socketOptions;
    protected QueryOptions queryOptions;
    protected MetricsOptions metricsOptions;
    protected AuthProvider authProvider;

    public JsonCassandraConfigurationImpl(final JsonObject config) {
        initialise(config);
    }

    @Override
    public List<String> getSeeds() {
        return Collections.unmodifiableList(seeds);
    }

    @Override
    public LoadBalancingPolicy getLoadBalancingPolicy() {
        return loadBalancingPolicy;
    }

    @Override
    public ReconnectionPolicy getReconnectionPolicy() {
        return reconnectionPolicy;
    }

    @Override
    public PoolingOptions getPoolingOptions() {
        return poolingOptions;
    }

    @Override
    public SocketOptions getSocketOptions() {
        return socketOptions;
    }

    @Override
    public QueryOptions getQueryOptions() {
        return queryOptions;
    }

    @Override
    public MetricsOptions getMetricsOptions() {
        return metricsOptions;
    }

    @Override
    public AuthProvider getAuthProvider() {
        return authProvider;
    }

    protected void initialise(final JsonObject config) {

        initSeeds(config);
        initPolicies(config);
        initPoolingOptions(config);
        initSocketOptions(config);
        initQueryOptions(config);
        initMetricsOptions(config);
        initAuthProvider(config);

    }

    protected void initSeeds(final JsonObject config) {
        JsonArray seeds = config.getArray(CONFIG_SEEDS);

        if (seeds == null || seeds.size() == 0) {
            seeds = new JsonArray().addString("127.0.0.1");
        }

        this.seeds = new ArrayList<>();

        for (int i = 0; i < seeds.size(); i++) {
            this.seeds.add(seeds.<String>get(i));
        }
    }

    protected void initPolicies(final JsonObject config) {
        final JsonObject policyConfig = config.getObject("policies");

        if (policyConfig == null) {
            return;
        }

        initLoadBalancingPolicy(policyConfig);
        initReconnectionPolicy(policyConfig);
    }

    protected void initLoadBalancingPolicy(final JsonObject policyConfig) {
        final JsonObject loadBalancing = policyConfig.getObject("load_balancing");

        if (loadBalancing == null) {
            return;
        }

        final String name = loadBalancing.getString("name");

        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("A load balancing policy must have a class name field");
        } else if ("DCAwareRoundRobinPolicy".equalsIgnoreCase(name)
                || "com.datastax.driver.core.policies.DCAwareRoundRobinPolicy".equalsIgnoreCase(name)) {

            final String localDc = loadBalancing.getString("local_dc");
            final int usedHostsPerRemoteDc = loadBalancing.getInteger("used_hosts_per_remote_dc", 0);

            if (localDc == null || localDc.isEmpty()) {
                throw new IllegalArgumentException("A DCAwareRoundRobinPolicy requires a local_dc in configuration.");
            }

            loadBalancingPolicy = new DCAwareRoundRobinPolicy(localDc, usedHostsPerRemoteDc);
        } else {
            Class<?> clazz;

            try {
                clazz = Thread.currentThread().getContextClassLoader().loadClass(name);
            } catch (final ClassNotFoundException e) {
                throw new RuntimeException(e);
            }

            if (LoadBalancingPolicy.class.isAssignableFrom(clazz)) {
                try {
                    loadBalancingPolicy = (LoadBalancingPolicy) clazz.newInstance();
                } catch (final IllegalAccessException | InstantiationException e) {
                    throw new RuntimeException(e);
                }
            } else {
                throw new IllegalArgumentException("Class " + name + " does not implement LoadBalancingPolicy");
            }
        }
    }

    protected void initReconnectionPolicy(final JsonObject policyConfig) {
        final JsonObject reconnection = policyConfig.getObject("reconnection");

        if (reconnection == null) {
            return;
        }

        final String name = reconnection.getString("name");

        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("A reconnection policy must have a class name field");
        } else if ("ConstantReconnectionPolicy".equalsIgnoreCase(name) || "constant".equalsIgnoreCase(name)) {
            final Integer delay = reconnection.getInteger("delay");

            if (delay == null) {
                throw new IllegalArgumentException("ConstantReconnectionPolicy requires a delay in configuration");
            }

            reconnectionPolicy = new ConstantReconnectionPolicy(delay);
        } else if ("ExponentialReconnectionPolicy".equalsIgnoreCase(name) || "exponential".equalsIgnoreCase(name)) {
            final Long baseDelay = reconnection.getLong("base_delay");
            final Long maxDelay = reconnection.getLong("max_delay");

            if (baseDelay == null && maxDelay == null) {
                throw new IllegalArgumentException("ExponentialReconnectionPolicy requires base_delay and max_delay in configuration");
            }

            reconnectionPolicy = new ExponentialReconnectionPolicy(baseDelay, maxDelay);
        } else {
            Class<?> clazz;

            try {
                clazz = Thread.currentThread().getContextClassLoader().loadClass(name);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }

            if (!ReconnectionPolicy.class.isAssignableFrom(clazz)) {
                throw new IllegalArgumentException("Class " + name + " does not implement ReconnectionPolicy");
            }

            try {
                reconnectionPolicy = (ReconnectionPolicy) clazz.newInstance();
            } catch (final IllegalAccessException | InstantiationException e) {
                throw new RuntimeException(e);
            }
        }
    }

    protected void initPoolingOptions(final JsonObject config) {
        final JsonObject poolingConfig = config.getObject("pooling");

        if (poolingConfig == null) {
            return;
        }

        poolingOptions = new PoolingOptions();

        final Integer coreConnectionsPerHostLocal = poolingConfig.getInteger("core_connections_per_host_local");
        final Integer coreConnectionsPerHostRemote = poolingConfig.getInteger("core_connections_per_host_remote");
        final Integer maxConnectionsPerHostLocal = poolingConfig.getInteger("max_connections_per_host_local");
        final Integer maxConnectionsPerHostRemote = poolingConfig.getInteger("max_connections_per_host_remote");
        final Integer minSimultaneousRequestsLocal = poolingConfig.getInteger("min_simultaneous_requests_local");
        final Integer minSimultaneousRequestsRemote = poolingConfig.getInteger("min_simultaneous_requests_remote");
        final Integer maxSimultaneousRequestsLocal = poolingConfig.getInteger("max_simultaneous_requests_local");
        final Integer maxSimultaneousRequestsRemote = poolingConfig.getInteger("max_simultaneous_requests_remote");

        if (coreConnectionsPerHostLocal != null) {
            poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, coreConnectionsPerHostLocal);
        }

        if (coreConnectionsPerHostRemote != null) {
            poolingOptions.setCoreConnectionsPerHost(HostDistance.REMOTE, coreConnectionsPerHostRemote);
        }

        if (maxConnectionsPerHostLocal != null) {
            poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, maxConnectionsPerHostLocal);
        }

        if (maxConnectionsPerHostRemote != null) {
            poolingOptions.setMaxConnectionsPerHost(HostDistance.REMOTE, maxConnectionsPerHostRemote);
        }

        if (minSimultaneousRequestsLocal != null) {
            poolingOptions.setMinSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL, minSimultaneousRequestsLocal);
        }

        if (minSimultaneousRequestsRemote != null) {
            poolingOptions.setMinSimultaneousRequestsPerConnectionThreshold(HostDistance.REMOTE, minSimultaneousRequestsRemote);
        }

        if (maxSimultaneousRequestsLocal != null) {
            poolingOptions.setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL, maxSimultaneousRequestsLocal);
        }

        if (maxSimultaneousRequestsRemote != null) {
            poolingOptions.setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.REMOTE, maxSimultaneousRequestsRemote);
        }
    }

    protected void initSocketOptions(final JsonObject config) {
        final JsonObject socketConfig = config.getObject("socket");

        if (socketConfig == null) {
            return;
        }

        socketOptions = new SocketOptions();

        final Integer connectTimeoutMillis = socketConfig.getInteger("connect_timeout_millis");
        final Integer readTimeoutMillis = socketConfig.getInteger("read_timeout_millis");
        final Boolean keepAlive = socketConfig.getBoolean("keep_alive");
        final Boolean reuseAddress = socketConfig.getBoolean("reuse_address");
        final Integer receiveBufferSize = socketConfig.getInteger("receive_buffer_size");
        final Integer sendBufferSize = socketConfig.getInteger("send_buffer_size");
        final Integer soLinger = socketConfig.getInteger("so_linger");
        final Boolean tcpNoDelay = socketConfig.getBoolean("tcp_no_delay");

        if (connectTimeoutMillis != null) {
            socketOptions.setConnectTimeoutMillis(connectTimeoutMillis);
        }

        if (readTimeoutMillis != null) {
            socketOptions.setReadTimeoutMillis(readTimeoutMillis);
        }

        if (keepAlive != null) {
            socketOptions.setKeepAlive(keepAlive);
        }

        if (reuseAddress != null) {
            socketOptions.setReuseAddress(reuseAddress);
        }

        if (receiveBufferSize != null) {
            socketOptions.setReceiveBufferSize(receiveBufferSize);
        }

        if (sendBufferSize != null) {
            socketOptions.setSendBufferSize(sendBufferSize);
        }

        if (soLinger != null) {
            socketOptions.setSoLinger(soLinger);
        }

        if (tcpNoDelay != null) {
            socketOptions.setTcpNoDelay(tcpNoDelay);
        }
    }

    protected void initQueryOptions(final JsonObject config) {
        final ConsistencyLevel consistency = getConsistency(config);

        if (consistency == null) {
            return;
        }

        queryOptions = new QueryOptions().setConsistencyLevel(consistency);
    }

    protected ConsistencyLevel getConsistency(final JsonObject config) {
        final String consistency = config.getString(CONFIG_CONSISTENCY_LEVEL);

        if (consistency == null || consistency.isEmpty()) {
            return null;
        }

        if (consistency.equalsIgnoreCase(CONSISTENCY_ANY)) {
            return ConsistencyLevel.ANY;
        }

        if (consistency.equalsIgnoreCase(CONSISTENCY_ONE)) {
            return ConsistencyLevel.ONE;
        }

        if (consistency.equalsIgnoreCase(CONSISTENCY_TWO)) {
            return ConsistencyLevel.TWO;
        }

        if (consistency.equalsIgnoreCase(CONSISTENCY_THREE)) {
            return ConsistencyLevel.THREE;
        }

        if (consistency.equalsIgnoreCase(CONSISTENCY_QUORUM)) {
            return ConsistencyLevel.QUORUM;
        }

        if (consistency.equalsIgnoreCase(CONSISTENCY_ALL)) {
            return ConsistencyLevel.ALL;
        }

        if (consistency.equalsIgnoreCase(CONSISTENCY_LOCAL_ONE)) {
            return ConsistencyLevel.LOCAL_ONE;
        }

        if (consistency.equalsIgnoreCase(CONSISTENCY_LOCAL_QUORUM)) {
            return ConsistencyLevel.LOCAL_QUORUM;
        }

        if (consistency.equalsIgnoreCase(CONSISTENCY_EACH_QUORUM)) {
            return ConsistencyLevel.EACH_QUORUM;
        }

        throw new IllegalArgumentException("'" + consistency + "' is not a valid consistency level.");
    }

    protected void initMetricsOptions(final JsonObject config) {
        final JsonObject metrics = config.getObject("metrics");

        if (metrics == null) {
            return;
        }

        boolean jmx_enabled = metrics.getBoolean("jmx_enabled", true);
        metricsOptions = new MetricsOptions(jmx_enabled);
    }

    protected void initAuthProvider(final JsonObject config) {
        final JsonObject auth = config.getObject("auth");

        if (auth == null) {
            return;
        }

        final String username = auth.getString("username");
        final String password = auth.getString("password");

        if (Strings.isNullOrEmpty(username)) {
            throw new IllegalArgumentException("A username field must be provided on an auth field.");
        }
        if (Strings.isNullOrEmpty(password)) {
            throw new IllegalArgumentException("A password field must be provided on an auth field.");
        }

        authProvider = new PlainTextAuthProvider(username, password);
    }
}
