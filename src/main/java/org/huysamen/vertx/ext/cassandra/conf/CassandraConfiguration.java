package org.huysamen.vertx.ext.cassandra.conf;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;

import java.util.List;

/**
 * Cassandra cluster configuration contract.
 *
 * @author <a href="http://nico.huysamen.org">Nicolaas Frederick Huysamen</a>
 * @since 1.0
 * @version 1.0
 */
public interface CassandraConfiguration {

    /**
     * The seed nodes addresses to use during cluster startup.
     *
     * @return The list of seed addresses.
     */
    public List<String> getSeeds();

    /**
     * The load balancing policy for the driver.
     *
     * @return The load balancing policy.
     */
    public LoadBalancingPolicy getLoadBalancingPolicy();

    /**
     * The reconnection policy for the driver.
     *
     * @return The reconnection policy.
     */
    public ReconnectionPolicy getReconnectionPolicy();

    /**
     * The pooling options for the driver.
     *
     * @return The pooling options.
     */
    public PoolingOptions getPoolingOptions();

    /**
     * The socket options for the driver.
     *
     * @return The socket options.
     */
    public SocketOptions getSocketOptions();

    /**
     * The query options for the driver.
     *
     * @return The query options.
     */
    public QueryOptions getQueryOptions();

    /**
     * The metrics options for the driver.
     *
     * @return The metrics options.
     */
    public MetricsOptions getMetricsOptions();

    /**
     * The authentication provider for the driver.
     *
     * @return The authentication provider.
     */
    public AuthProvider getAuthProvider();
}
