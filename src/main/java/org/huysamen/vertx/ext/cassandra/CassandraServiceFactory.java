package org.huysamen.vertx.ext.cassandra;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

/**
 * Cassandra service verticle factory contract.
 *
 * @author <a href="http://nico.huysamen.org">Nicolaas Frederick Huysamen</a>
 * @since 1.0
 * @version 1.0
 */
public interface CassandraServiceFactory {

    /**
     * Create a new Cassandra service from a given configuration.
     *
     * @param vertx The owning Vert.x container.
     * @param config The Cassandra service specific configuration parameters.
     *
     * @return The Cassandra service instance.
     */
    public CassandraService create(final Vertx vertx, final JsonObject config);

    /**
     * Create a Cassandra service event bus proxy on a given address.
     *
     * @param vertx The owning Vert.x container.
     * @param address The event bus address to use in the proxy.
     *
     * @return The Cassandra service instance.
     */
    public CassandraService createEventBusProxy(final Vertx vertx, final String address);
}
