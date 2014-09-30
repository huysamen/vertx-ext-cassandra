package org.huysamen.vertx.ext.cassandra;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.ServiceHelper;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

/**
 * The Cassandra service contract.
 *
 * @author <a href="http://nico.huysamen.org">Nicolaas Frederick Huysamen</a>
 * @since 1.0
 * @version 1.0
 */
public interface CassandraService {

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Factory Boilerplate
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public static final CassandraServiceFactory factory = ServiceHelper.loadFactory(CassandraServiceFactory.class);

    public static CassandraService create(final Vertx vertx, final JsonObject config) {
        return factory.create(vertx, config);
    }

    public static CassandraService createEventBusProxy(final Vertx vertx, final String address) {
        return factory.createEventBusProxy(vertx, address);
    }


    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Service Contracts
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Start the Cassandra cluster.
     */
    public void start();

    /**
     * Stop the Cassandra cluster.
     */
    public void stop();

    /**
     * Test that the current Cassandra service is operational. Useful for simple database health checking.
     *
     * @param resultHandler The asynchronous callback handler.
     */
    public void test(final Handler<AsyncResult<String>> resultHandler);

    /**
     * Executes a raw statement against the Cassandra cluster.
     *
     * @param statement The raw CQL statement to execute.
     * @param resultHandler The asynchronous callback handler.
     */
    public void executeRaw(final String statement, final Handler<AsyncResult<JsonObject>> resultHandler);
}
