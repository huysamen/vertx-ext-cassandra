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
     * Reconnects to the cluster.
     */
    public void reconnect();

    /**
     * Test that the current Cassandra service is operational. Useful for simple database health checking.
     *
     * @param resultHandler The asynchronous callback handler.
     */
    public void metrics(final Handler<AsyncResult<JsonObject>> handler);

    /**
     * Executes a raw statement asynchronously against the Cassandra cluster.
     *
     * @param statement The raw CQL statement to execute.
     * @param resultHandler The asynchronous callback handler.
     */
    public void execute(final String statement, final Handler<AsyncResult<JsonObject>> handler);

    /**
     * Prepare a named statement. This will be stored in the service for future use.
     *
     * @param name The name of the statement.
     * @param statement The actual statement to prepare.
     * @param handler The asynchronous callback handler.
     */
    public void prepare(final String name, final String statement, final Handler<AsyncResult<JsonObject>> handler);

    /**
     * Execute a previously prepared named statement.
     *
     * @param statement The message containing the name and values of the statement.
     * @param handler The asynchronous callback handler.
     */
    public void prepared(final JsonObject statement, final Handler<AsyncResult<JsonObject>> handler);
}
