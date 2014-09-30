package org.huysamen.vertx.ext.cassandra.impl;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.huysamen.vertx.ext.cassandra.CassandraService;
import org.huysamen.vertx.ext.cassandra.CassandraServiceFactory;

/**
 * Implementation of the Cassandra service factory contract.
 *
 * @author <a href="http://nico.huysamen.org">Nicolaas Frederick Huysamen</a>
 * @since 1.0
 * @version 1.0
 */
public class CassandraServiceFactoryImpl implements CassandraServiceFactory {

    @Override
    public CassandraService create(final Vertx vertx, final JsonObject config) {
        return new CassandraServiceImpl(vertx, config);
    }

    @Override
    public CassandraService createEventBusProxy(final Vertx vertx, final String address) {
        return vertx.eventBus().createProxy(CassandraService.class, address);
    }
}
