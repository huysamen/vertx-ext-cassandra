package org.huysamen.vertx.ext.cassandra;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;

/**
 * Verticle to start up a Cassandra service instance.
 *
 * @author <a href="http://nico.huysamen.org">Nicolaas Frederick Huysamen</a>
 * @since 1.0
 * @version 1.0
 */
public class CassandraServiceVerticle extends AbstractVerticle {

    private CassandraService service;

    @Override
    public void start() throws Exception {
        final JsonObject config = vertx.context().config();

        service = CassandraService.create(vertx, config);
        service.start();

        vertx.eventBus()
                .registerService(service, config.getString("vertx.cassandra", "org.huysamen.vertx.ext.cassandra"));
    }
}
