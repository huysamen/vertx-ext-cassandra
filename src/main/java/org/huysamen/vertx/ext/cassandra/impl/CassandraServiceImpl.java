package org.huysamen.vertx.ext.cassandra.impl;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.*;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.impl.LoggerFactory;
import org.huysamen.vertx.ext.cassandra.CassandraService;

import java.util.stream.IntStream;

/**
 * Implementation of the Cassandra service contract.
 *
 * @author <a href="http://nico.huysamen.org">Nicolaas Frederick Huysamen</a>
 * @since 1.0
 * @version 1.0
 */
public class CassandraServiceImpl implements CassandraService {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraServiceImpl.class);
    private static final int PROTOCOL_VERSION = 2;

    private final Vertx vertx;
    private final JsonObject config;

    private Cluster cluster;
    private Session session;

    public CassandraServiceImpl(final Vertx vertx, final JsonObject config) {
        this.vertx = vertx;
        this.config = config;
    }

    @Override
    public void start() {
        LOG.info("Connecting to Cassandra cluster...");

        final Cluster.Builder builder = Cluster.builder().withProtocolVersion(PROTOCOL_VERSION);

        // Contact point seeds
        //
        final JsonArray seeds = config.getArray("seeds", new JsonArray("[\"127.0.0.1\"]"));

        for (int i = 0; i < seeds.size(); i++) {
            builder.addContactPoint(seeds.get(i));
        }


        // Port
        //
        builder.withPort(config.getInteger("port", 9042));


        // Credentials
        //
        if (config.containsField("credentials")) {
            final JsonObject credentials = config.getObject("credentials");

            if (credentials != null && credentials.containsField("username") && credentials.containsField("password")) {
                builder.withCredentials(credentials.getString("username"), credentials.getString("password"));
            }
        }


        // SSL
        //
        if (config.getBoolean("ssl", false)) {
            builder.withSSL();
        }


        // Compression
        //
        builder.withCompression(ProtocolOptions.Compression.valueOf(config.getString("compression", "NONE")));


        // Retry policy
        //
        switch (config.getString("retry", "default")) {
            case "downgrading":
                builder.withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE);
                break;

            case "fallthrough":
                builder.withRetryPolicy(FallthroughRetryPolicy.INSTANCE);
                break;

            default:
                builder.withRetryPolicy(Policies.defaultRetryPolicy());
                break;
        }


        // Reconnection policy
        //
        if (config.containsField("reconnection")) {
            final JsonObject reconnection = config.getObject("reconnection");

            if (reconnection == null
                    || reconnection.getString("policy") == null
                    || reconnection.getLong("delay") == null) {

                builder.withReconnectionPolicy(Policies.defaultReconnectionPolicy());
            } else {
                final long delay = reconnection.getLong("delay", 500);
                final long max = reconnection.getLong("max", 30000);

                switch (reconnection.getString("policy", "default")) {
                    case "constant":
                        builder.withReconnectionPolicy(new ConstantReconnectionPolicy(delay));
                        break;

                    case "exponential":
                        builder.withReconnectionPolicy(new ExponentialReconnectionPolicy(delay, max));
                        break;

                    default:
                        builder.withReconnectionPolicy(Policies.defaultReconnectionPolicy());
                        break;
                }
            }
        }

        cluster = builder.build();

        final Metadata metadata = cluster.getMetadata();
        session = cluster.connect();

        LOG.info("Connected to Cassandra cluster: " + metadata.getClusterName());
    }

    @Override
    public void stop() {
        cluster.close();
    }

    @Override
    public void test(final Handler<AsyncResult<String>> resultHandler) {
        if (cluster != null && !cluster.isClosed()) {
            resultHandler.handle(Future.completedFuture("OK"));
        } else {
            resultHandler.handle(Future.completedFuture("BAD"));
        }
    }

    @Override
    public void executeRaw(final String query, final Handler<AsyncResult<JsonObject>> resultHandler) {
        final Statement statement = new SimpleStatement(query);
        final ResultSetFuture futureResult = session.executeAsync(statement);

        Futures.addCallback(futureResult, new FutureCallback<ResultSet>() {
            @Override
            public void onSuccess(final ResultSet rows) {
                if (rows.getAvailableWithoutFetching() <= 0) {
                    resultHandler.handle(Future.completedFuture(resultOk()));
                } else {
                    resultHandler.handle(Future.completedFuture(resultToJson(rows)));
                }
            }

            @Override
            public void onFailure(final Throwable throwable) {
                resultHandler.handle(Future.completedFuture(throwable));
            }
        });
    }

    private JsonObject resultOk() {
        final JsonObject result = new JsonObject();

        result.putString("result", "OK");

        return result;
    }

    private JsonObject resultToJson(final ResultSet results) {
        final JsonObject resultMessage = new JsonObject();
        final JsonArray resultRows = new JsonArray();

        resultMessage.putString("result", "OK");
        resultMessage.putNumber("count", results.getAvailableWithoutFetching());

        results.all()
                .stream()
                .forEach((row) -> {
                    final JsonArray resultRowColumns = new JsonArray();
                    final ColumnDefinitions columns = row.getColumnDefinitions();

                    IntStream.range(0, columns.size())
                            .filter((idx) -> !row.isNull(idx))
                            .forEach((idx) -> {
                                final JsonObject value = new JsonObject();

                                value.putString("keyspace", columns.getKeyspace(idx));
                                value.putString("column", columns.getName(idx));
                                value.putString("type", columns.getType(idx).toString());
                                value.putValue("value", columns.getType(idx).deserialize(row.getBytesUnsafe(idx), PROTOCOL_VERSION));

                                resultRowColumns.add(value);
                            });

                    resultRows.add(resultRowColumns);
                });

        resultMessage.putArray("values", resultRows);

        return resultMessage;
    }
}
