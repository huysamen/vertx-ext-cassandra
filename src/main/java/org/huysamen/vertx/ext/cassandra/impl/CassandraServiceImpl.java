package org.huysamen.vertx.ext.cassandra.impl;

import com.datastax.driver.core.*;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.vertx.core.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.huysamen.vertx.ext.cassandra.CassandraService;
import org.huysamen.vertx.ext.cassandra.conf.CassandraConfiguration;
import org.huysamen.vertx.ext.cassandra.conf.impl.JsonCassandraConfigurationImpl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of the Cassandra service {@link org.huysamen.vertx.ext.cassandra.CassandraService} contract.
 *
 * @author <a href="http://nico.huysamen.org">Nicolaas Frederick Huysamen</a>
 * @since 1.0
 * @version 1.0
 */
public class CassandraServiceImpl implements CassandraService {

    private static final int PROTOCOL_VERSION = 2;

    private final Vertx vertx;
    private final Map<String, PreparedStatement> statementRegistry = new ConcurrentHashMap<>();

    protected Cluster cluster;
    protected Session session;
    protected Metrics metrics;
    protected CassandraConfiguration config;

    public CassandraServiceImpl(final Vertx vertx, final JsonObject config) {
        this.vertx = vertx;
        this.config = new JsonCassandraConfigurationImpl(config);
        this.metrics = new Metrics(this);
    }

    protected Cluster getCluster() {
        return cluster;
    }

    protected CassandraConfiguration getConfig() {
        return config;
    }

    protected boolean isClosed() {
        return session.isClosed();
    }

    @Override
    public void start() {
        final Cluster.Builder clusterBuilder = new Cluster.Builder();

        // Get array of IPs, default to localhost
        final List<String> seeds = config.getSeeds();

        if (seeds == null || seeds.isEmpty()) {
            throw new RuntimeException("Cassandra seeds are missing");
        }

        // Add cassandra cluster contact points
        seeds.forEach(clusterBuilder::addContactPoint);

        // Add policies to cluster builder
        if (config.getLoadBalancingPolicy() != null) {
            clusterBuilder.withLoadBalancingPolicy(config.getLoadBalancingPolicy());
        }

        if (config.getReconnectionPolicy() != null) {
            clusterBuilder.withReconnectionPolicy(config.getReconnectionPolicy());
        }

        // Add pooling options to cluster builder
        if (config.getPoolingOptions() != null) {
            clusterBuilder.withPoolingOptions(config.getPoolingOptions());
        }

        // Add socket options to cluster builder
        if (config.getSocketOptions() != null) {
            clusterBuilder.withSocketOptions(config.getSocketOptions());
        }

        if (config.getQueryOptions() != null) {
            clusterBuilder.withQueryOptions(config.getQueryOptions());
        }

        if (config.getMetricsOptions() != null) {
            if (!config.getMetricsOptions().isJMXReportingEnabled()) {
                clusterBuilder.withoutJMXReporting();
            }
        }

        if (config.getAuthProvider() != null) {
            clusterBuilder.withAuthProvider(config.getAuthProvider());
        }

        // Build cluster and connect
        cluster = clusterBuilder.build();
        reconnect();
    }

    @Override
    public void stop() {
        if (metrics != null) {
            metrics.close();
            metrics = null;
        }

        if (cluster != null) {
            cluster.closeAsync().force();
            cluster = null;
            session = null;
        }
    }

    @Override
    public void reconnect() {
        final Session staleSession = session;

        session = cluster.connect();

        if (staleSession != null) {
            staleSession.closeAsync();
        }

        metrics.afterReconnect();
    }

    @Override
    public void metrics(final Handler<AsyncResult<JsonObject>> handler) {
        if (metrics == null) {
            handler.handle(createAsyncResult(simpleResult("No metrics registered")));
            return;
        }

        if (cluster != null && !cluster.isClosed()) {
            // TODO: Serialize metrics for manual checks
            handler.handle(createAsyncResult(simpleResult("OK: TODO")));
        } else {
            handler.handle(createAsyncResult(simpleResult("Cluster closed")));
        }
    }

    @Override
    public void execute(final String query, final Handler<AsyncResult<JsonObject>> handler) {
        final ResultSetFuture future = session.executeAsync(new SimpleStatement(query));

        Futures.addCallback(future, new FutureCallback<ResultSet>() {
            @Override
            public void onSuccess(final ResultSet resultSet) {
                handler.handle(createAsyncResult(resultSetAsJson(resultSet)));
            }

            @Override
            public void onFailure(final Throwable throwable) {
                handler.handle(createAsyncResult(throwable));
            }
        });
    }

    @Override
    public void prepare(final String name, final String statement, final Handler<AsyncResult<JsonObject>> handler) {
        final ListenableFuture<PreparedStatement> future = session.prepareAsync(statement);

        Futures.addCallback(future, new FutureCallback<PreparedStatement>() {
            @Override
            public void onSuccess(final PreparedStatement preparedStatement) {
                if (statementRegistry.put(name, preparedStatement) == null) {
                    handler.handle(createAsyncResult(simpleResult("Added")));
                } else {
                    handler.handle(createAsyncResult(simpleResult("Updated")));
                }
            }

            @Override
            public void onFailure(final Throwable throwable) {
                handler.handle(createAsyncResult(throwable));
            }
        });
    }

    @Override
    public void prepared(final JsonObject statement, final Handler<AsyncResult<JsonObject>> handler) {
        // TODO
//        final String name = statement.getString("name");
//
//        if (name == null) {
//            handler.handle(createAsyncResult(simpleResult("No name specified")));
//            return;
//        }
//
//        if (!statementRegistry.containsKey(name)) {
//            handler.handle(createAsyncResult(simpleResult("No named statement matching name found")));
//            return;
//        }
//
//        final PreparedStatement preparedStatement = statementRegistry.get(name);
    }

    private AsyncResult<JsonObject> createAsyncResult(final JsonObject result) {
        return new AsyncResult<JsonObject>() {
            @Override
            public JsonObject result() {
                return result;
            }

            @Override
            public Throwable cause() {
                return null;
            }

            @Override
            public boolean succeeded() {
                return true;
            }

            @Override
            public boolean failed() {
                return false;
            }
        };
    }

    private AsyncResult<JsonObject> createAsyncResult(final Throwable error) {
        return new AsyncResult<JsonObject>() {
            @Override
            public JsonObject result() {
                return null;
            }

            @Override
            public Throwable cause() {
                return error;
            }

            @Override
            public boolean succeeded() {
                return false;
            }

            @Override
            public boolean failed() {
                return true;
            }
        };
    }

    private JsonObject simpleResult(final String result) {
        final JsonObject message = new JsonObject();
        message.putString("result", result);
        return message;
    }

    private JsonObject resultSetAsJson(final ResultSet resultSet) {
        final JsonObject result = new JsonObject();
        final JsonArray columnObjects = new JsonArray();
        final JsonArray rowObjects = new JsonArray();

        result.putString("result", "OK");
        result.putNumber("count", resultSet.getAvailableWithoutFetching());
        result.putArray("columns", columnObjects);
        result.putArray("rows", rowObjects);

        int r = 0;
        for (final Row row : resultSet) {
            final ColumnDefinitions columnDefinitions = row.getColumnDefinitions();
            final JsonArray rowObject = new JsonArray();

            for (int i = 0; i < columnDefinitions.size(); i++) {
                if (row.isNull(i)) {
                    continue;
                }

                final JsonObject column = new JsonObject();
                final Object value = columnDefinitions.getType(i).deserialize(row.getBytesUnsafe(i), PROTOCOL_VERSION);

                rowObject.add(value);

                if (r == 0) {
                    column.putString("name", columnDefinitions.getName(i));
                    column.putString("type", columnDefinitions.getType(i).getName().name());
                    columnObjects.add(column);
                }
            }

            rowObjects.add(rowObject);
            r++;
        }

        return result;
    }
}
