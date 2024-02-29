/*
 * Copyright 2023 asyncer.io projects
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.asyncer.r2dbc.mysql;

import io.asyncer.r2dbc.mysql.api.MySqlBatch;
import io.asyncer.r2dbc.mysql.api.MySqlTransactionDefinition;
import io.asyncer.r2dbc.mysql.authentication.MySqlAuthProvider;
import io.asyncer.r2dbc.mysql.cache.PrepareCache;
import io.asyncer.r2dbc.mysql.client.Client;
import io.asyncer.r2dbc.mysql.client.FluxExchangeable;
import io.asyncer.r2dbc.mysql.constant.CompressionAlgorithm;
import io.asyncer.r2dbc.mysql.constant.ServerStatuses;
import io.asyncer.r2dbc.mysql.constant.SslMode;
import io.asyncer.r2dbc.mysql.internal.util.StringUtils;
import io.asyncer.r2dbc.mysql.message.client.AuthResponse;
import io.asyncer.r2dbc.mysql.message.client.ClientMessage;
import io.asyncer.r2dbc.mysql.message.client.HandshakeResponse;
import io.asyncer.r2dbc.mysql.message.client.LocalInfileResponse;
import io.asyncer.r2dbc.mysql.message.client.SubsequenceClientMessage;
import io.asyncer.r2dbc.mysql.message.client.PingMessage;
import io.asyncer.r2dbc.mysql.message.client.PrepareQueryMessage;
import io.asyncer.r2dbc.mysql.message.client.PreparedCloseMessage;
import io.asyncer.r2dbc.mysql.message.client.PreparedExecuteMessage;
import io.asyncer.r2dbc.mysql.message.client.PreparedFetchMessage;
import io.asyncer.r2dbc.mysql.message.client.PreparedResetMessage;
import io.asyncer.r2dbc.mysql.message.client.PreparedTextQueryMessage;
import io.asyncer.r2dbc.mysql.message.client.SslRequest;
import io.asyncer.r2dbc.mysql.message.client.TextQueryMessage;
import io.asyncer.r2dbc.mysql.message.server.AuthMoreDataMessage;
import io.asyncer.r2dbc.mysql.message.server.ChangeAuthMessage;
import io.asyncer.r2dbc.mysql.message.server.CompleteMessage;
import io.asyncer.r2dbc.mysql.message.server.EofMessage;
import io.asyncer.r2dbc.mysql.message.server.ErrorMessage;
import io.asyncer.r2dbc.mysql.message.server.HandshakeHeader;
import io.asyncer.r2dbc.mysql.message.server.HandshakeRequest;
import io.asyncer.r2dbc.mysql.message.server.LocalInfileRequest;
import io.asyncer.r2dbc.mysql.message.server.OkMessage;
import io.asyncer.r2dbc.mysql.message.server.PreparedOkMessage;
import io.asyncer.r2dbc.mysql.message.server.ServerMessage;
import io.asyncer.r2dbc.mysql.message.server.ServerStatusMessage;
import io.asyncer.r2dbc.mysql.message.server.SyntheticMetadataMessage;
import io.asyncer.r2dbc.mysql.message.server.SyntheticSslResponseMessage;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcPermissionDeniedException;
import io.r2dbc.spi.TransactionDefinition;
import org.jetbrains.annotations.Nullable;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.SynchronousSink;
import reactor.util.concurrent.Queues;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * A message flow considers both of parametrized and text queries, such as {@link TextParametrizedStatement},
 * {@link PrepareParametrizedStatement}, {@link TextSimpleStatement}, {@link PrepareSimpleStatement} and
 * {@link MySqlBatch}.
 */
final class QueryFlow {

    static final InternalLogger logger = InternalLoggerFactory.getInstance(QueryFlow.class);

    // Metadata EOF message will be not receive in here.
    private static final Predicate<ServerMessage> RESULT_DONE = message -> message instanceof CompleteMessage;

    private static final Consumer<ServerMessage> EXECUTE_VOID = message -> {
        if (message instanceof ErrorMessage) {
            throw ((ErrorMessage) message).toException();
        } else if (message instanceof ReferenceCounted) {
            ReferenceCountUtil.safeRelease(message);
        }
    };

    /**
     * Execute multiple bindings of a server-preparing statement with one-by-one binary execution. The
     * execution terminates with the last {@link CompleteMessage} or a {@link ErrorMessage}. If client
     * receives a {@link ErrorMessage} will cancel subsequent {@link Binding}s. The exchange will be completed
     * by {@link CompleteMessage} after receive the last result for the last binding.
     *
     * @param client    the {@link Client} to exchange messages with.
     * @param sql       the statement for exception tracing.
     * @param bindings  the data of bindings.
     * @param fetchSize the size of fetching, if it less than or equal to {@literal 0} means fetch all rows.
     * @param cache     the cache of server-preparing result.
     * @return the messages received in response to this exchange.
     */
    static Flux<Flux<ServerMessage>> execute(Client client, String sql, List<Binding> bindings, int fetchSize,
        PrepareCache cache) {
        return Flux.defer(() -> {
            if (bindings.isEmpty()) {
                return Flux.empty();
            }

            // Note: the prepared SQL may not be sent when the cache matches.
            return client.exchange(new PrepareExchangeable(cache, sql, bindings.iterator(), fetchSize))
                .windowUntil(RESULT_DONE);
        });
    }

    /**
     * Execute multiple bindings of a client-preparing statement with one-by-one text query. The execution
     * terminates with the last {@link CompleteMessage} or a {@link ErrorMessage}. The {@link ErrorMessage}
     * will emit an exception and cancel subsequent {@link Binding}s. This exchange will be completed by
     * {@link CompleteMessage} after receive the last result for the last binding.
     *
     * @param client    the {@link Client} to exchange messages with.
     * @param query     the {@link Query} for synthetic client-preparing statement.
     * @param returning the {@code RETURNING} identifiers.
     * @param bindings  the data of bindings.
     * @return the messages received in response to this exchange.
     */
    static Flux<Flux<ServerMessage>> execute(
        Client client, Query query, String returning, List<Binding> bindings
    ) {
        return Flux.defer(() -> {
            if (bindings.isEmpty()) {
                return Flux.empty();
            }

            return client.exchange(new TextQueryExchangeable(query, returning, bindings.iterator()))
                .windowUntil(RESULT_DONE);
        });
    }

    /**
     * Execute a simple compound query. Query execution terminates with the last {@link CompleteMessage} or a
     * {@link ErrorMessage}. The {@link ErrorMessage} will emit an exception. The exchange will be completed
     * by {@link CompleteMessage} after receive the last result for the last binding.
     *
     * @param client the {@link Client} to exchange messages with.
     * @param sql    the query to execute, can be contains multi-statements.
     * @return the messages received in response to this exchange.
     */
    static Flux<Flux<ServerMessage>> execute(Client client, String sql) {
        return Flux.defer(() -> execute0(client, sql).windowUntil(RESULT_DONE));
    }

    /**
     * Execute multiple simple compound queries with one-by-one. Query execution terminates with the last
     * {@link CompleteMessage} or a {@link ErrorMessage}. The {@link ErrorMessage} will emit an exception and
     * cancel subsequent statements' execution. The exchange will be completed by {@link CompleteMessage}
     * after receive the last result for the last binding.
     *
     * @param client     the {@link Client} to exchange messages with.
     * @param statements bundled sql for execute.
     * @return the messages received in response to this exchange.
     */
    static Flux<Flux<ServerMessage>> execute(Client client, List<String> statements) {
        return Flux.defer(() -> {
            switch (statements.size()) {
                case 0:
                    return Flux.empty();
                case 1:
                    return execute0(client, statements.get(0)).windowUntil(RESULT_DONE);
                default:
                    return client.exchange(new MultiQueryExchangeable(statements.iterator()))
                        .windowUntil(RESULT_DONE);
            }
        });
    }

    /**
     * Login a {@link Client} and receive the {@code client} after logon. It will emit an exception when
     * client receives a {@link ErrorMessage}.
     *
     * @param client                the {@link Client} to exchange messages with.
     * @param sslMode               the {@link SslMode} defines SSL capability and behavior.
     * @param database              the database that will be connected.
     * @param user                  the user that will be login.
     * @param password              the password of the {@code user}.
     * @param compressionAlgorithms the list of compression algorithms.
     * @param zstdCompressionLevel  the zstd compression level.
     * @param context               the {@link ConnectionContext} for initialization.
     * @return the messages received in response to the login exchange.
     */
    static Mono<Client> login(Client client, SslMode sslMode, String database, String user,
        @Nullable CharSequence password,
        Set<CompressionAlgorithm> compressionAlgorithms, int zstdCompressionLevel,
        ConnectionContext context) {
        return client.exchange(new LoginExchangeable(client, sslMode, database, user, password,
                compressionAlgorithms, zstdCompressionLevel, context))
            .onErrorResume(e -> client.forceClose().then(Mono.error(e)))
            .then(Mono.just(client));
    }

    /**
     * Execute a simple query and return a {@link Mono} for the complete signal or error. Query execution
     * terminates with the last {@link CompleteMessage} or a {@link ErrorMessage}. The {@link ErrorMessage}
     * will emit an exception. The exchange will be completed by {@link CompleteMessage} after receive the
     * last result for the last binding.
     * <p>
     * Note: this method does not support {@code LOCAL INFILE} due to it should be used for excepted queries.
     *
     * @param client the {@link Client} to exchange messages with.
     * @param sql    the query to execute, can be contains multi-statements.
     * @return receives complete signal.
     */
    static Mono<Void> executeVoid(Client client, String sql) {
        return Mono.defer(() -> client.<ServerMessage>exchange(new TextQueryMessage(sql), (message, sink) -> {
            if (message instanceof ErrorMessage) {
                sink.next(((ErrorMessage) message).offendedBy(sql));
                sink.complete();
            } else {
                sink.next(message);

                if (message instanceof CompleteMessage && ((CompleteMessage) message).isDone()) {
                    sink.complete();
                }
            }
        }).doOnSubscribe(ignored -> QueryLogger.log(sql)).doOnNext(EXECUTE_VOID).then());
    }

    /**
     * Begins a new transaction with a {@link TransactionDefinition}.  It will change current transaction
     * statuses of the {@link ConnectionState}.
     *
     * @param client         the {@link Client} to exchange messages with.
     * @param state          the connection state for checks and sets transaction statuses.
     * @param batchSupported if connection supports batch query.
     * @param definition     the {@link TransactionDefinition}.
     * @return receives complete signal.
     */
    static Mono<Void> beginTransaction(Client client, ConnectionState state, boolean batchSupported,
        TransactionDefinition definition) {
        final StartTransactionState startState = new StartTransactionState(state, definition);

        if (batchSupported) {
            return client.exchange(new TransactionBatchExchangeable(startState)).then();
        }

        return client.exchange(new TransactionMultiExchangeable(startState)).then();
    }

    /**
     * Commits or rollbacks current transaction.  It will recover statuses of the {@link ConnectionState} in
     * the initial connection state.
     *
     * @param client         the {@link Client} to exchange messages with.
     * @param state          the connection state for checks and resets transaction statuses.
     * @param commit         if it is commit, otherwise rollback.
     * @param batchSupported if connection supports batch query.
     * @return receives complete signal.
     */
    static Mono<Void> doneTransaction(Client client, ConnectionState state, boolean commit,
        boolean batchSupported) {
        final CommitRollbackState commitState = new CommitRollbackState(state, commit);

        if (batchSupported) {
            return client.exchange(new TransactionBatchExchangeable(commitState)).then();
        }

        return client.exchange(new TransactionMultiExchangeable(commitState)).then();
    }

    static Mono<Void> createSavepoint(Client client, ConnectionState state, String name,
        boolean batchSupported) {
        final CreateSavepointState savepointState = new CreateSavepointState(state, name);
        if (batchSupported) {
            return client.exchange(new TransactionBatchExchangeable(savepointState)).then();
        }
        return client.exchange(new TransactionMultiExchangeable(savepointState)).then();
    }

    /**
     * Execute a simple query statement. Query execution terminates with the last {@link CompleteMessage} or a
     * {@link ErrorMessage}. The {@link ErrorMessage} will emit an exception. The exchange will be completed
     * by {@link CompleteMessage} after receive the last result for the last binding. The exchange will be
     * completed by {@link CompleteMessage} after receive the last result for the last binding.
     *
     * @param client the {@link Client} to exchange messages with.
     * @param sql    the query to execute, can be contains multi-statements.
     * @return the messages received in response to this exchange.
     */
    private static Flux<ServerMessage> execute0(Client client, String sql) {
        return client.exchange(new SimpleQueryExchangeable(sql));
    }

    private QueryFlow() { }
}

/**
 * An abstraction of {@link FluxExchangeable} that considers multi-queries without binary protocols.
 */
abstract class BaseFluxExchangeable extends FluxExchangeable<ServerMessage> {

    protected final Sinks.Many<ClientMessage> requests = Sinks.many().unicast()
        .onBackpressureBuffer(Queues.<ClientMessage>one().get());

    @Override
    public final void subscribe(CoreSubscriber<? super ClientMessage> actual) {
        requests.asFlux().subscribe(actual);
        tryNextOrComplete(null);
    }

    @Override
    public final void accept(ServerMessage message, SynchronousSink<ServerMessage> sink) {
        if (message instanceof ErrorMessage) {
            sink.next(((ErrorMessage) message).offendedBy(offendingSql()));
            sink.complete();
        } else if (message instanceof LocalInfileRequest) {
            LocalInfileRequest request = (LocalInfileRequest) message;
            String path = request.getPath();

            QueryLogger.logLocalInfile(path);

            requests.emitNext(
                new LocalInfileResponse(path, sink),
                Sinks.EmitFailureHandler.FAIL_FAST
            );
        } else {
            sink.next(message);

            if (message instanceof CompleteMessage && ((CompleteMessage) message).isDone()) {
                tryNextOrComplete(sink);
            }
        }
    }

    protected abstract void tryNextOrComplete(@Nullable SynchronousSink<ServerMessage> sink);

    protected abstract String offendingSql();
}

final class SimpleQueryExchangeable extends BaseFluxExchangeable {

    private static final int INIT = 0;

    private static final int EXECUTE = 1;

    private static final int DISPOSE = 2;

    private final AtomicInteger state = new AtomicInteger(INIT);

    private final String sql;

    SimpleQueryExchangeable(String sql) {
        this.sql = sql;
    }

    @Override
    public void dispose() {
        if (state.getAndSet(DISPOSE) != DISPOSE) {
            requests.tryEmitComplete();
        }
    }

    @Override
    public boolean isDisposed() {
        return state.get() == DISPOSE;
    }

    @Override
    protected void tryNextOrComplete(@Nullable SynchronousSink<ServerMessage> sink) {
        if (state.compareAndSet(INIT, EXECUTE)) {
            QueryLogger.log(sql);

            Sinks.EmitResult result = requests.tryEmitNext(new TextQueryMessage(sql));

            if (result == Sinks.EmitResult.OK) {
                return;
            }

            QueryFlow.logger.error("Emit request failed due to {}", result);
        }

        if (sink != null) {
            sink.complete();
        }
    }

    @Override
    protected String offendingSql() {
        return sql;
    }
}

/**
 * An implementation of {@link FluxExchangeable} that considers client-preparing requests.
 */
final class TextQueryExchangeable extends BaseFluxExchangeable {

    private final AtomicBoolean disposed = new AtomicBoolean();

    private final Query query;

    private final String returning;

    private final Iterator<Binding> bindings;

    TextQueryExchangeable(Query query, String returning, Iterator<Binding> bindings) {
        this.query = query;
        this.returning = returning;
        this.bindings = bindings;
    }

    @Override
    public void dispose() {
        if (disposed.compareAndSet(false, true)) {
            // No particular error condition handling for complete signal.
            requests.tryEmitComplete();

            while (bindings.hasNext()) {
                bindings.next().clear();
            }
        }
    }

    @Override
    public boolean isDisposed() {
        return disposed.get();
    }

    @Override
    protected void tryNextOrComplete(@Nullable SynchronousSink<ServerMessage> sink) {
        if (this.bindings.hasNext()) {
            PreparedTextQueryMessage message = this.bindings.next().toTextMessage(this.query, this.returning);
            Sinks.EmitResult result = this.requests.tryEmitNext(message);

            if (result == Sinks.EmitResult.OK) {
                return;
            }

            QueryFlow.logger.error("Emit request failed due to {}", result);
            message.dispose();
        }

        if (sink != null) {
            sink.complete();
        }
    }

    @Override
    protected String offendingSql() {
        return StringUtils.extendReturning(query.getFormattedSql(), returning);
    }
}

/**
 * An implementation of {@link FluxExchangeable} that considers multiple simple statements.
 */
final class MultiQueryExchangeable extends BaseFluxExchangeable {

    private final Iterator<String> statements;

    private String current;

    MultiQueryExchangeable(Iterator<String> statements) {
        this.statements = statements;
    }

    @Override
    public void dispose() {
        // No particular error condition handling for complete signal.
        requests.tryEmitComplete();
    }

    @Override
    public boolean isDisposed() {
        return requests.scanOrDefault(Scannable.Attr.TERMINATED, Boolean.FALSE);
    }

    @Override
    protected void tryNextOrComplete(@Nullable SynchronousSink<ServerMessage> sink) {
        if (this.statements.hasNext()) {
            String current = this.statements.next();

            QueryLogger.log(current);
            this.current = current;

            Sinks.EmitResult result = this.requests.tryEmitNext(new TextQueryMessage(current));

            if (result == Sinks.EmitResult.OK) {
                return;
            }

            QueryFlow.logger.error("Emit request failed due to {}", result);
        }

        if (sink != null) {
            sink.complete();
        }
    }

    @Override
    protected String offendingSql() {
        return current;
    }
}

/**
 * An implementation of {@link FluxExchangeable} that considers server-preparing queries. Which contains a
 * built-in state machine.
 * <p>
 * It will reset a prepared statement if cache has matched it, otherwise it will prepare statement to a new
 * statement ID and put the ID into the cache. If the statement ID does not exist in the cache after the last
 * row sent, the ID will be closed.
 */
final class PrepareExchangeable extends FluxExchangeable<ServerMessage> {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(PrepareExchangeable.class);

    private static final int PREPARE_OR_RESET = 1;

    private static final int EXECUTE = 2;

    private static final int FETCH = 3;

    private final AtomicBoolean disposed = new AtomicBoolean();

    private final Sinks.Many<ClientMessage> requests = Sinks.many().unicast()
        .onBackpressureBuffer(Queues.<ClientMessage>one().get());

    private final PrepareCache cache;

    private final String sql;

    private final Iterator<Binding> bindings;

    private final int fetchSize;

    private int mode = PREPARE_OR_RESET;

    @Nullable
    private Integer statementId;

    private boolean shouldClose;

    PrepareExchangeable(PrepareCache cache, String sql, Iterator<Binding> bindings, int fetchSize) {
        this.cache = cache;
        this.sql = sql;
        this.bindings = bindings;
        this.fetchSize = fetchSize;
    }

    @Override
    public void subscribe(CoreSubscriber<? super ClientMessage> actual) {
        // It is also initialization method.
        requests.asFlux().subscribe(actual);

        // After subscribe.
        Integer statementId = cache.getIfPresent(sql);
        if (statementId == null) {
            logger.debug("Prepare cache mismatch, try to preparing");
            this.shouldClose = true;
            QueryLogger.log(sql);
            Sinks.EmitResult result = this.requests.tryEmitNext(new PrepareQueryMessage(sql));

            if (result != Sinks.EmitResult.OK) {
                logger.error("Fail to emit prepare query message due to {}", result);
            }
        } else {
            logger.debug("Prepare cache matched statement {} when getting", statementId);
            // Should reset only when it comes from cache.
            this.shouldClose = false;
            this.statementId = statementId;
            QueryLogger.log(statementId, sql);
            Sinks.EmitResult result = this.requests.tryEmitNext(new PreparedResetMessage(statementId));

            if (result != Sinks.EmitResult.OK) {
                logger.error("Fail to emit reset statement message due to {}", result);
            }
        }
    }

    @Override
    public void accept(ServerMessage message, SynchronousSink<ServerMessage> sink) {
        if (message instanceof ErrorMessage) {
            sink.next(((ErrorMessage) message).offendedBy(sql));
            sink.complete();
            return;
        }

        switch (mode) {
            case PREPARE_OR_RESET:
                if (message instanceof OkMessage) {
                    // Reset succeed.
                    Integer statementId = this.statementId;
                    if (statementId == null) {
                        logger.error("Reset succeed but statement ID was null");
                        return;
                    }

                    doNextExecute(statementId, sink);
                } else if (message instanceof PreparedOkMessage) {
                    PreparedOkMessage ok = (PreparedOkMessage) message;
                    int statementId = ok.getStatementId();
                    int columns = ok.getTotalColumns();
                    int parameters = ok.getTotalParameters();

                    this.statementId = statementId;
                    QueryLogger.log(statementId, sql);

                    // columns + parameters <= 0, has not metadata follow in,
                    if (columns <= -parameters) {
                        putToCache(statementId);
                        doNextExecute(statementId, sink);
                    }
                } else if (message instanceof SyntheticMetadataMessage &&
                    ((SyntheticMetadataMessage) message).isCompleted()) {
                    Integer statementId = this.statementId;
                    if (statementId == null) {
                        logger.error("Prepared OK message not found");
                        return;
                    }

                    putToCache(statementId);
                    doNextExecute(statementId, sink);
                } else {
                    ReferenceCountUtil.safeRelease(message);
                }
                // Ignore all messages in preparing phase.
                break;
            case EXECUTE:
                if (message instanceof CompleteMessage && ((CompleteMessage) message).isDone()) {
                    // Complete message means execute or fetch phase done (when cursor is not opened).
                    onCompleteMessage((CompleteMessage) message, sink);
                } else if (message instanceof SyntheticMetadataMessage) {
                    EofMessage eof = ((SyntheticMetadataMessage) message).getEof();
                    if (eof instanceof ServerStatusMessage) {
                        // Otherwise, cursor does not be opened, wait for end of row EOF message.
                        if ((((ServerStatusMessage) eof).getServerStatuses() &
                            ServerStatuses.CURSOR_EXISTS) != 0) {
                            if (doNextFetch(sink)) {
                                sink.next(message);
                            }

                            break;
                        }
                    }
                    // EOF is deprecated (null) or using EOF without statuses.
                    // EOF is deprecated: wait for OK message.
                    // EOF without statuses: means cursor does not be opened, wait for end of row EOF message.
                    // Metadata message should be always emitted in EXECUTE phase.
                    setMode(FETCH);
                    sink.next(message);
                } else {
                    sink.next(message);
                }

                break;
            default:
                if (message instanceof CompleteMessage && ((CompleteMessage) message).isDone()) {
                    onCompleteMessage((CompleteMessage) message, sink);
                } else {
                    sink.next(message);
                }
                break;
        }
    }

    @Override
    public void dispose() {
        if (disposed.compareAndSet(false, true)) {
            Integer statementId = this.statementId;
            if (shouldClose && statementId != null) {
                logger.debug("Closing statement {} after used", statementId);

                Sinks.EmitResult result = requests.tryEmitNext(new PreparedCloseMessage(statementId));

                if (result != Sinks.EmitResult.OK) {
                    logger.error("Fail to close statement {} due to {}", statementId, result);
                }
            }
            // No particular error condition handling for complete signal.
            requests.tryEmitComplete();

            while (bindings.hasNext()) {
                bindings.next().clear();
            }
        }
    }

    @Override
    public boolean isDisposed() {
        return disposed.get();
    }

    private void putToCache(Integer statementId) {
        boolean putSucceed;

        try {
            putSucceed = cache.putIfAbsent(sql, statementId, evictId -> {
                logger.debug("Prepare cache evicts statement {} when putting", evictId);

                Sinks.EmitResult result = requests.tryEmitNext(new PreparedCloseMessage(evictId));

                if (result != Sinks.EmitResult.OK) {
                    logger.error("Fail to close evicted statement {} due to {}", statementId, result);
                }
            });
        } catch (Throwable e) {
            logger.error("Put statement {} to cache failed", statementId, e);
            putSucceed = false;
        }

        // If put failed, should close it.
        this.shouldClose = !putSucceed;
        logger.debug("Prepare cache put statement {} is {}", statementId, putSucceed ? "succeed" : "fails");
    }

    private void doNextExecute(int statementId, SynchronousSink<ServerMessage> sink) {
        setMode(EXECUTE);

        PreparedExecuteMessage message = bindings.next().toExecuteMessage(statementId, fetchSize <= 0);
        Sinks.EmitResult result = requests.tryEmitNext(message);

        if (result != Sinks.EmitResult.OK) {
            logger.error("Fail to execute {} due to {}", statementId, result);
            message.dispose();
            sink.complete();
        }
    }

    private boolean doNextFetch(SynchronousSink<ServerMessage> sink) {
        Integer statementId = this.statementId;

        if (statementId == null) {
            sink.error(new IllegalStateException("Statement ID must not be null when fetching"));
            return false;
        }

        setMode(FETCH);

        Sinks.EmitResult result = requests.tryEmitNext(new PreparedFetchMessage(statementId, fetchSize));

        if (result == Sinks.EmitResult.OK) {
            return true;
        }

        logger.error("Fail to fetch {} due to {}", statementId, result);
        sink.complete();

        return false;
    }

    private void setMode(int mode) {
        logger.debug("Mode is changed to {}", mode == EXECUTE ? "EXECUTE" : "FETCH");
        this.mode = mode;
    }

    private void onCompleteMessage(CompleteMessage message, SynchronousSink<ServerMessage> sink) {
        if (requests.scanOrDefault(Scannable.Attr.TERMINATED, Boolean.FALSE)) {
            logger.error("Unexpected terminated on requests");
            sink.next(message);
            sink.complete();
            return;
        }

        if (message instanceof ServerStatusMessage) {
            short statuses = ((ServerStatusMessage) message).getServerStatuses();
            if ((statuses & ServerStatuses.CURSOR_EXISTS) != 0 &&
                (statuses & ServerStatuses.LAST_ROW_SENT) == 0) {
                doNextFetch(sink);
                // Not last complete message, no need emit.
                return;
            }
            // Otherwise, it is last row sent or did not open cursor.
        }

        // The last row complete message should be emitted, whatever cursor has been opened.
        sink.next(message);

        if (bindings.hasNext()) {
            Integer statementId = this.statementId;

            if (statementId == null) {
                sink.error(new IllegalStateException("Statement ID must not be null when executing"));
                return;
            }

            doNextExecute(statementId, sink);
        } else {
            sink.complete();
        }
    }
}

/**
 * An implementation of {@link FluxExchangeable} that considers login to the database.
 * <p>
 * Not like other {@link FluxExchangeable}s, it is started by a server-side message, which should be an
 * implementation of {@link HandshakeRequest}.
 */
final class LoginExchangeable extends FluxExchangeable<Void> {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(LoginExchangeable.class);

    private static final Map<String, String> ATTRIBUTES = Collections.emptyMap();

    private static final String CLI_SPECIFIC = "HY000";

    private static final int HANDSHAKE_VERSION = 10;

    private final Sinks.Many<SubsequenceClientMessage> requests = Sinks.many().unicast()
        .onBackpressureBuffer(Queues.<SubsequenceClientMessage>one().get());

    private final Client client;

    private final SslMode sslMode;

    private final String database;

    private final String user;

    @Nullable
    private final CharSequence password;

    private final Set<CompressionAlgorithm> compressions;

    private final int zstdCompressionLevel;

    private final ConnectionContext context;

    private boolean handshake = true;

    private MySqlAuthProvider authProvider;

    private byte[] salt;

    private boolean sslCompleted;

    LoginExchangeable(Client client, SslMode sslMode, String database, String user,
        @Nullable CharSequence password, Set<CompressionAlgorithm> compressions,
        int zstdCompressionLevel, ConnectionContext context) {
        this.client = client;
        this.sslMode = sslMode;
        this.database = database;
        this.user = user;
        this.password = password;
        this.compressions = compressions;
        this.zstdCompressionLevel = zstdCompressionLevel;
        this.context = context;
        this.sslCompleted = sslMode == SslMode.TUNNEL;
    }

    @Override
    public void subscribe(CoreSubscriber<? super ClientMessage> actual) {
        requests.asFlux().subscribe(actual);
    }

    @Override
    public void accept(ServerMessage message, SynchronousSink<Void> sink) {
        if (message instanceof ErrorMessage) {
            sink.error(((ErrorMessage) message).toException());
            return;
        }

        // Ensures it will be initialized only once.
        if (handshake) {
            handshake = false;
            if (message instanceof HandshakeRequest) {
                HandshakeRequest request = (HandshakeRequest) message;
                Capability capability = initHandshake(request);

                if (capability.isSslEnabled()) {
                    emitNext(SslRequest.from(capability, context.getClientCollation().getId()), sink);
                } else {
                    emitNext(createHandshakeResponse(capability), sink);
                }
            } else {
                sink.error(new R2dbcPermissionDeniedException("Unexpected message type '" +
                    message.getClass().getSimpleName() + "' in init phase"));
            }

            return;
        }

        if (message instanceof OkMessage) {
            client.loginSuccess();
            sink.complete();
        } else if (message instanceof SyntheticSslResponseMessage) {
            sslCompleted = true;
            emitNext(createHandshakeResponse(context.getCapability()), sink);
        } else if (message instanceof AuthMoreDataMessage) {
            AuthMoreDataMessage msg = (AuthMoreDataMessage) message;

            if (msg.isFailed()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Connection (id {}) fast authentication failed, use full authentication",
                        context.getConnectionId());
                }

                emitNext(createAuthResponse("full authentication"), sink);
            }
            // Otherwise success, wait until OK message or Error message.
        } else if (message instanceof ChangeAuthMessage) {
            ChangeAuthMessage msg = (ChangeAuthMessage) message;

            authProvider = MySqlAuthProvider.build(msg.getAuthType());
            salt = msg.getSalt();
            emitNext(createAuthResponse("change authentication"), sink);
        } else {
            sink.error(new R2dbcPermissionDeniedException("Unexpected message type '" +
                message.getClass().getSimpleName() + "' in login phase"));
        }
    }

    @Override
    public void dispose() {
        // No particular error condition handling for complete signal.
        this.requests.tryEmitComplete();
    }

    private void emitNext(SubsequenceClientMessage message, SynchronousSink<Void> sink) {
        Sinks.EmitResult result = requests.tryEmitNext(message);

        if (result != Sinks.EmitResult.OK) {
            sink.error(new IllegalStateException("Fail to emit a login request due to " + result));
        }
    }

    private AuthResponse createAuthResponse(String phase) {
        MySqlAuthProvider authProvider = getAndNextProvider();

        if (authProvider.isSslNecessary() && !sslCompleted) {
            throw new R2dbcPermissionDeniedException(authFails(authProvider.getType(), phase), CLI_SPECIFIC);
        }

        return new AuthResponse(authProvider.authentication(password, salt, context.getClientCollation()));
    }

    private Capability clientCapability(Capability serverCapability) {
        Capability.Builder builder = serverCapability.mutate();

        builder.disableSessionTrack();
        builder.disableDatabasePinned();
        builder.disableIgnoreAmbiguitySpace();
        builder.disableInteractiveTimeout();

        if (sslMode == SslMode.TUNNEL) {
            // Tunnel does not use MySQL SSL protocol, disable it.
            builder.disableSsl();
        } else if (!serverCapability.isSslEnabled()) {
            // Server unsupported SSL.
            if (sslMode.requireSsl()) {
                throw new R2dbcPermissionDeniedException("Server version '" + context.getServerVersion() +
                    "' does not support SSL but mode '" + sslMode + "' requires SSL", CLI_SPECIFIC);
            } else if (sslMode.startSsl()) {
                // SSL has start yet, and client can disable SSL, disable now.
                client.sslUnsupported();
            }
        } else {
            // The server supports SSL, but the user does not want to use SSL, disable it.
            if (!sslMode.startSsl()) {
                builder.disableSsl();
            }
        }

        if (isZstdAllowed(serverCapability)) {
            if (isZstdSupported()) {
                builder.disableZlibCompression();
            } else {
                logger.warn("Server supports zstd, but zstd-jni dependency is missing");

                if (isZlibAllowed(serverCapability)) {
                    builder.disableZstdCompression();
                } else if (compressions.contains(CompressionAlgorithm.UNCOMPRESSED)) {
                    builder.disableCompression();
                } else {
                    throw new R2dbcNonTransientResourceException(
                        "Environment does not support a compression algorithm in " + compressions +
                            ", config does not allow uncompressed mode", CLI_SPECIFIC);
                }
            }
        } else if (isZlibAllowed(serverCapability)) {
            builder.disableZstdCompression();
        } else if (compressions.contains(CompressionAlgorithm.UNCOMPRESSED)) {
            builder.disableCompression();
        } else {
            throw new R2dbcPermissionDeniedException(
                "Environment does not support a compression algorithm in " + compressions +
                    ", config does not allow uncompressed mode", CLI_SPECIFIC);
        }

        if (database.isEmpty()) {
            builder.disableConnectWithDatabase();
        }

        if (context.getLocalInfilePath() == null) {
            builder.disableLoadDataLocalInfile();
        }

        if (ATTRIBUTES.isEmpty()) {
            builder.disableConnectAttributes();
        }

        return builder.build();
    }

    private Capability initHandshake(HandshakeRequest message) {
        HandshakeHeader header = message.getHeader();
        int handshakeVersion = header.getProtocolVersion();
        ServerVersion serverVersion = header.getServerVersion();

        if (handshakeVersion < HANDSHAKE_VERSION) {
            logger.warn("MySQL use handshake V{}, server version is {}, maybe most features are unavailable",
                handshakeVersion, serverVersion);
        }

        Capability capability = clientCapability(message.getServerCapability());

        // No need initialize server statuses because it has initialized by read filter.
        this.context.init(header.getConnectionId(), serverVersion, capability);
        this.authProvider = MySqlAuthProvider.build(message.getAuthType());
        this.salt = message.getSalt();

        return capability;
    }

    private MySqlAuthProvider getAndNextProvider() {
        MySqlAuthProvider authProvider = this.authProvider;
        this.authProvider = authProvider.next();
        return authProvider;
    }

    private HandshakeResponse createHandshakeResponse(Capability capability) {
        MySqlAuthProvider authProvider = getAndNextProvider();

        if (authProvider.isSslNecessary() && !sslCompleted) {
            throw new R2dbcPermissionDeniedException(authFails(authProvider.getType(), "handshake"),
                CLI_SPECIFIC);
        }

        byte[] authorization = authProvider.authentication(password, salt, context.getClientCollation());
        String authType = authProvider.getType();

        if (MySqlAuthProvider.NO_AUTH_PROVIDER.equals(authType)) {
            // Authentication type is not matter because of it has no authentication type.
            // Server need send a Change Authentication Message after handshake response.
            authType = MySqlAuthProvider.CACHING_SHA2_PASSWORD;
        }

        return HandshakeResponse.from(capability, context.getClientCollation().getId(), user, authorization,
            authType, database, ATTRIBUTES, zstdCompressionLevel);
    }

    private boolean isZstdAllowed(Capability capability) {
        return capability.isZstdCompression() && compressions.contains(CompressionAlgorithm.ZSTD);
    }

    private boolean isZlibAllowed(Capability capability) {
        return capability.isZlibCompression() && compressions.contains(CompressionAlgorithm.ZLIB);
    }

    private static String authFails(String authType, String phase) {
        return "Authentication type '" + authType + "' must require SSL in " + phase + " phase";
    }

    private static boolean isZstdSupported() {
        try {
            ClassLoader loader = AccessController.doPrivileged((PrivilegedAction<ClassLoader>) () -> {
                ClassLoader cl = Thread.currentThread().getContextClassLoader();
                return cl == null ? ClassLoader.getSystemClassLoader() : cl;
            });
            Class.forName("com.github.luben.zstd.Zstd", false, loader);
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }
}

abstract class AbstractTransactionState {

    final ConnectionState state;

    final List<String> statements = new ArrayList<>(5);

    /**
     * A bitmap of unfinished tasks, the lowest one bit is current task.
     */
    int tasks;

    @Nullable
    private String sql;

    protected AbstractTransactionState(ConnectionState state) {
        this.state = state;
    }

    final void setSql(String sql) {
        this.sql = sql;
    }

    final String batchStatement() {
        if (statements.size() == 1) {
            return statements.get(0);
        }

        return String.join(";", statements);
    }

    final Iterator<String> statements() {
        return statements.iterator();
    }

    final boolean accept(ServerMessage message, SynchronousSink<Void> sink) {
        if (message instanceof ErrorMessage) {
            sink.error(((ErrorMessage) message).toException(sql));
            return false;
        } else if (message instanceof CompleteMessage) {
            // Note: if CompleteMessage.isDone() is true, it is the last complete message of the entire
            // operation in batch mode and the last complete message of the current query in multi-query mode.
            // That means each complete message should be processed whatever it is done or not IN BATCH MODE.
            // And process only the complete message that's done IN MULTI-QUERY MODE.
            // So if we need to check isDone() here, should give an extra boolean variable: is it batch mode?
            // Currently, the tasks can determine current state, no need check isDone(). The Occam’s razor.
            int task = Integer.lowestOneBit(tasks);

            // Remove current task for prepare next task.
            this.tasks -= task;

            return process(task, sink);
        } else if (message instanceof ReferenceCounted) {
            ReferenceCountUtil.safeRelease(message);
        }

        return false;
    }

    abstract boolean cancelTasks();

    protected abstract boolean process(int task, SynchronousSink<Void> sink);
}

final class CommitRollbackState extends AbstractTransactionState {

    private static final int LOCK_WAIT_TIMEOUT = 1;

    private static final int COMMIT_OR_ROLLBACK = 2;

    private static final int CANCEL = 4;

    private final boolean commit;

    CommitRollbackState(ConnectionState state, boolean commit) {
        super(state);
        this.commit = commit;
    }

    @Override
    boolean cancelTasks() {
        if (!state.isInTransaction()) {
            tasks |= CANCEL;
            return true;
        }

        if (state.isLockWaitTimeoutChanged()) {
            tasks |= LOCK_WAIT_TIMEOUT;
            statements.add("SET innodb_lock_wait_timeout=" + state.getSessionLockWaitTimeout());
        }

        tasks |= COMMIT_OR_ROLLBACK;
        final String doneSql = commit ? "COMMIT" : "ROLLBACK";
        statements.add(doneSql);
        return false;
    }

    @Override
    protected boolean process(int task, SynchronousSink<Void> sink) {
        switch (task) {
            case LOCK_WAIT_TIMEOUT:
                state.resetCurrentLockWaitTimeout();
                return true;
            case COMMIT_OR_ROLLBACK:
                state.resetIsolationLevel();
                sink.complete();
                return false;
            case CANCEL:
                sink.complete();
                return false;
        }

        sink.error(new IllegalStateException("Undefined commit task: " + task + ", remain: " + tasks));

        return false;
    }
}

final class StartTransactionState extends AbstractTransactionState {

    private static final int LOCK_WAIT_TIMEOUT = 1;

    private static final int ISOLATION_LEVEL = 2;

    private static final int START_TRANSACTION = 4;

    private static final int CANCEL = 8;

    private final TransactionDefinition definition;

    StartTransactionState(ConnectionState state, TransactionDefinition definition) {
        super(state);
        this.definition = definition;
    }

    @Override
    boolean cancelTasks() {
        if (state.isInTransaction()) {
            tasks |= CANCEL;
            return true;
        }
        final Duration timeout = definition.getAttribute(TransactionDefinition.LOCK_WAIT_TIMEOUT);
        if (timeout != null) {
            final long lockWaitTimeout = timeout.getSeconds();
            tasks |= LOCK_WAIT_TIMEOUT;
            statements.add("SET innodb_lock_wait_timeout=" + lockWaitTimeout);
        }

        final IsolationLevel isolationLevel = definition.getAttribute(TransactionDefinition.ISOLATION_LEVEL);

        if (isolationLevel != null) {
            statements.add("SET TRANSACTION ISOLATION LEVEL " + isolationLevel.asSql());
            tasks |= ISOLATION_LEVEL;
        }

        tasks |= START_TRANSACTION;
        statements.add(buildStartTransaction(definition));

        return false;
    }

    @Override
    protected boolean process(int task, SynchronousSink<Void> sink) {
        switch (task) {
            case LOCK_WAIT_TIMEOUT:
                final Duration timeout = definition.getAttribute(TransactionDefinition.LOCK_WAIT_TIMEOUT);
                if (timeout != null) {
                    final long lockWaitTimeout = timeout.getSeconds();
                    state.setCurrentLockWaitTimeout(lockWaitTimeout);
                }
                return true;
            case ISOLATION_LEVEL:
                final IsolationLevel isolationLevel =
                    definition.getAttribute(TransactionDefinition.ISOLATION_LEVEL);
                if (isolationLevel != null) {
                    state.setIsolationLevel(isolationLevel);
                }
                return true;
            case START_TRANSACTION:
            case CANCEL:
                sink.complete();
                return false;

        }

        sink.error(new IllegalStateException("Undefined transaction task: " + task + ", remain: " + tasks));
        return false;
    }

    /**
     * Visible for testing.
     *
     * @param definition the transaction definition
     * @return the {@code START TRANSACTION} statement
     */
    static String buildStartTransaction(TransactionDefinition definition) {
        Boolean readOnly = definition.getAttribute(TransactionDefinition.READ_ONLY);
        Boolean snapshot = definition.getAttribute(MySqlTransactionDefinition.WITH_CONSISTENT_SNAPSHOT);

        if (readOnly == null && !Boolean.TRUE.equals(snapshot)) {
            return "BEGIN";
        }

        StringBuilder builder = new StringBuilder(90).append("START TRANSACTION");
        boolean first = true;

        if (Boolean.TRUE.equals(snapshot)) {
            // Compatible for enum ConsistentSnapshotEngine.
            Object eng = definition.getAttribute(MySqlTransactionDefinition.CONSISTENT_SNAPSHOT_ENGINE);
            String engine = eng == null ? null : eng.toString();

            first = false;
            builder.append(" WITH CONSISTENT ");

            if (engine == null) {
                builder.append("SNAPSHOT");
            } else {
                builder.append(engine).append(" SNAPSHOT");
            }

            Long sessionId =
                definition.getAttribute(MySqlTransactionDefinition.CONSISTENT_SNAPSHOT_FROM_SESSION);

            if (sessionId != null) {
                builder.append(" FROM SESSION ").append(Long.toUnsignedString(sessionId));
            }
        }

        if (readOnly != null) {
            if (!first) {
                builder.append(',');
            }

            if (readOnly) {
                builder.append(" READ ONLY");
            } else {
                builder.append(" READ WRITE");
            }
        }

        return builder.toString();
    }
}

final class CreateSavepointState extends AbstractTransactionState {

    private static final int START_TRANSACTION = 1;

    private static final int CREATE_SAVEPOINT = 2;

    private final String name;

    CreateSavepointState(final ConnectionState state, final String name) {
        super(state);
        this.name = name;
    }

    @Override
    boolean cancelTasks() {
        if (!state.isInTransaction()) {
            tasks |= START_TRANSACTION;
            statements.add("BEGIN");
        }

        final String doneSql = "SAVEPOINT " + StringUtils.quoteIdentifier(name);
        tasks |= CREATE_SAVEPOINT;
        statements.add(doneSql);
        return false;
    }

    @Override
    protected boolean process(int task, SynchronousSink<Void> sink) {
        switch (task) {
            case START_TRANSACTION:
                return true;
            case CREATE_SAVEPOINT:
                sink.complete();
                return false;
        }

        sink.error(new IllegalStateException("Undefined transaction task: " + task + ", remain: " + tasks));
        return false;
    }
}

final class TransactionBatchExchangeable extends FluxExchangeable<Void> {

    private final AbstractTransactionState state;

    TransactionBatchExchangeable(AbstractTransactionState state) {
        this.state = state;
    }

    @Override
    public void accept(ServerMessage message, SynchronousSink<Void> sink) {
        state.accept(message, sink);
    }

    @Override
    public void dispose() {
        // Do nothing.
    }

    @Override
    public void subscribe(CoreSubscriber<? super ClientMessage> s) {
        if (state.cancelTasks()) {
            s.onSubscribe(Operators.scalarSubscription(s, PingMessage.INSTANCE));
            return;
        }

        String sql = state.batchStatement();

        QueryLogger.log(sql);
        state.setSql(sql);
        s.onSubscribe(Operators.scalarSubscription(s, new TextQueryMessage(sql)));
    }
}

final class TransactionMultiExchangeable extends FluxExchangeable<Void> {

    private final Sinks.Many<ClientMessage> requests = Sinks.many().unicast()
        .onBackpressureBuffer(Queues.<ClientMessage>one().get());

    private final AbstractTransactionState state;

    @Nullable
    private Iterator<String> statements;

    TransactionMultiExchangeable(AbstractTransactionState state) {
        this.state = state;
    }

    @Override
    public void accept(ServerMessage message, SynchronousSink<Void> sink) {
        if (state.accept(message, sink)) {
            String sql = statements.next();

            QueryLogger.log(sql);
            state.setSql(sql);

            Sinks.EmitResult result = requests.tryEmitNext(new TextQueryMessage(sql));

            if (result != Sinks.EmitResult.OK) {
                QueryFlow.logger.error("Fail to emit a transaction message due to {}", result);
                sink.complete();
            }
        }
    }

    @Override
    public void dispose() {
        // No particular error condition handling for complete signal.
        requests.tryEmitComplete();
    }

    @Override
    public void subscribe(CoreSubscriber<? super ClientMessage> s) {
        if (state.cancelTasks()) {
            s.onSubscribe(Operators.scalarSubscription(s, PingMessage.INSTANCE));

            return;
        }
        statements = state.statements();

        String sql = statements.next();

        QueryLogger.log(sql);
        state.setSql(sql);
        requests.asFlux().subscribe(s);

        Sinks.EmitResult result = requests.tryEmitNext(new TextQueryMessage(sql));

        if (result != Sinks.EmitResult.OK) {
            QueryFlow.logger.error("Fail to emit a transaction message due to {}", result);
        }
    }
}
