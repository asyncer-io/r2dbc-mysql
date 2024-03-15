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
import io.asyncer.r2dbc.mysql.api.MySqlConnection;
import io.asyncer.r2dbc.mysql.api.MySqlConnectionMetadata;
import io.asyncer.r2dbc.mysql.api.MySqlResult;
import io.asyncer.r2dbc.mysql.api.MySqlStatement;
import io.asyncer.r2dbc.mysql.api.MySqlTransactionDefinition;
import io.asyncer.r2dbc.mysql.cache.PrepareCache;
import io.asyncer.r2dbc.mysql.cache.QueryCache;
import io.asyncer.r2dbc.mysql.client.Client;
import io.asyncer.r2dbc.mysql.codec.Codecs;
import io.asyncer.r2dbc.mysql.constant.ServerStatuses;
import io.asyncer.r2dbc.mysql.internal.util.StringUtils;
import io.asyncer.r2dbc.mysql.message.client.InitDbMessage;
import io.asyncer.r2dbc.mysql.message.client.PingMessage;
import io.asyncer.r2dbc.mysql.message.server.CompleteMessage;
import io.asyncer.r2dbc.mysql.message.server.ErrorMessage;
import io.asyncer.r2dbc.mysql.message.server.ServerMessage;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.Readable;
import io.r2dbc.spi.TransactionDefinition;
import io.r2dbc.spi.ValidationDepth;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.ZoneId;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonEmpty;
import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link MySqlConnection} for connecting to the MySQL database.
 */
final class MySqlSimpleConnection implements MySqlConnection, ConnectionState {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(MySqlSimpleConnection.class);

    private static final String PING_MARKER = "/* ping */";

    private static final ServerVersion MARIA_11_1_1 = ServerVersion.create(11, 1, 1, true);

    private static final ServerVersion MYSQL_8_0_3 = ServerVersion.create(8, 0, 3);

    private static final ServerVersion MYSQL_5_7_20 = ServerVersion.create(5, 7, 20);

    private static final ServerVersion MYSQL_8 = ServerVersion.create(8, 0, 0);

    private static final ServerVersion MYSQL_5_7_4 = ServerVersion.create(5, 7, 4);

    private static final ServerVersion MARIA_10_1_1 = ServerVersion.create(10, 1, 1, true);

    private static final Function<ServerMessage, Boolean> VALIDATE = message -> {
        if (message instanceof CompleteMessage && ((CompleteMessage) message).isDone()) {
            return true;
        }

        if (message instanceof ErrorMessage) {
            ErrorMessage msg = (ErrorMessage) message;
            logger.debug("Remote validate failed: [{}] [{}] {}", msg.getCode(), msg.getSqlState(),
                msg.getMessage());
        } else {
            ReferenceCountUtil.safeRelease(message);
        }

        return false;
    };

    private static final BiConsumer<ServerMessage, SynchronousSink<ServerMessage>> PING = (message, sink) -> {
        if (message instanceof ErrorMessage) {
            sink.next(message);
            sink.complete();
        } else if (message instanceof CompleteMessage && ((CompleteMessage) message).isDone()) {
            sink.next(message);
            sink.complete();
        } else {
            ReferenceCountUtil.safeRelease(message);
        }
    };

    private static final BiConsumer<ServerMessage, SynchronousSink<Boolean>> INIT_DB = (message, sink) -> {
        if (message instanceof ErrorMessage) {
            ErrorMessage msg = (ErrorMessage) message;
            logger.debug("Use database failed: [{}] [{}] {}", msg.getCode(), msg.getSqlState(),
                msg.getMessage());
            sink.next(false);
            sink.complete();
        } else if (message instanceof CompleteMessage && ((CompleteMessage) message).isDone()) {
            sink.next(true);
            sink.complete();
        } else {
            ReferenceCountUtil.safeRelease(message);
        }
    };

    private static final BiConsumer<ServerMessage, SynchronousSink<Void>> INIT_DB_AFTER = (message, sink) -> {
        if (message instanceof ErrorMessage) {
            sink.error(((ErrorMessage) message).toException());
        } else if (message instanceof CompleteMessage && ((CompleteMessage) message).isDone()) {
            sink.complete();
        } else {
            ReferenceCountUtil.safeRelease(message);
        }
    };

    private final Client client;

    private final Codecs codecs;

    private final boolean batchSupported;

    private final MySqlConnectionMetadata metadata;

    private volatile IsolationLevel sessionLevel;

    private final QueryCache queryCache;

    private final PrepareCache prepareCache;

    @Nullable
    private final Predicate<String> prepare;

    /**
     * Current isolation level inferred by past statements.
     * <p>
     * Inference rules:
     * <ol><li>In the beginning, it is also {@link #sessionLevel}.</li>
     * <li>After the user calls {@link #setTransactionIsolationLevel(IsolationLevel)}, it will change to
     * the user-specified value.</li>
     * <li>After the end of a transaction (commit or rollback), it will recover to {@link #sessionLevel}.</li>
     * </ol>
     */
    private volatile IsolationLevel currentLevel;

    /**
     * Session lock wait timeout.
     */
    private volatile long lockWaitTimeout;

    /**
     * Current transaction lock wait timeout.
     */
    private volatile long currentLockWaitTimeout;

    MySqlSimpleConnection(Client client, Codecs codecs, IsolationLevel level,
        long lockWaitTimeout, QueryCache queryCache, PrepareCache prepareCache, @Nullable String product,
        @Nullable Predicate<String> prepare) {
        ConnectionContext context = client.getContext();

        this.client = client;
        this.sessionLevel = level;
        this.currentLevel = level;
        this.codecs = codecs;
        this.lockWaitTimeout = lockWaitTimeout;
        this.currentLockWaitTimeout = lockWaitTimeout;
        this.queryCache = queryCache;
        this.prepareCache = prepareCache;
        this.metadata = new MySqlSimpleConnectionMetadata(context.getServerVersion().toString(), product,
            context.isMariaDb());
        this.batchSupported = context.getCapability().isMultiStatementsAllowed();
        this.prepare = prepare;

        if (this.batchSupported) {
            logger.debug("Batch is supported by server");
        } else {
            logger.warn("The MySQL server does not support batch, fallback to executing one-by-one");
        }
    }

    @Override
    public Mono<Void> beginTransaction() {
        return beginTransaction(MySqlTransactionDefinition.empty());
    }

    @Override
    public Mono<Void> beginTransaction(TransactionDefinition definition) {
        return Mono.defer(() -> QueryFlow.beginTransaction(client, this, batchSupported, definition));
    }

    @Override
    public Mono<Void> close() {
        Mono<Void> closer = client.close();

        if (logger.isDebugEnabled()) {
            return closer.doOnSubscribe(s -> logger.debug("Connection closing"))
                .doOnSuccess(ignored -> logger.debug("Connection close succeed"));
        }

        return closer;
    }

    @Override
    public Mono<Void> commitTransaction() {
        return Mono.defer(() -> QueryFlow.doneTransaction(client, this, true, batchSupported));
    }

    @Override
    public MySqlBatch createBatch() {
        return batchSupported ? new MySqlBatchingBatch(client, codecs) : new MySqlSyntheticBatch(client, codecs);
    }

    @Override
    public Mono<Void> createSavepoint(String name) {
        requireNonEmpty(name, "Savepoint name must not be empty");

        return QueryFlow.createSavepoint(client, this, name, batchSupported);
    }

    @Override
    public MySqlStatement createStatement(String sql) {
        requireNonNull(sql, "sql must not be null");

        if (sql.startsWith(PING_MARKER)) {
            return new PingStatement(client, codecs);
        }

        Query query = queryCache.get(sql);

        if (query.isSimple()) {
            if (prepare != null && prepare.test(sql)) {
                logger.debug("Create a simple statement provided by prepare query");
                return new PrepareSimpleStatement(client, codecs, sql, prepareCache);
            }

            logger.debug("Create a simple statement provided by text query");

            return new TextSimpleStatement(client, codecs, sql);
        }

        if (prepare == null) {
            logger.debug("Create a parameterized statement provided by text query");
            return new TextParameterizedStatement(client, codecs, query);
        }

        logger.debug("Create a parameterized statement provided by prepare query");

        return new PrepareParameterizedStatement(client, codecs, query, prepareCache);
    }

    @Override
    public Mono<Void> postAllocate() {
        return Mono.empty();
    }

    @Override
    public Mono<Void> preRelease() {
        // Rollback if the connection is in transaction.
        return rollbackTransaction();
    }

    @Override
    public Mono<Void> releaseSavepoint(String name) {
        requireNonEmpty(name, "Savepoint name must not be empty");

        return QueryFlow.executeVoid(client, "RELEASE SAVEPOINT " + StringUtils.quoteIdentifier(name));
    }

    @Override
    public Mono<Void> rollbackTransaction() {
        return Mono.defer(() -> QueryFlow.doneTransaction(client, this, false, batchSupported));
    }

    @Override
    public Mono<Void> rollbackTransactionToSavepoint(String name) {
        requireNonEmpty(name, "Savepoint name must not be empty");

        return QueryFlow.executeVoid(client, "ROLLBACK TO SAVEPOINT " + StringUtils.quoteIdentifier(name));
    }

    @Override
    public MySqlConnectionMetadata getMetadata() {
        return metadata;
    }

    /**
     * MySQL does not have any way to query the isolation level of the current transaction, only inferred from past
     * statements, so driver can not make sure the result is right.
     * <p>
     * See <a href="https://bugs.mysql.com/bug.php?id=53341">MySQL Bug 53341</a>
     * <p>
     * {@inheritDoc}
     */
    @Override
    public IsolationLevel getTransactionIsolationLevel() {
        return currentLevel;
    }

    /**
     * Gets session transaction isolation level(Only for testing).
     *
     * @return session transaction isolation level.
     */
    IsolationLevel getSessionTransactionIsolationLevel() {
        return sessionLevel;
    }

    @Override
    public Mono<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
        requireNonNull(isolationLevel, "isolationLevel must not be null");

        // Set subsequent transaction isolation level.
        return QueryFlow.executeVoid(client,
                "SET SESSION TRANSACTION ISOLATION LEVEL " + isolationLevel.asSql())
            .doOnSuccess(ignored -> {
                this.sessionLevel = isolationLevel;
                if (!this.isInTransaction()) {
                    this.currentLevel = isolationLevel;
                }
            });
    }

    @Override
    public Mono<Boolean> validate(ValidationDepth depth) {
        requireNonNull(depth, "depth must not be null");

        if (depth == ValidationDepth.LOCAL) {
            return Mono.fromSupplier(client::isConnected);
        }

        return Mono.defer(() -> {
            if (!client.isConnected()) {
                return Mono.just(false);
            }

            return doPingInternal(client)
                .last()
                .map(VALIDATE)
                .onErrorResume(e -> {
                    // `last` maybe emit a NoSuchElementException, exchange maybe emit exception by Netty.
                    // But should NEVER emit any exception, so logging exception and emit false.
                    logger.debug("Remote validate failed", e);
                    return Mono.just(false);
                });
        });
    }

    @Override
    public boolean isAutoCommit() {
        // Within transaction, autocommit remains disabled until end the transaction with COMMIT or ROLLBACK.
        // The autocommit mode then reverts to its previous state.
        return !isInTransaction() && isSessionAutoCommit();
    }

    @Override
    public Mono<Void> setAutoCommit(boolean autoCommit) {
        return Mono.defer(() -> {
            if (autoCommit == isSessionAutoCommit()) {
                return Mono.empty();
            }

            return QueryFlow.executeVoid(client, "SET autocommit=" + (autoCommit ? 1 : 0));
        });
    }

    @Override
    public void setIsolationLevel(IsolationLevel level) {
        this.currentLevel = level;
    }

    @Override
    public long getSessionLockWaitTimeout() {
        return lockWaitTimeout;
    }

    @Override
    public void setCurrentLockWaitTimeout(long timeoutSeconds) {
        this.currentLockWaitTimeout = timeoutSeconds;
    }

    @Override
    public void resetIsolationLevel() {
        this.currentLevel = this.sessionLevel;
    }

    @Override
    public boolean isLockWaitTimeoutChanged() {
        return currentLockWaitTimeout != lockWaitTimeout;
    }

    @Override
    public void resetCurrentLockWaitTimeout() {
        this.currentLockWaitTimeout = this.lockWaitTimeout;
    }

    @Override
    public boolean isInTransaction() {
        return (client.getContext().getServerStatuses() & ServerStatuses.IN_TRANSACTION) != 0;
    }

    @Override
    public Mono<Void> setLockWaitTimeout(Duration timeout) {
        requireNonNull(timeout, "timeout must not be null");

        if (!client.getContext().isLockWaitTimeoutSupported()) {
            logger.warn("Lock wait timeout is not supported by server, setLockWaitTimeout operation is ignored");
            return Mono.empty();
        }

        long timeoutSeconds = timeout.getSeconds();
        return QueryFlow.executeVoid(client, "SET innodb_lock_wait_timeout=" + timeoutSeconds)
            .doOnSuccess(ignored -> this.lockWaitTimeout = this.currentLockWaitTimeout = timeoutSeconds);
    }

    @Override
    public Mono<Void> setStatementTimeout(Duration timeout) {
        requireNonNull(timeout, "timeout must not be null");

        final ConnectionContext context = client.getContext();
        final boolean isMariaDb = context.isMariaDb();
        final ServerVersion serverVersion = context.getServerVersion();
        final long timeoutMs = timeout.toMillis();
        final String sql = isMariaDb ? "SET max_statement_time=" + timeoutMs / 1000.0
            : "SET SESSION MAX_EXECUTION_TIME=" + timeoutMs;

        // mariadb: https://mariadb.com/kb/en/aborting-statements/
        // mysql: https://dev.mysql.com/blog-archive/server-side-select-statement-timeouts/
        // ref: https://github.com/mariadb-corporation/mariadb-connector-r2dbc
        if (isMariaDb && serverVersion.isGreaterThanOrEqualTo(MARIA_10_1_1)
            || !isMariaDb && serverVersion.isGreaterThanOrEqualTo(MYSQL_5_7_4)) {
            return QueryFlow.executeVoid(client, sql);
        }

        return Mono.error(
            new R2dbcNonTransientResourceException(
                "Statement timeout is not supported by server version " + serverVersion,
                "HY000",
                -1,
                sql
            )
        );
    }

    private boolean isSessionAutoCommit() {
        return (client.getContext().getServerStatuses() & ServerStatuses.AUTO_COMMIT) != 0;
    }

    static Flux<ServerMessage> doPingInternal(Client client) {
        return client.exchange(PingMessage.INSTANCE, PING);
    }

    /**
     * Initialize a {@link MySqlConnection} after login.
     *
     * @param client           must be logged-in.
     * @param codecs           the {@link Codecs}.
     * @param database         the database that should be lazy init.
     * @param queryCache       the cache of {@link Query}.
     * @param prepareCache     the cache of server-preparing result.
     * @param sessionVariables the session variables to set.
     * @param prepare          judging for prefer use prepare statement to execute simple query.
     * @return a {@link Mono} will emit an initialized {@link MySqlConnection}.
     */
    static Mono<MySqlConnection> init(
        Client client, Codecs codecs, String database,
        QueryCache queryCache, PrepareCache prepareCache,
        List<String> sessionVariables, @Nullable Predicate<String> prepare
    ) {
        Mono<MySqlConnection> connection = initSessionVariables(client, sessionVariables)
            .then(loadSessionVariables(client, codecs))
            .flatMap(data -> loadInnoDbEngineStatus(data, client, codecs))
            .map(data -> {
                ConnectionContext context = client.getContext();
                ZoneId timeZone = data.timeZone;
                if (timeZone != null) {
                    logger.debug("Got server time zone {} from loading session variables", timeZone);
                    context.initTimeZone(timeZone);
                }

                if (data.lockWaitTimeoutSupported) {
                    context.enableLockWaitTimeoutSupported();
                } else {
                    logger.info("Lock wait timeout is not supported by server, all related operations will be ignored");
                }

                return new MySqlSimpleConnection(client, codecs, data.level, data.lockWaitTimeout,
                    queryCache, prepareCache, data.product, prepare);
            });

        if (database.isEmpty()) {
            return connection;
        }

        return connection.flatMap(c -> initDatabase(client, database).thenReturn(c));
    }

    private static Mono<Void> initSessionVariables(Client client, List<String> sessionVariables) {
        if (sessionVariables.isEmpty()) {
            return Mono.empty();
        }

        StringBuilder query = new StringBuilder(sessionVariables.size() * 32 + 16).append("SET ");
        boolean comma = false;

        for (String variable : sessionVariables) {
            if (variable.isEmpty()) {
                continue;
            }

            if (comma) {
                query.append(',');
            } else {
                comma = true;
            }

            if (variable.startsWith("@")) {
                query.append(variable);
            } else {
                query.append("SESSION ").append(variable);
            }
        }

        return QueryFlow.executeVoid(client, query.toString());
    }

    private static Mono<SessionData> loadSessionVariables(Client client, Codecs codecs) {
        ConnectionContext context = client.getContext();
        StringBuilder query = new StringBuilder(128)
            .append("SELECT ")
            .append(transactionIsolationColumn(context))
            .append(",@@version_comment AS v");

        Function<MySqlResult, Flux<SessionData>> handler;

        if (context.isTimeZoneInitialized()) {
            handler = r -> convertSessionData(r, false);
        } else {
            query.append(",@@system_time_zone AS s,@@time_zone AS t");
            handler = r -> convertSessionData(r, true);
        }

        return new TextSimpleStatement(client, codecs, query.toString())
            .execute()
            .flatMap(handler)
            .last();
    }

    private static Mono<SessionData> loadInnoDbEngineStatus(SessionData data, Client client, Codecs codecs) {
        return new TextSimpleStatement(client, codecs, "SHOW VARIABLES LIKE 'innodb\\\\_lock\\\\_wait\\\\_timeout'")
            .execute()
            .flatMap(r -> r.map(readable -> {
                String value = readable.get(1, String.class);

                if (value == null || value.isEmpty()) {
                    return data;
                } else {
                    return data.lockWaitTimeout(Long.parseLong(value));
                }
            }))
            .single(data);
    }

    private static Mono<Void> initDatabase(Client client, String database) {
        return client.exchange(new InitDbMessage(database), INIT_DB)
            .last()
            .flatMap(success -> {
                if (success) {
                    return Mono.empty();
                }

                String sql = "CREATE DATABASE IF NOT EXISTS " + StringUtils.quoteIdentifier(database);

                return QueryFlow.executeVoid(client, sql)
                    .then(client.exchange(new InitDbMessage(database), INIT_DB_AFTER).then());
            });
    }

    private static Flux<SessionData> convertSessionData(MySqlResult r, boolean timeZone) {
        return r.map(readable -> {
            IsolationLevel level = convertIsolationLevel(readable.get(0, String.class));
            String product = readable.get(1, String.class);

            return new SessionData(level, product, timeZone ? readZoneId(readable) : null);
        });
    }

    private static ZoneId readZoneId(Readable readable) {
        String systemTimeZone = readable.get(2, String.class);
        String timeZone = readable.get(3, String.class);

        if (timeZone == null || timeZone.isEmpty() || "SYSTEM".equalsIgnoreCase(timeZone)) {
            if (systemTimeZone == null || systemTimeZone.isEmpty()) {
                logger.warn("MySQL does not return any timezone, trying to use system default timezone");
                return ZoneId.systemDefault().normalized();
            } else {
                return convertZoneId(systemTimeZone);
            }
        } else {
            return convertZoneId(timeZone);
        }
    }

    private static ZoneId convertZoneId(String id) {
        try {
            return StringUtils.parseZoneId(id);
        } catch (DateTimeException e) {
            logger.warn("The server timezone is unknown <{}>, trying to use system default timezone", id, e);

            return ZoneId.systemDefault().normalized();
        }
    }

    private static IsolationLevel convertIsolationLevel(@Nullable String name) {
        if (name == null) {
            logger.warn("Isolation level is null in current session, fallback to repeatable read");

            return IsolationLevel.REPEATABLE_READ;
        }

        switch (name) {
            case "READ-UNCOMMITTED":
                return IsolationLevel.READ_UNCOMMITTED;
            case "READ-COMMITTED":
                return IsolationLevel.READ_COMMITTED;
            case "REPEATABLE-READ":
                return IsolationLevel.REPEATABLE_READ;
            case "SERIALIZABLE":
                return IsolationLevel.SERIALIZABLE;
        }

        logger.warn("Unknown isolation level {} in current session, fallback to repeatable read", name);

        return IsolationLevel.REPEATABLE_READ;
    }

    /**
     * Resolves the column of session isolation level, the {@literal @@tx_isolation} has been marked as deprecated.
     * <p>
     * If server is MariaDB, {@literal @@transaction_isolation} is used starting from {@literal 11.1.1}.
     * <p>
     * If the server is MySQL, use {@literal @@transaction_isolation} starting from {@literal 8.0.3}, or between
     * {@literal 5.7.20} and {@literal 8.0.0} (exclusive).
     */
    private static String transactionIsolationColumn(ConnectionContext context) {
        ServerVersion version = context.getServerVersion();

        if (context.isMariaDb()) {
            return version.isGreaterThanOrEqualTo(MARIA_11_1_1) ? "@@transaction_isolation AS i" :
                "@@tx_isolation AS i";
        }

        return version.isGreaterThanOrEqualTo(MYSQL_8_0_3) ||
            (version.isGreaterThanOrEqualTo(MYSQL_5_7_20) && version.isLessThan(MYSQL_8)) ?
            "@@transaction_isolation AS i" : "@@tx_isolation AS i";
    }

    private static final class SessionData {

        private final IsolationLevel level;

        @Nullable
        private final String product;

        @Nullable
        private final ZoneId timeZone;

        private long lockWaitTimeout = -1;

        private boolean lockWaitTimeoutSupported;

        private SessionData(IsolationLevel level, @Nullable String product, @Nullable ZoneId timeZone) {
            this.level = level;
            this.product = product;
            this.timeZone = timeZone;
        }

        SessionData lockWaitTimeout(long timeout) {
            this.lockWaitTimeoutSupported = true;
            this.lockWaitTimeout = timeout;
            return this;
        }
    }
}
