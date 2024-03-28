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
import io.asyncer.r2dbc.mysql.api.MySqlStatement;
import io.asyncer.r2dbc.mysql.api.MySqlTransactionDefinition;
import io.asyncer.r2dbc.mysql.cache.QueryCache;
import io.asyncer.r2dbc.mysql.client.Client;
import io.asyncer.r2dbc.mysql.codec.Codecs;
import io.asyncer.r2dbc.mysql.internal.util.StringUtils;
import io.asyncer.r2dbc.mysql.message.server.CompleteMessage;
import io.asyncer.r2dbc.mysql.message.server.ErrorMessage;
import io.asyncer.r2dbc.mysql.message.server.ServerMessage;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.TransactionDefinition;
import io.r2dbc.spi.ValidationDepth;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonEmpty;
import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link MySqlConnection} for connecting to the MySQL database.
 */
final class MySqlSimpleConnection implements MySqlConnection {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(MySqlSimpleConnection.class);

    private static final String PING_MARKER = "/* ping */";

    private static final Function<ServerMessage, Boolean> VALIDATE = message -> {
        if (message instanceof CompleteMessage && ((CompleteMessage) message).isDone()) {
            return true;
        }

        if (message instanceof ErrorMessage) {
            ErrorMessage msg = (ErrorMessage) message;
            logger.debug("Remote validate failed: [{}] [{}] {}", msg.getCode(), msg.getSqlState(), msg.getMessage());
        } else {
            ReferenceCountUtil.safeRelease(message);
        }

        return false;
    };

    private final Client client;

    private final Codecs codecs;

    private final MySqlConnectionMetadata metadata;

    private final QueryCache queryCache;

    @Nullable
    private final Predicate<String> prepare;

    // TODO: Check it when executing
    private final boolean batchSupported;

    MySqlSimpleConnection(Client client, Codecs codecs, QueryCache queryCache, @Nullable Predicate<String> prepare) {
        ConnectionContext context = client.getContext();

        this.client = client;
        this.codecs = codecs;
        this.metadata = new MySqlClientConnectionMetadata(client);
        this.queryCache = queryCache;
        this.prepare = prepare;
        this.batchSupported = context.getCapability().isMultiStatementsAllowed();

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
        return Mono.defer(() -> QueryFlow.beginTransaction(client, batchSupported, definition));
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
        return Mono.defer(() -> QueryFlow.doneTransaction(client, true, batchSupported));
    }

    @Override
    public MySqlBatch createBatch() {
        return batchSupported ? new MySqlBatchingBatch(client, codecs) : new MySqlSyntheticBatch(client, codecs);
    }

    @Override
    public Mono<Void> createSavepoint(String name) {
        requireNonEmpty(name, "Savepoint name must not be empty");

        return QueryFlow.createSavepoint(client, name, batchSupported);
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
                return new PrepareSimpleStatement(client, codecs, sql);
            }

            logger.debug("Create a simple statement provided by text query");

            return new TextSimpleStatement(client, codecs, sql);
        }

        if (prepare == null) {
            logger.debug("Create a parameterized statement provided by text query");
            return new TextParameterizedStatement(client, codecs, query);
        }

        logger.debug("Create a parameterized statement provided by prepare query");

        return new PrepareParameterizedStatement(client, codecs, query);
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
        return Mono.defer(() -> QueryFlow.doneTransaction(client, false, batchSupported));
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
     * MySQL does not have a way to query the isolation level of the current transaction, only inferred from past
     * statements, so driver can not make sure the result is right.
     * <p>
     * See <a href="https://bugs.mysql.com/bug.php?id=53341">MySQL Bug 53341</a>
     * <p>
     * {@inheritDoc}
     */
    @Override
    public IsolationLevel getTransactionIsolationLevel() {
        return client.getContext().getCurrentIsolationLevel();
    }

    @Override
    public Mono<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
        requireNonNull(isolationLevel, "isolationLevel must not be null");

        // Set subsequent transaction isolation level.
        return QueryFlow.executeVoid(client,
                "SET SESSION TRANSACTION ISOLATION LEVEL " + isolationLevel.asSql())
            .doOnSuccess(ignored -> {
                ConnectionContext context = client.getContext();

                context.setSessionIsolationLevel(isolationLevel);
                if (!context.isInTransaction()) {
                    context.setCurrentIsolationLevel(isolationLevel);
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

            return QueryFlow.ping(client)
                .map(VALIDATE)
                .last()
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
        return client.getContext().isAutoCommit();
    }

    @Override
    public Mono<Void> setAutoCommit(boolean autoCommit) {
        return Mono.defer(() -> QueryFlow.executeVoid(client, "SET autocommit=" + (autoCommit ? 1 : 0)));
    }

    @Override
    public Mono<Void> setLockWaitTimeout(Duration timeout) {
        requireNonNull(timeout, "timeout must not be null");

        if (client.getContext().isLockWaitTimeoutSupported()) {
            return QueryFlow.executeVoid(client, StringUtils.lockWaitTimeoutStatement(timeout))
                .doOnSuccess(ignored -> client.getContext().setAllLockWaitTimeout(timeout));
        }

        logger.warn("Lock wait timeout is not supported by server, setLockWaitTimeout operation is ignored");
        return Mono.empty();

    }

    @Override
    public Mono<Void> setStatementTimeout(Duration timeout) {
        requireNonNull(timeout, "timeout must not be null");

        ConnectionContext context = client.getContext();

        // mariadb: https://mariadb.com/kb/en/aborting-statements/
        // mysql: https://dev.mysql.com/blog-archive/server-side-select-statement-timeouts/
        // ref: https://github.com/mariadb-corporation/mariadb-connector-r2dbc
        if (context.isStatementTimeoutSupported()) {
            String variable = StringUtils.statementTimeoutVariable(timeout, context.isMariaDb());
            return QueryFlow.setSessionVariable(client, variable);
        }

        return Mono.error(
            new R2dbcNonTransientResourceException(
                "Statement timeout is not supported by server version " + context.getServerVersion(),
                "HY000",
                -1
            )
        );
    }

    /**
     * Visible only for testing.
     *
     * @return current connection context
     */
    @TestOnly
    ConnectionContext context() {
        return client.getContext();
    }
}
