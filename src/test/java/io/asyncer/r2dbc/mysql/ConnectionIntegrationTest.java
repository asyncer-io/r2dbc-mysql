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

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.R2dbcPermissionDeniedException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.node.ArrayNode;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.node.ObjectNode;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;

import static io.r2dbc.spi.IsolationLevel.READ_COMMITTED;
import static io.r2dbc.spi.IsolationLevel.READ_UNCOMMITTED;
import static io.r2dbc.spi.IsolationLevel.REPEATABLE_READ;
import static io.r2dbc.spi.IsolationLevel.SERIALIZABLE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link MySqlConnection}.
 */
class ConnectionIntegrationTest extends IntegrationTestSupport {

    private static final MySqlConnectionConfiguration config = configuration(
        "r2dbc", false, false, null, null);

    ConnectionIntegrationTest() {
        super(config);
    }

    @BeforeAll
    static void initGlobalVariables() {
        // TODO: move it to GitHub Actions instead of test code
        MySqlConnectionFactory.from(config)
            .create()
            .flatMap(conn -> conn.createStatement("SET GLOBAL local_infile=ON")
                .execute()
                .flatMap(MySqlResult::getRowsUpdated)
                .then(conn.close())
                .onErrorResume(e -> conn.close().then(Mono.error(e))))
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void isInTransaction() {
        complete(connection -> Mono.<Void>fromRunnable(() -> assertThat(connection.isInTransaction())
                .isFalse())
            .then(connection.beginTransaction())
            .doOnSuccess(ignored -> assertThat(connection.isInTransaction()).isTrue())
            .then(connection.commitTransaction())
            .doOnSuccess(ignored -> assertThat(connection.isInTransaction()).isFalse())
            .then(connection.beginTransaction())
            .doOnSuccess(ignored -> assertThat(connection.isInTransaction()).isTrue())
            .then(connection.rollbackTransaction())
            .doOnSuccess(ignored -> assertThat(connection.isInTransaction()).isFalse()));
    }

    @Test
    void autoRollbackPreRelease() {
        // Mock pool allocate/release.
        complete(conn -> conn.postAllocate()
            .thenMany(conn.createStatement("CREATE TEMPORARY TABLE test (id INT NOT NULL PRIMARY KEY)")
                .execute())
            .flatMap(MySqlResult::getRowsUpdated)
            .then(conn.beginTransaction())
            .thenMany(conn.createStatement("INSERT INTO test VALUES (1)")
                .execute())
            .flatMap(MySqlResult::getRowsUpdated)
            .single()
            .doOnNext(it -> assertThat(it).isEqualTo(1))
            .doOnSuccess(ignored -> assertThat(conn.isInTransaction()).isTrue())
            .then(conn.preRelease())
            .doOnSuccess(ignored -> assertThat(conn.isInTransaction()).isFalse())
            .then(conn.postAllocate())
            .thenMany(conn.createStatement("SELECT * FROM test")
                .execute())
            .flatMap(it -> it.map((row, metadata) -> row.get(0, Integer.class)))
            .count()
            .doOnNext(it -> assertThat(it).isZero()));
    }

    @Test
    void shouldNotRollbackCommittedPreRelease() {
        // Mock pool allocate/release.
        complete(conn -> conn.postAllocate()
            .thenMany(conn.createStatement("CREATE TEMPORARY TABLE test (id INT NOT NULL PRIMARY KEY)")
                .execute())
            .flatMap(MySqlResult::getRowsUpdated)
            .then(conn.beginTransaction())
            .thenMany(conn.createStatement("INSERT INTO test VALUES (1)")
                .execute())
            .flatMap(MySqlResult::getRowsUpdated)
            .single()
            .doOnNext(it -> assertThat(it).isEqualTo(1))
            .then(conn.commitTransaction())
            .then(conn.preRelease())
            .doOnSuccess(ignored -> assertThat(conn.isInTransaction()).isFalse())
            .then(conn.postAllocate())
            .thenMany(conn.createStatement("SELECT * FROM test")
                .execute())
            .flatMap(it -> it.map((row, metadata) -> row.get(0, Integer.class)))
            .collectList()
            .doOnNext(it -> assertThat(it).isEqualTo(Collections.singletonList(1))));
    }

    @Test
    void transactionDefinitionLockWaitTimeout() {
        complete(connection -> connection.beginTransaction(MySqlTransactionDefinition.builder()
                .lockWaitTimeout(Duration.ofSeconds(345))
                .build())
            .doOnSuccess(ignored -> {
                assertThat(connection.isInTransaction()).isTrue();
                assertThat(connection.getTransactionIsolationLevel()).isEqualTo(REPEATABLE_READ);
                assertThat(connection.isLockWaitTimeoutChanged()).isTrue();
            })
            .then(connection.rollbackTransaction())
            .doOnSuccess(ignored -> {
                assertThat(connection.isInTransaction()).isFalse();
                assertThat(connection.getTransactionIsolationLevel()).isEqualTo(REPEATABLE_READ);
                assertThat(connection.isLockWaitTimeoutChanged()).isFalse();
            }));
    }

    @Test
    void transactionDefinitionIsolationLevel() {
        complete(connection -> connection.beginTransaction(MySqlTransactionDefinition.builder()
                .isolationLevel(READ_COMMITTED)
                .build())
            .doOnSuccess(ignored -> {
                assertThat(connection.isInTransaction()).isTrue();
                assertThat(connection.getTransactionIsolationLevel()).isEqualTo(READ_COMMITTED);
                assertThat(connection.isLockWaitTimeoutChanged()).isFalse();
            })
            .then(connection.rollbackTransaction())
            .doOnSuccess(ignored -> {
                assertThat(connection.isInTransaction()).isFalse();
                assertThat(connection.getTransactionIsolationLevel()).isEqualTo(REPEATABLE_READ);
                assertThat(connection.isLockWaitTimeoutChanged()).isFalse();
            }));
    }

    @Test
    void setTransactionLevelNotInTransaction() {
        complete(connection ->
            // check initial session isolation level
            Mono.fromSupplier(connection::getTransactionIsolationLevel)
                .doOnSuccess(it -> assertThat(it).isEqualTo(REPEATABLE_READ))
                .then(connection.beginTransaction())
                .doOnSuccess(ignored -> assertThat(connection.isInTransaction()).isTrue())
                .then(Mono.fromSupplier(connection::getTransactionIsolationLevel))
                .doOnSuccess(it -> assertThat(it).isEqualTo(REPEATABLE_READ))
                .then(connection.rollbackTransaction())
                .then(connection.setTransactionIsolationLevel(READ_COMMITTED))
                // ensure that session isolation level is changed
                .then(Mono.fromSupplier(connection::getTransactionIsolationLevel))
                .doOnSuccess(it -> assertThat(it).isEqualTo(READ_COMMITTED))
                .then(connection.beginTransaction())
                .doOnSuccess(ignored -> assertThat(connection.isInTransaction()).isTrue())
                // ensure transaction isolation level applies to subsequent transactions
                .then(Mono.fromSupplier(connection::getTransactionIsolationLevel))
                .doOnSuccess(it -> assertThat(it).isEqualTo(READ_COMMITTED))
        );
    }

    @Test
    void setTransactionLevelInTransaction() {
        complete(connection ->
            // check initial session transaction isolation level
            Mono.fromSupplier(connection::getTransactionIsolationLevel)
                .doOnSuccess(it -> assertThat(it).isEqualTo(REPEATABLE_READ))
                .then(connection.beginTransaction())
                .then(connection.setTransactionIsolationLevel(READ_COMMITTED))
                // ensure that current transaction isolation level is not changed
                .then(Mono.fromSupplier(connection::getTransactionIsolationLevel))
                .doOnSuccess(it -> assertThat(it).isNotEqualTo(READ_COMMITTED))
                .then(connection.rollbackTransaction())
                .doOnSuccess(ignored -> assertThat(connection.isInTransaction()).isFalse())
                // ensure that session isolation level is changed after rollback
                .then(Mono.fromSupplier(connection::getTransactionIsolationLevel))
                .doOnSuccess(it -> assertThat(it).isEqualTo(READ_COMMITTED))
                // ensure transaction isolation level applies to subsequent transactions
                .then(connection.beginTransaction())
                .doOnSuccess(ignored -> assertThat(connection.isInTransaction()).isTrue())
        );
    }

    @Test
    void transactionDefinition() {
        // The WITH CONSISTENT SNAPSHOT phrase can only be used with the REPEATABLE READ isolation level.
        complete(connection -> connection.beginTransaction(MySqlTransactionDefinition.builder()
                .lockWaitTimeout(Duration.ofSeconds(112))
                .isolationLevel(REPEATABLE_READ)
                .withConsistentSnapshot(true)
                .build())
            .doOnSuccess(ignored -> {
                assertThat(connection.isInTransaction()).isTrue();
                assertThat(connection.getTransactionIsolationLevel()).isEqualTo(REPEATABLE_READ);
                assertThat(connection.isLockWaitTimeoutChanged()).isTrue();
            })
            .then(connection.rollbackTransaction())
            .doOnSuccess(ignored -> {
                assertThat(connection.isInTransaction()).isFalse();
                assertThat(connection.getTransactionIsolationLevel()).isEqualTo(REPEATABLE_READ);
                assertThat(connection.isLockWaitTimeoutChanged()).isFalse();
            }));
    }

    @Test
    void setAutoCommit() {
        complete(connection -> Mono.<Void>fromRunnable(() -> assertThat(connection.isAutoCommit()).isTrue())
            .then(connection.setAutoCommit(false))
            .doOnSuccess(ignored -> assertThat(connection.isAutoCommit()).isFalse())
            .then(connection.setAutoCommit(true))
            .doOnSuccess(ignored -> assertThat(connection.isAutoCommit()).isTrue()));
    }

    @Test
    void autoCommitAutomaticallyTurnedOffInTransaction() {
        complete(connection -> Mono.<Void>fromRunnable(() -> assertThat(connection.isAutoCommit()).isTrue())
                                   .then(connection.beginTransaction())
                                   .doOnSuccess(ignored -> assertThat(connection.isAutoCommit()).isFalse())
                                   .then(connection.commitTransaction())
                                   .doOnSuccess(ignored -> assertThat(connection.isAutoCommit()).isTrue()));
    }

    @Test
    void autoCommitStatusIsRestoredAfterTransaction() {
        complete(connection -> Mono.<Void>fromRunnable(() -> assertThat(connection.isAutoCommit()).isTrue())
                                   .then(connection.setAutoCommit(false))
                                   .doOnSuccess(ignored -> assertThat(connection.isAutoCommit()).isFalse())
                                   .then(connection.beginTransaction())
                                   .doOnSuccess(ignored -> assertThat(connection.isAutoCommit()).isFalse())
                                   .then(connection.commitTransaction())
                                   .doOnSuccess(ignored -> assertThat(connection.isAutoCommit()).isFalse())
                                   .then(connection.setAutoCommit(true))
                                   .doOnSuccess(ignored -> assertThat(connection.isAutoCommit()).isTrue()));
    }

    @ParameterizedTest
    @ValueSource(strings = { "test", "save`point" })
    void createSavepointAndRollbackToSavepoint(String savepoint) {
        complete(connection -> Mono.from(connection.createStatement(
                "CREATE TEMPORARY TABLE test (id INT NOT NULL PRIMARY KEY, name VARCHAR(50))").execute())
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .then(connection.beginTransaction())
            .doOnSuccess(ignored -> assertThat(connection.isInTransaction()).isTrue())
            .then(Mono.from(connection.createStatement("INSERT INTO test VALUES (1, 'test1')")
                .execute()))
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .then(Mono.from(connection.createStatement("INSERT INTO test VALUES (2, 'test2')")
                .execute()))
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .then(Mono.from(connection.createStatement("SELECT COUNT(*) FROM test").execute()))
            .flatMap(result -> Mono.from(result.map((row, metadata) -> row.get(0, Long.class))))
            .doOnSuccess(count -> assertThat(count).isEqualTo(2))
            .then(connection.createSavepoint(savepoint))
            .doOnSuccess(ignored -> assertThat(connection.isInTransaction()).isTrue())
            .then(Mono.from(connection.createStatement("INSERT INTO test VALUES (3, 'test3')")
                .execute()))
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .then(Mono.from(connection.createStatement("INSERT INTO test VALUES (4, 'test4')")
                .execute()))
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .then(Mono.from(connection.createStatement("SELECT COUNT(*) FROM test").execute()))
            .flatMap(result -> Mono.from(result.map((row, metadata) -> row.get(0, Long.class))))
            .doOnSuccess(count -> assertThat(count).isEqualTo(4))
            .then(connection.rollbackTransactionToSavepoint(savepoint))
            .doOnSuccess(ignored -> assertThat(connection.isInTransaction()).isTrue())
            .then(Mono.from(connection.createStatement("SELECT COUNT(*) FROM test").execute()))
            .flatMap(result -> Mono.from(result.map((row, metadata) -> row.get(0, Long.class))))
            .doOnSuccess(count -> assertThat(count).isEqualTo(2))
            .then(connection.rollbackTransaction())
            .doOnSuccess(ignored -> assertThat(connection.isInTransaction()).isFalse())
            .then(Mono.from(connection.createStatement("SELECT COUNT(*) FROM test").execute()))
            .flatMap(result -> Mono.from(result.map((row, metadata) -> row.get(0, Long.class))))
            .doOnSuccess(count -> assertThat(count).isEqualTo(0))
        );
    }

    @ParameterizedTest
    @ValueSource(strings = { "test", "save`point" })
    void createSavepointAndRollbackEntireTransaction(String savepoint) {
        complete(connection -> Mono.from(connection.createStatement(
                                           "CREATE TEMPORARY TABLE test (id INT NOT NULL PRIMARY KEY, name VARCHAR(50))").execute())
                                   .flatMap(IntegrationTestSupport::extractRowsUpdated)
                                   .then(connection.beginTransaction())
                                   .doOnSuccess(ignored -> assertThat(connection.isInTransaction()).isTrue())
                                   .then(Mono.from(connection.createStatement("INSERT INTO test VALUES (1, 'test1')")
                                                             .execute()))
                                   .flatMap(IntegrationTestSupport::extractRowsUpdated)
                                   .then(Mono.from(connection.createStatement("INSERT INTO test VALUES (2, 'test2')")
                                                             .execute()))
                                   .flatMap(IntegrationTestSupport::extractRowsUpdated)
                                   .then(Mono.from(connection.createStatement("SELECT COUNT(*) FROM test").execute()))
                                   .flatMap(result -> Mono.from(result.map((row, metadata) -> row.get(0, Long.class))))
                                   .doOnSuccess(count -> assertThat(count).isEqualTo(2))
                                   .then(connection.createSavepoint(savepoint))
                                   .doOnSuccess(ignored -> assertThat(connection.isInTransaction()).isTrue())
                                   .then(Mono.from(connection.createStatement("INSERT INTO test VALUES (3, 'test3')")
                                                             .execute()))
                                   .flatMap(IntegrationTestSupport::extractRowsUpdated)
                                   .then(Mono.from(connection.createStatement("INSERT INTO test VALUES (4, 'test4')")
                                                             .execute()))
                                   .flatMap(IntegrationTestSupport::extractRowsUpdated)
                                   .then(Mono.from(connection.createStatement("SELECT COUNT(*) FROM test").execute()))
                                   .flatMap(result -> Mono.from(result.map((row, metadata) -> row.get(0, Long.class))))
                                   .doOnSuccess(count -> assertThat(count).isEqualTo(4))
                                   .then(connection.rollbackTransaction())
                                   .doOnSuccess(ignored -> assertThat(connection.isInTransaction()).isFalse())
                                   .then(Mono.from(connection.createStatement("SELECT COUNT(*) FROM test").execute()))
                                   .flatMap(result -> Mono.from(result.map((row, metadata) -> row.get(0, Long.class))))
                                   .doOnSuccess(count -> assertThat(count).isEqualTo(0))
        );
    }

    @Test
    void commitTransactionWithoutBegin() {
        complete(MySqlConnection::commitTransaction);
    }

    @Test
    void rollbackTransactionWithoutBegin() {
        complete(MySqlConnection::rollbackTransaction);
    }

    @Test
    void setTransactionIsolationLevel() {
        complete(connection -> Flux.just(READ_UNCOMMITTED, READ_COMMITTED, REPEATABLE_READ, SERIALIZABLE)
            .concatMap(level -> connection.setTransactionIsolationLevel(level)
                .map(ignored -> assertThat(level))
                .doOnNext(a -> a.isEqualTo(connection.getTransactionIsolationLevel()))));
    }

    @Test
    void errorPropagteRequestQueue() {
        illegalArgument(connection -> Flux.merge(
                                connection.createStatement("SELECT 'Result 1', SLEEP(1)").execute(),
                                connection.createStatement("SELECT 'Result 2'").execute(),
                                connection.createStatement("SELECT 'Result 3'").execute()
                        ).flatMap(result -> result.map((row, meta) -> row.get(0, Integer.class)))
        );
    }

    @Test
    void commitTransactionShouldRespectQueuedMessages() {
        final String tdl = "CREATE TEMPORARY TABLE test (id INT NOT NULL PRIMARY KEY, name VARCHAR(50))";
        complete(connection ->
                         Mono.from(connection.createStatement(tdl).execute())
                             .flatMap(IntegrationTestSupport::extractRowsUpdated)
                             .thenMany(Flux.merge(
                                             connection.beginTransaction(),
                                             connection.createStatement("INSERT INTO test VALUES (1, 'test1')")
                                                       .execute(),
                                             connection.commitTransaction()
                                     ))
                             .doOnComplete(() -> assertThat(connection.isInTransaction()).isFalse())
                             .thenMany(connection.createStatement("SELECT COUNT(*) FROM test").execute())
                             .flatMap(result ->
                                              Mono.from(result.map((row, metadata) -> row.get(0, Long.class)))
                             )
                             .doOnNext(text -> assertThat(text).isEqualTo(1L))
        );
    }

    @Test
    void rollbackTransactionShouldRespectQueuedMessages() {
        final String tdl = "CREATE TEMPORARY TABLE test (id INT NOT NULL PRIMARY KEY, name VARCHAR(50))";
        complete(connection ->
                         Mono.from(connection.createStatement(tdl).execute())
                             .flatMap(IntegrationTestSupport::extractRowsUpdated)
                             .thenMany(Flux.merge(
                                     connection.beginTransaction(),
                                     connection.createStatement("INSERT INTO test VALUES (1, 'test1')")
                                               .execute(),
                                     connection.rollbackTransaction()
                             ))
                             .doOnComplete(() -> assertThat(connection.isInTransaction()).isFalse())
                             .thenMany(connection.createStatement("SELECT COUNT(*) FROM test").execute())
                             .flatMap(result -> Mono.from(result.map((row, metadata) -> row.get(0, Long.class)))
                             .doOnNext(count -> assertThat(count).isEqualTo(0L)))
        );
    }

    @Test
    void beginTransactionShouldRespectQueuedMessages() {
        final String tdl = "CREATE TEMPORARY TABLE test (id INT NOT NULL PRIMARY KEY, name VARCHAR(50))";
        complete(connection ->
                         Mono.from(connection.createStatement(tdl).execute())
                             .flatMap(IntegrationTestSupport::extractRowsUpdated)
                             .then(Mono.from(connection.beginTransaction()))
                             .doOnSuccess(ignored -> assertThat(connection.isInTransaction()).isTrue())
                             .thenMany(Flux.merge(
                                     connection.createStatement("INSERT INTO test VALUES (1, 'test1')").execute(),
                                     connection.commitTransaction(),
                                     connection.beginTransaction()
                             ))
                             .doOnComplete(() -> assertThat(connection.isInTransaction()).isTrue())
                             .then(Mono.from(connection.rollbackTransaction()))
                             .doOnSuccess(ignored -> assertThat(connection.isInTransaction()).isFalse())
                             .thenMany(connection.createStatement("SELECT COUNT(*) FROM test").execute())
                             .flatMap(result -> Mono.from(result.map((row, metadata) -> row.get(0, Long.class)))
                             .doOnNext(count -> assertThat(count).isEqualTo(1L)))
        );
    }

    @Test
    void loadDataLocalInfileRestricted() throws URISyntaxException {
        URL safeUrl = Objects.requireNonNull(getClass().getResource("/local/"));
        URL unsafeUrl = Objects.requireNonNull(getClass().getResource("/"));
        Path safePath = Paths.get(safeUrl.toURI());
        Path path = Paths.get(unsafeUrl.toURI()).resolve("logback-test.xml");

        process(connection -> Mono.from(connection.createStatement("CREATE TEMPORARY TABLE test" +
                "(id INT NOT NULL PRIMARY KEY, name VARCHAR(50))").execute())
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .thenMany(connection.createStatement("LOAD DATA LOCAL INFILE '" + path +
                "' INTO TABLE test").execute())
            .flatMap(IntegrationTestSupport::extractRowsUpdated))
            .verifyErrorMatches(msg -> msg instanceof R2dbcPermissionDeniedException &&
                msg.getMessage().contains(path.toString()) && msg.getMessage().contains(safePath.toString()));
    }

    @ParameterizedTest
    @ValueSource(strings = { "stations", "users" })
    @SuppressWarnings("SqlSourceToSinkFlow")
    void loadDataLocalInfile(String name) throws URISyntaxException, IOException {
        URL tdlUrl = Objects.requireNonNull(getClass().getResource(String.format("/local/%s.sql", name)));
        URL csvUrl = Objects.requireNonNull(getClass().getResource(String.format("/local/%s.csv", name)));
        URL jsonUrl = Objects.requireNonNull(getClass().getResource(String.format("/local/%s.json", name)));
        String tdl = new String(Files.readAllBytes(Paths.get(tdlUrl.toURI())), StandardCharsets.UTF_8);
        String path = Paths.get(csvUrl.toURI()).toString();
        String loadData = String.format("LOAD DATA LOCAL INFILE '%s' INTO TABLE `%s` " +
            "FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'", path, name);
        String select = String.format("SELECT * FROM `%s` ORDER BY `id`", name);
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode arrayNode = (ArrayNode) mapper.readTree(jsonUrl);
        String json = mapper.writeValueAsString(arrayNode);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        complete(conn -> conn.createStatement(tdl)
            .execute()
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .thenMany(conn.createStatement(loadData).execute())
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .doOnNext(it -> assertThat(it).isEqualTo(arrayNode.size()))
            .thenMany(conn.createStatement(select).execute())
            .flatMap(result -> result.map((row, metadata) -> {
                ObjectNode node = mapper.createObjectNode();

                for (ColumnMetadata column : metadata.getColumnMetadatas()) {
                    String columnName = column.getName();
                    Object value = row.get(columnName);

                    if (value instanceof TemporalAccessor) {
                        node.set(columnName, node.textNode(formatter.format((TemporalAccessor) value)));
                    } else if (value instanceof Long) {
                        node.set(columnName, node.numberNode(((Long) value)));
                    } else if (value instanceof Integer) {
                        node.set(columnName, node.numberNode(((Integer) value)));
                    } else if (value instanceof String) {
                        node.set(columnName, node.textNode(((String) value)));
                    } else if (value == null) {
                        node.set(columnName, node.nullNode());
                    } else {
                        throw new IllegalArgumentException("Unsupported type: " + value.getClass());
                    }
                }

                return node;
            }))
            .collectList()
            .<String>handle((list, sink) -> {
                ArrayNode array = mapper.createArrayNode();

                for (ObjectNode node : list) {
                    array.add(node);
                }

                try {
                    sink.next(mapper.writeValueAsString(array));
                } catch (JsonProcessingException e) {
                    sink.error(e);
                }
            })
            .doOnNext(it -> assertThat(it).isEqualTo(json)));
    }

    @Test
    void batchCrud() {
        // TODO: spilt it to multiple test cases and move it to BatchIntegrationTest
        String isEven = "id % 2 = 0";
        String isOdd = "id % 2 = 1";
        String firstData = "first-data";
        String secondData = "second-data";
        String thirdData = "third-data";
        String fourthData = "fourth-data";
        String fifthData = "fifth-data";
        String sixthData = "sixth-data";
        String seventhData = "seventh-data";

        complete(connection -> {
            MySqlBatch selectBatch = connection.createBatch();
            MySqlBatch insertBatch = connection.createBatch();
            MySqlBatch updateBatch = connection.createBatch();
            MySqlBatch deleteBatch = connection.createBatch();
            MySqlStatement selectStmt = connection.createStatement(formattedSelect(""));

            selectBatch.add(formattedSelect(isEven));
            selectBatch.add(formattedSelect(isOdd));

            insertBatch.add(formattedInsert(firstData));
            insertBatch.add(formattedInsert(secondData));
            insertBatch.add(formattedInsert(thirdData));
            insertBatch.add(formattedInsert(fourthData));
            insertBatch.add(formattedInsert(fifthData));

            updateBatch.add(formattedUpdate(sixthData, isEven));
            updateBatch.add(formattedUpdate(seventhData, isOdd));

            deleteBatch.add(formattedDelete(isOdd));
            deleteBatch.add(formattedDelete(isEven));

            String tdl = "CREATE TEMPORARY TABLE test(id INT PRIMARY KEY AUTO_INCREMENT,value VARCHAR(20))";

            return Mono.from(connection.createStatement(tdl)
                    .execute())
                .thenMany(insertBatch.execute())
                .concatMap(r -> Mono.from(r.getRowsUpdated()))
                .doOnNext(updated -> assertThat(updated).isEqualTo(1))
                .reduce(Math::addExact)
                .doOnNext(all -> assertThat(all).isEqualTo(5))
                .then(Mono.from(selectStmt.execute()))
                .flatMapMany(result -> result.map((row, metadata) -> row.get("value", String.class)))
                .collectList()
                .doOnNext(data -> assertThat(data)
                    .isEqualTo(Arrays.asList(firstData, secondData, thirdData, fourthData, fifthData)))
                .thenMany(updateBatch.execute())
                .concatMap(r -> Mono.from(r.getRowsUpdated()))
                .collectList()
                .doOnNext(updated -> assertThat(updated).isEqualTo(Arrays.asList(2L, 3L)))
                .thenMany(selectBatch.execute())
                .concatMap(result -> result.map((row, metadata) -> row.get("value", String.class)))
                .collectList()
                .doOnNext(data -> assertThat(data)
                    .isEqualTo(Arrays.asList(sixthData, sixthData, seventhData, seventhData, seventhData)))
                .thenMany(deleteBatch.execute())
                .concatMap(r -> Mono.from(r.getRowsUpdated()))
                .collectList()
                .doOnNext(deleted -> assertThat(deleted).isEqualTo(Arrays.asList(3L, 2L)))
                .then();
        });
    }

    private static String formattedSelect(String condition) {
        if (condition.isEmpty()) {
            return "SELECT id,value FROM test ORDER BY id";
        }
        return String.format("SELECT id,value FROM test WHERE %s ORDER BY id", condition);
    }

    private static String formattedInsert(String data) {
        return String.format("INSERT INTO test(value)VALUES('%s')", data);
    }

    private static String formattedUpdate(String data, String condition) {
        return String.format("UPDATE test SET value='%s' WHERE %s", data, condition);
    }

    private static String formattedDelete(String condition) {
        return String.format("DELETE FROM test WHERE %s", condition);
    }
}
