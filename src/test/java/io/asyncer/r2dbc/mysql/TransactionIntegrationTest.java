package io.asyncer.r2dbc.mysql;

import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Result;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class TransactionIntegrationTest extends IntegrationTestSupport {

    private final MySqlConnection connection;

    TransactionIntegrationTest() {
        super(configuration(false, null, sql -> false));
        connection = create().block();
    }


    @Test
    void savepointsSynchronized() {

        createTable(connection);

        connection.beginTransaction()
                  .as(StepVerifier::create)
                  .verifyComplete();

        Flux.from(connection.createStatement("INSERT INTO r2dbc_example VALUES(?p0, ?p1, ?p2)")
                            .bind(0, 0).bind(1, "Walter").bind(2, "White").add()
                            .bind(0, 1).bind(1, "Jesse").bind(2, "Pinkman").execute())
            .flatMap(Result::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNextCount(2)
            .verifyComplete();

        connection.createSavepoint("savepoint")
                  .as(StepVerifier::create)
                  .verifyComplete();

        Flux.from(connection.createStatement("INSERT INTO r2dbc_example VALUES(?p0, ?p1, ?p2)")
                            .bind(0, 2).bind(1, "Hank").bind(2, "Schrader").execute())
            .flatMap(Result::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();

        Flux.from(connection.createStatement("SELECT COUNT(*) FROM r2dbc_example")
                            .execute())
            .flatMap(result -> result.map((row, rowMetadata) -> row.get(0, Integer.class)))
            .as(StepVerifier::create)
            .expectNext(3)
            .verifyComplete();

        connection.rollbackTransactionToSavepoint("savepoint")
                  .as(StepVerifier::create)
                  .verifyComplete();

        Flux.from(connection.createStatement("SELECT COUNT(*) FROM r2dbc_example /* in-tx */")
                            .execute())
            .flatMap(result -> result.map((row, rowMetadata) -> row.get(0, Integer.class)))
            .as(StepVerifier::create)
            .expectNext(2)
            .verifyComplete();

        connection.commitTransaction()
                  .as(StepVerifier::create)
                  .verifyComplete();

        Flux.from(connection.createStatement("SELECT COUNT(*) FROM r2dbc_example /* after-tx */")
                            .execute())
            .flatMap(result -> result.map((row, rowMetadata) -> row.get(0, Integer.class)))
            .as(StepVerifier::create)
            .expectNext(2)
            .verifyComplete();
    }

    @Test
    void savepointsConcatWith() {

        createTable(connection);

        connection.beginTransaction()
                  .cast(Object.class)
                  .concatWith(Flux.from(connection.createStatement("INSERT INTO r2dbc_example VALUES(?p0, ?p1, ?p2)")
                                                  .bind(0, 0).bind(1, "Walter").bind(2, "White").execute())
                                  .flatMap(Result::getRowsUpdated))
                  .concatWith(connection.createSavepoint("savepoint.1"))
                  .concatWith(Flux.from(connection.createStatement("INSERT INTO r2dbc_example VALUES(?p0, ?p1, ?p2)")
                                                  .bind(0, 2).bind(1, "Hank").bind(2, "Schrader").execute())
                                  .flatMap(Result::getRowsUpdated))
                  .concatWith(connection.rollbackTransactionToSavepoint("savepoint.1"))
                  .concatWith(Flux.from(connection.createStatement("SELECT COUNT(*) FROM r2dbc_example /* in-tx */")
                                                  .execute())
                                  .flatMap(result -> result.map((row, rowMetadata) -> row.get(0, Integer.class))))
                  .concatWith(connection.commitTransaction())
                  .concatWith(Flux.from(connection.createStatement("SELECT COUNT(*) FROM r2dbc_example /* after-tx */")
                                                  .execute())
                                  .flatMap(result -> result.map((row, rowMetadata) -> row.get(0, Integer.class))))
                  .as(StepVerifier::create)
                  .expectNext(1L).as("Affected Rows Count from first INSERT")
                  .expectNext(1L).as("Affected Rows Count from second INSERT")
                  .expectNext(1).as("SELECT COUNT(*) after ROLLBACK TO SAVEPOINT")
                  .expectNext(1).as("SELECT COUNT(*) after COMMIT")
                  .verifyComplete();
    }

    @Test
    void autoCommitDisabled() {

        createTable(connection);

        assertThat(connection.isAutoCommit()).isTrue();

        connection.setAutoCommit(false)
                  .as(StepVerifier::create)
                  .verifyComplete();

        assertThat(connection.isAutoCommit()).isFalse();

        connection.createStatement("INSERT INTO r2dbc_example VALUES(0, 'Walter', 'White')").execute()
                  .<Object>flatMap(Result::getRowsUpdated)
                  .concatWith(connection.rollbackTransaction())
                  .as(StepVerifier::create)
                  .expectNext(1L).as("Affected Rows Count from first INSERT")
                  .verifyComplete();

        create().flatMapMany(c -> c.createStatement("SELECT * FROM r2dbc_example")
                                   .execute().flatMap(it -> it.map((row, metadata) -> row.get("first_name"))))
                .as(StepVerifier::create)
                .verifyComplete();
    }

    @Test
    void savepointStartsTransaction() {

        createTable(connection);


        connection.createStatement("INSERT INTO r2dbc_example VALUES(1, 'Jesse', 'Pinkman')").execute()
                  .flatMap(Result::getRowsUpdated)
                  .as(StepVerifier::create)
                  .expectNext(1L)
                  .verifyComplete();

        connection.createSavepoint("s1")
                  .thenMany(
                          connection.createStatement("INSERT INTO r2dbc_example VALUES(0, 'Walter', 'White')").execute()
                                    .<Object>flatMap(Result::getRowsUpdated))
                  .concatWith(Mono.fromSupplier(() -> connection.isAutoCommit()))
                  .concatWith(connection.rollbackTransaction())
                  .as(StepVerifier::create)
                  .expectNext(1L).as("Affected Rows Count from first INSERT")
                  .expectNext(false).as("Auto-commit disabled by createSavepoint")
                  .verifyComplete();

        connection.createStatement("INSERT INTO r2dbc_example VALUES(0, 'Walter', 'White')").execute()
                  .<Object>flatMap(Result::getRowsUpdated)
                  .concatWith(Mono.fromSupplier(() -> connection.isAutoCommit()))
                  .concatWith(connection.rollbackTransaction()) // cannot be rollbacked since outside of transaction
                  .as(StepVerifier::create)
                  .expectNext(1L).as("Affected Rows Count from second INSERT")
                  .expectNext(true).as("auto-commit is back to initial state")
                  .verifyComplete();

        create().flatMapMany(c -> c.createStatement("SELECT * FROM r2dbc_example")
                                   .execute().flatMap(it -> it.map((row, metadata) -> row.get("first_name"))))
                .as(StepVerifier::create)
                .expectNext("Walter")
                .expectNext("Jesse")
                .verifyComplete();
    }

    @Test
    void commitTransaction() {

        createTable(connection);

        connection.beginTransaction()
                  .as(StepVerifier::create)
                  .verifyComplete();

        Flux.from(connection.createStatement("INSERT INTO r2dbc_example VALUES(?p0, ?p1, ?p2)")
                            .bind(0, 0).bind(1, "Walter").bind(2, "White").add()
                            .bind(0, 1).bind(1, "Jesse").bind(2, "Pinkman").execute())
            .flatMap(Result::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNextCount(2)
            .verifyComplete();

        connection.commitTransaction()
                  .as(StepVerifier::create)
                  .verifyComplete();

        Flux.from(connection.createStatement("SELECT COUNT(*) FROM r2dbc_example")
                            .execute())
            .flatMap(result -> result.map((row, rowMetadata) -> row.get(0, Integer.class)))
            .as(StepVerifier::create)
            .expectNext(2)
            .verifyComplete();
    }

    @Test
    void rollbackTransaction() {

        createTable(connection);

        connection.beginTransaction()
                  .as(StepVerifier::create)
                  .verifyComplete();

        Flux.from(connection.createStatement("INSERT INTO r2dbc_example VALUES(?p0, ?p1, ?p2)")
                            .bind(0, 0).bind(1, "Walter").bind(2, "White").add()
                            .bind(0, 1).bind(1, "Jesse").bind(2, "Pinkman").execute())
            .flatMap(Result::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNextCount(2)
            .verifyComplete();

        connection.rollbackTransaction()
                  .as(StepVerifier::create)
                  .verifyComplete();

        Flux.from(connection.createStatement("SELECT COUNT(*) FROM r2dbc_example")
                            .execute())
            .flatMap(result -> result.map((row, rowMetadata) -> row.get(0, Long.class)))
            .as(StepVerifier::create)
            .expectNext(0L)
            .verifyComplete();
    }

    @Test
    void shouldIsolationLevelApplied() {

        createTable(connection);

        assertThat(connection.getTransactionIsolationLevel()).isEqualTo(IsolationLevel.REPEATABLE_READ);

        connection.beginTransaction(MySqlTransactionDefinition.builder()
                                                              .isolationLevel(IsolationLevel.READ_UNCOMMITTED)
                                                              .lockWaitTimeout(Duration.ofMinutes(1))
                                                              .build())
                  .as(StepVerifier::create)
                  .verifyComplete();


        assertThat(connection.isInTransaction()).isTrue();
        assertThat(connection.getTransactionIsolationLevel()).isEqualTo(IsolationLevel.READ_UNCOMMITTED);

        connection.createStatement("SHOW VARIABLES LIKE 'innodb_lock_wait_timeout'").execute()
                  .flatMap(it -> it.map((row, rowMetadata) -> row.get(1, String.class)))
                  .as(StepVerifier::create)
                  .expectNext(String.valueOf(TimeUnit.MINUTES.toSeconds(1)))
                  .verifyComplete();

        final MySqlConnection connection2 = create().block();
        connection2.beginTransaction().as(StepVerifier::create).verifyComplete();
        connection2.createStatement("INSERT INTO r2dbc_example VALUES(0, 'Walter', 'White')")
                   .execute()
                   .flatMap(Result::getRowsUpdated)
                   .as(StepVerifier::create)
                   .expectNext(1L)
                   .verifyComplete();

        connection.createStatement("SELECT * FROM r2dbc_example")
                  .execute()
                  .flatMap(it -> it.map((row, rowMetadata) -> row.get("first_name")))
                  .as(StepVerifier::create)
                  .expectNext("Walter")
                  .verifyComplete();


        connection2.rollbackTransaction().as(StepVerifier::create).verifyComplete();
        connection.createStatement("SELECT COUNT(*) FROM r2dbc_example")
                  .execute()
                  .flatMap(result -> result.map((row, rowMetadata) -> row.get(0, Long.class)))
                  .as(StepVerifier::create)
                  .expectNext(0L)
                  .verifyComplete();
        connection.rollbackTransaction().as(StepVerifier::create).verifyComplete();
    }

    @Test
    void shouldResetIsolationLevelAfterTransaction() {

        getIsolationLevel()
                .as(StepVerifier::create)
                .expectNext(IsolationLevel.REPEATABLE_READ)
                .verifyComplete();
        assertThat(connection.getTransactionIsolationLevel()).isEqualTo(IsolationLevel.REPEATABLE_READ);

        connection.beginTransaction(IsolationLevel.READ_UNCOMMITTED).as(StepVerifier::create).verifyComplete();

        assertThat(connection.getTransactionIsolationLevel()).isEqualTo(IsolationLevel.READ_UNCOMMITTED);
        connection.rollbackTransaction().as(StepVerifier::create).verifyComplete();

        assertThat(connection.getTransactionIsolationLevel()).isEqualTo(IsolationLevel.REPEATABLE_READ);
    }


    Mono<IsolationLevel> getIsolationLevel() {

        return connection.createStatement("SELECT @@transaction_isolation").execute()
                         .flatMap(it -> it.map((row, rowMetadata) -> row.get(0, String.class)))
                         .map(it -> {

                             switch (it) {
                             case "READ-UNCOMMITTED":
                                 return IsolationLevel.READ_UNCOMMITTED;
                             case "READ-COMMITTED":
                                 return IsolationLevel.READ_COMMITTED;
                             case "SERIALIZABLE":
                                 return IsolationLevel.SERIALIZABLE;
                             case "REPEATABLE-READ":
                                 return IsolationLevel.REPEATABLE_READ;
                             }
                             return null;
                         }).single();
    }

    private static void createTable(MySqlConnection connection) {

        connection.createStatement("DROP TABLE r2dbc_example")
                  .execute()
                  .flatMap(MySqlResult::getRowsUpdated)
                  .onErrorResume(e -> Mono.empty())
                  .thenMany(connection.createStatement("CREATE TABLE r2dbc_example (" +
                                                       "id int PRIMARY KEY, " +
                                                       "first_name varchar(255), " +
                                                       "last_name varchar(255))")
                                      .execute().flatMap(MySqlResult::getRowsUpdated).then())
                  .as(StepVerifier::create)
                  .verifyComplete();
    }

}
