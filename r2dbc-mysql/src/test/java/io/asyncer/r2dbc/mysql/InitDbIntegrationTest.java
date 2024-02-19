package io.asyncer.r2dbc.mysql;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@code createDatabaseIfNotExist}.
 */
class InitDbIntegrationTest extends IntegrationTestSupport {

    private static final String DATABASE = "test-" + ThreadLocalRandom.current().nextInt(10000);

    InitDbIntegrationTest() {
        super(configuration(builder -> builder.database(DATABASE).createDatabaseIfNotExist(true)));
    }

    @Test
    void shouldCreateDatabase() {
        complete(conn -> conn.createStatement("SHOW DATABASES")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> row.get(0, String.class)))
            .collect(Collectors.toSet())
            .doOnNext(it -> assertThat(it).contains(DATABASE))
            .thenMany(conn.createStatement("DROP DATABASE `" + DATABASE + "`")
                .execute()
                .flatMap(MySqlResult::getRowsUpdated)));
    }
}
