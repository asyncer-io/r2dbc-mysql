/*
 * Copyright 2024 asyncer.io projects
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

import io.asyncer.r2dbc.mysql.constant.CompressionAlgorithm;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base class for compression integration tests.
 */
abstract class CompressionIntegrationTestSupport extends IntegrationTestSupport {

    CompressionIntegrationTestSupport(CompressionAlgorithm algorithm) {
        super(configuration(builder -> builder.compressionAlgorithms(algorithm)));
    }

    @Test
    void simpleQuery() {
        byte[] hello = "Hello".getBytes(StandardCharsets.US_ASCII);
        byte[] repeatedBytes = new byte[hello.length * 50];

        for (int i = 0; i < 50; i++) {
            System.arraycopy(hello, 0, repeatedBytes, i * hello.length, hello.length);
        }

        String repeated = new String(repeatedBytes, StandardCharsets.US_ASCII);

        complete(connection -> connection.createStatement("SELECT REPEAT('Hello', 50)").execute()
            .flatMap(result -> result.map((row, rowMetadata) -> row.get(0, String.class)))
            .collectList()
            .doOnNext(actual -> assertThat(actual).isEqualTo(Collections.singletonList(repeated))));
    }

    @ParameterizedTest
    @ValueSource(strings = { "stations", "users" })
    @SuppressWarnings("SqlSourceToSinkFlow")
    void loadDataLocalInfile(String name) throws URISyntaxException, IOException {
        URL tdlUrl = Objects.requireNonNull(getClass().getResource(String.format("/local/%s.sql", name)));
        URL csvUrl = Objects.requireNonNull(getClass().getResource(String.format("/local/%s.csv", name)));
        String tdl = new String(Files.readAllBytes(Paths.get(tdlUrl.toURI())), StandardCharsets.UTF_8);
        String path = Paths.get(csvUrl.toURI()).toString();
        String loadData = String.format("LOAD DATA LOCAL INFILE '%s' INTO TABLE `%s` " +
            "FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'", path, name);
        String select = String.format("SELECT * FROM `%s` ORDER BY `id`", name);
        AtomicInteger count = new AtomicInteger(-1);

        complete(conn -> conn.createStatement(tdl)
            .execute()
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .thenMany(conn.createStatement(loadData).execute())
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .reduce(0L, Long::sum)
            .doOnNext(it -> count.set(it.intValue()))
            .doOnNext(it -> assertThat(it).isGreaterThan(0))
            .thenMany(conn.createStatement(select).execute())
            .flatMap(result -> result.map(r -> 1))
            .reduce(0, Integer::sum)
            .doOnNext(it -> assertThat(it).isEqualTo(count.get())));
    }
}
