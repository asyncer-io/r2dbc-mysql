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

import io.r2dbc.spi.Readable;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

import java.time.ZonedDateTime;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base class considers integration tests for MariaDB.
 */
abstract class MariaDbIntegrationTestSupport extends IntegrationTestSupport {

    MariaDbIntegrationTestSupport(@Nullable Predicate<String> preferPrepared) {
        super(configuration("r2dbc", false, false, null, preferPrepared));
    }

    @Test
    void returningExpression() {
        complete(conn -> conn.createStatement("CREATE TEMPORARY TABLE test (" +
            "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,value INT NOT NULL)")
            .execute()
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .thenMany(conn.createStatement("INSERT INTO test(value) VALUES (?)")
                .bind(0, 2)
                .returnGeneratedValues("CURRENT_TIMESTAMP")
                .execute())
            .flatMap(result -> result.map(r -> r.get(0, ZonedDateTime.class)))
            .collectList()
            .doOnNext(list -> assertThat(list).hasSize(1)
                .noneMatch(it -> it.isBefore(ZonedDateTime.now().minusSeconds(10)))));
    }

    @Test
    void allReturning() {
        complete(conn -> conn.createStatement("CREATE TEMPORARY TABLE test (" +
                "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY," +
                "value INT NOT NULL," +
                "created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3))")
            .execute()
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .thenMany(conn.createStatement("INSERT INTO test(value) VALUES (?),(?),(?),(?),(?)")
                .bind(0, 2)
                .bind(1, 4)
                .bind(2, 6)
                .bind(3, 8)
                .bind(4, 10)
                .returnGeneratedValues()
                .execute())
            .flatMap(result -> result.map(DataEntity::read))
            .collectList()
            .doOnNext(list -> assertThat(list).hasSize(5)
                .map(DataEntity::getValue)
                .containsExactly(2, 4, 6, 8, 10))
            .doOnNext(list -> assertThat(list.stream().map(DataEntity::getId).distinct()).hasSize(5))
            .doOnNext(list -> assertThat(list.stream().map(DataEntity::getCreatedAt))
                .noneMatch(it -> it.isBefore(ZonedDateTime.now().minusSeconds(10))))
            .thenMany(conn.createStatement("REPLACE test(id, value) VALUES (1,?),(2,?),(3,?),(4,?),(5,?)")
                .bind(0, 3)
                .bind(1, 5)
                .bind(2, 7)
                .bind(3, 9)
                .bind(4, 11)
                .returnGeneratedValues()
                .execute())
            .flatMap(result -> result.map(DataEntity::read))
            .collectList()
            .doOnNext(list -> assertThat(list).hasSize(5)
                .map(DataEntity::getValue)
                .containsExactly(3, 5, 7, 9, 11))
            .doOnNext(list -> assertThat(list.stream().map(DataEntity::getCreatedAt))
                .noneMatch(it -> it.isBefore(ZonedDateTime.now().minusSeconds(10)))));
    }

    @Test
    void partialReturning() {
        complete(conn -> conn.createStatement("CREATE TEMPORARY TABLE test (" +
                "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY," +
                "value INT NOT NULL," +
                "created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3))")
            .execute()
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .thenMany(conn.createStatement("INSERT INTO test(value) VALUES (?),(?),(?),(?),(?)")
                .bind(0, 2)
                .bind(1, 4)
                .bind(2, 6)
                .bind(3, 8)
                .bind(4, 10)
                .returnGeneratedValues("id", "created_at")
                .execute())
            .flatMap(result -> result.map(DataEntity::withoutValue))
            .collectList()
            .doOnNext(list -> assertThat(list).hasSize(5)
                .map(DataEntity::getValue)
                .containsOnly(0))
            .doOnNext(list -> assertThat(list.stream().map(DataEntity::getId).distinct()).hasSize(5))
            .doOnNext(list -> assertThat(list.stream().map(DataEntity::getCreatedAt))
                .noneMatch(it -> it.isBefore(ZonedDateTime.now().minusSeconds(10))))
            .thenMany(conn.createStatement("REPLACE test(id, value) VALUES (1,?),(2,?),(3,?),(4,?),(5,?)")
                .bind(0, 3)
                .bind(1, 5)
                .bind(2, 7)
                .bind(3, 9)
                .bind(4, 11)
                .returnGeneratedValues("id", "created_at")
                .execute())
            .flatMap(result -> result.map(DataEntity::withoutValue))
            .collectList()
            .doOnNext(list -> assertThat(list).hasSize(5)
                .map(DataEntity::getValue)
                .containsOnly(0))
            .doOnNext(list -> assertThat(list.stream().map(DataEntity::getCreatedAt))
                .noneMatch(it -> it.isBefore(ZonedDateTime.now().minusSeconds(10))))
        );
    }

    private static final class DataEntity {

        private final int id;

        private final int value;

        private final ZonedDateTime createdAt;

        private DataEntity(int id, int value, ZonedDateTime createdAt) {
            this.id = id;
            this.value = value;
            this.createdAt = createdAt;
        }

        int getId() {
            return id;
        }

        int getValue() {
            return value;
        }

        ZonedDateTime getCreatedAt() {
            return createdAt;
        }

        static DataEntity read(Readable readable) {
            Integer id = readable.get("id", Integer.TYPE);
            Integer value = readable.get("value", Integer.class);
            ZonedDateTime createdAt = readable.get("created_at", ZonedDateTime.class);

            requireNonNull(id, "id must not be null");
            requireNonNull(value, "value must not be null");
            requireNonNull(createdAt, "createdAt must not be null");

            return new DataEntity(id, value, createdAt);
        }

        static DataEntity withoutValue(Readable readable) {
            Integer id = readable.get("id", Integer.TYPE);
            ZonedDateTime createdAt = readable.get("created_at", ZonedDateTime.class);

            requireNonNull(id, "id must not be null");
            requireNonNull(createdAt, "createdAt must not be null");

            return new DataEntity(id, 0, createdAt);
        }
    }
}
