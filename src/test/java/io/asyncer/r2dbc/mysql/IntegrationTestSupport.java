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

import io.r2dbc.spi.R2dbcBadGrammarException;
import io.r2dbc.spi.Result;
import org.jetbrains.annotations.Nullable;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.time.ZoneId;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base class considers connection factory and general function for integration tests.
 */
abstract class IntegrationTestSupport {

    private final MySqlConnectionFactory connectionFactory;

    IntegrationTestSupport(MySqlConnectionConfiguration configuration) {
        this.connectionFactory = MySqlConnectionFactory.from(configuration);
    }

    void complete(Function<? super MySqlConnection, Publisher<?>> runner) {
        process(runner).verifyComplete();
    }

    void badGrammar(Function<? super MySqlConnection, Publisher<?>> runner) {
        process(runner).verifyError(R2dbcBadGrammarException.class);
    }

    void illegalArgument(Function<? super MySqlConnection, Publisher<?>> runner) {
        process(runner).expectError(IllegalArgumentException.class).verify(Duration.ofSeconds(3));
    }

    Mono<MySqlConnection> create() {
        return connectionFactory.create();
    }

    private StepVerifier.FirstStep<Void> process(Function<? super MySqlConnection, Publisher<?>> runner) {
        return create()
            .flatMap(connection -> Flux.from(runner.apply(connection))
                .onErrorResume(e -> connection.close().then(Mono.error(e)))
                .concatWith(connection.close().then(Mono.empty()))
                .then())
            .as(StepVerifier::create);
    }

    static Mono<Integer> extractRowsUpdated(Result result) {
        return Mono.from(result.getRowsUpdated());
    }

    static MySqlConnectionConfiguration configuration(
        String database, boolean createDatabaseIfNotExist, boolean autodetectExtensions,
        @Nullable ZoneId serverZoneId, @Nullable Predicate<String> preferPrepared
    ) {
        String password = System.getProperty("test.mysql.password");

        assertThat(password).withFailMessage("Property test.mysql.password must exists and not be empty")
            .isNotNull()
            .isNotEmpty();

        MySqlConnectionConfiguration.Builder builder = MySqlConnectionConfiguration.builder()
            .host("127.0.0.1")
            .connectTimeout(Duration.ofSeconds(3))
            .user("root")
            .password(password)
            .database(database)
            .createDatabaseIfNotExist(createDatabaseIfNotExist)
            .autodetectExtensions(autodetectExtensions);

        if (serverZoneId != null) {
            builder.serverZoneId(serverZoneId);
        }

        if (preferPrepared == null) {
            builder.useClientPrepareStatement();
        } else {
            builder.useServerPrepareStatement(preferPrepared);
        }

        return builder.build();
    }
}
