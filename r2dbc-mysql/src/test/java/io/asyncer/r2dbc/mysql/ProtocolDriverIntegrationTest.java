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

import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.ValidationDepth;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for DNS SRV records.
 */
class ProtocolDriverIntegrationTest {

    @ParameterizedTest
    @ValueSource(strings = {
        "r2dbc:mysql+srv:loadbalance://localhost:3306/r2dbc",
    })
    void anyAvailable(String url) {
        // localhost should be resolved to 127.0.0.1 and [::1], but I can't make sure GitHub Actions support IPv6
        MySqlConnectionFactory.from(MySqlConnectionFactoryProvider.setup(setupUrlAndCredentials(url)))
            .create()
            .flatMapMany(connection -> connection.validate(ValidationDepth.REMOTE)
                .onErrorReturn(false)
                .concatWith(connection.close().then(Mono.empty())))
            .as(StepVerifier::create)
            .expectNext(true)
            .verifyComplete();
    }

    private static ConnectionFactoryOptions setupUrlAndCredentials(String url) {
        String password = System.getProperty("test.mysql.password");

        assertThat(password).withFailMessage("Property test.mysql.password must exists and not be empty")
            .isNotNull()
            .isNotEmpty();

        return ConnectionFactoryOptions.parse(url).mutate()
            .option(ConnectionFactoryOptions.USER, "root")
            .option(ConnectionFactoryOptions.PASSWORD, password)
            .option(ConnectionFactoryOptions.CONNECT_TIMEOUT, Duration.ofSeconds(3))
            .build();
    }
}
