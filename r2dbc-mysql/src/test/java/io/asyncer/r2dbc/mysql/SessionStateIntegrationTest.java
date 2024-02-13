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

import io.asyncer.r2dbc.mysql.internal.util.StringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.ZoneId;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Integration tests for session states.
 */
class SessionStateIntegrationTest {

    @Test
    void forcedLocalTimeZone() {
        ZoneId zoneId = ZoneId.systemDefault().normalized();

        connectionFactory(builder -> builder.connectionTimeZone("local")
            .forceConnectionTimeZoneToSession(true))
            .create()
            .flatMapMany(
                connection -> connection.createStatement("SELECT @@time_zone").execute()
                    .flatMap(result -> result.map(r -> r.get(0, String.class)))
                    .map(StringUtils::parseZoneId)
                    .onErrorResume(e -> connection.close().then(Mono.error(e)))
                    .concatWith(connection.close().then(Mono.empty()))
            )
            .as(StepVerifier::create)
            .expectNext(zoneId)
            .verifyComplete();
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "America/New_York",
        "Asia/Seoul",
        "Asia/Shanghai",
        "Asia/Tokyo",
        "Europe/London",
        "Factory",
        "GMT",
        "JST",
        "ROC",
        "UTC",
        "+00:00",
        "+09:00",
        "-09:00",
    })
    void forcedConnectionTimeZone(String timeZone) {
        ZoneId zoneId = StringUtils.parseZoneId(timeZone);

        connectionFactory(builder -> builder.connectionTimeZone(timeZone)
            .forceConnectionTimeZoneToSession(true))
            .create()
            .flatMapMany(
                connection -> connection.createStatement("SELECT @@time_zone").execute()
                    .flatMap(result -> result.map(r -> r.get(0, String.class)))
                    .map(StringUtils::parseZoneId)
                    .onErrorResume(e -> connection.close().then(Mono.error(e)))
                    .concatWith(connection.close().then(Mono.empty()))
            )
            .as(StepVerifier::create)
            .expectNext(zoneId)
            .verifyComplete();
    }

    @ParameterizedTest
    @MethodSource
    void sessionVariables(Map<String, String> variables) {
        String[] pairs = variables.entrySet().stream()
            .map(entry -> entry.getKey() + "=" + entry.getValue())
            .toArray(String[]::new);
        String[] keys = variables.keySet().toArray(new String[0]);
        String selection = variables.keySet().stream()
            .map(it -> "@@session." + it + " AS " + it)
            .collect(Collectors.joining(",", "SELECT ", ""));
        Map<String, String> expected = variables.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().startsWith("'") ?
                entry.getValue().substring(1, entry.getValue().length() - 1) : entry.getValue()));

        connectionFactory(builder -> builder.sessionVariables(pairs))
            .create()
            .flatMapMany(connection -> connection.createStatement(selection).execute()
                .flatMap(result -> result.map(r -> {
                    Map<String, String> map = new LinkedHashMap<>();
                    for (String key : keys) {
                        map.put(key, r.get(key, String.class));
                    }
                    return map;
                }))
                .onErrorResume(e -> connection.close().then(Mono.error(e)))
                .concatWith(connection.close().then(Mono.empty()))
            )
            .as(StepVerifier::create)
            .expectNext(expected)
            .verifyComplete();
    }

    static Stream<Arguments> sessionVariables() {
        return Stream.of(
            Arguments.of(mapOf("sql_mode", "ANSI_QUOTES")),
            Arguments.of(mapOf("time_zone", "'+00:00'")),
            Arguments.of(mapOf("sql_mode", "'ANSI_QUOTES,STRICT_ALL_TABLES'", "time_zone", "'Asia/Tokyo'"))
        );
    }

    private static MySqlConnectionFactory connectionFactory(
        Function<MySqlConnectionConfiguration.Builder, MySqlConnectionConfiguration.Builder> customizer
    ) {
        String password = System.getProperty("test.mysql.password");

        if (password == null || password.isEmpty()) {
            throw new IllegalStateException("Property test.mysql.password must exists and not be empty");
        }

        MySqlConnectionConfiguration.Builder builder = MySqlConnectionConfiguration.builder()
            .host("localhost")
            .port(3306)
            .user("root")
            .password(password)
            .database("r2dbc");

        return MySqlConnectionFactory.from(customizer.apply(builder).build());
    }

    private static Map<String, String> mapOf(String... paris) {
        if (paris.length % 2 != 0) {
            throw new IllegalArgumentException("Pairs must be even");
        }

        Map<String, String> map = new LinkedHashMap<>();

        for (int i = 0; i < paris.length; i += 2) {
            map.put(paris[i], paris[i + 1]);
        }

        return map;
    }
}
