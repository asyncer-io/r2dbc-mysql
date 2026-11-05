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

import io.asyncer.r2dbc.mysql.client.Client;
import io.asyncer.r2dbc.mysql.client.FluxExchangeable;
import io.asyncer.r2dbc.mysql.codec.Codecs;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class InitFlowLockWaitTimeoutTest {

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void missingVariableKeepsTimeoutUnsupported(boolean configured) throws Exception {
        Client client = failingClient(
            new R2dbcNonTransientResourceException("Unknown system variable", "HY000", 1193));
        Object state = newSessionState();

        load(state, client, configured ? Duration.ofSeconds(10) : null)
            .as(StepVerifier::create)
            .assertNext(result -> assertThat(result).isSameAs(state))
            .verifyComplete();

        verify(client, times(1)).exchange(any(FluxExchangeable.class));
    }

    @ParameterizedTest
    @ValueSource(ints = { 1045, 1064, 2013 })
    void otherDatabaseErrorsArePropagated(int code) throws Exception {
        RuntimeException error = new R2dbcNonTransientResourceException("Failure", "HY000", code);

        load(newSessionState(), failingClient(error), null)
            .as(StepVerifier::create)
            .expectErrorMatches(actual -> actual == error)
            .verify();
    }

    @Test
    void nonDatabaseErrorsArePropagated() throws Exception {
        RuntimeException error = new IllegalStateException("Connection closed");

        load(newSessionState(), failingClient(error), null)
            .as(StepVerifier::create)
            .expectErrorMatches(actual -> actual == error)
            .verify();
    }

    private static Client failingClient(RuntimeException error) {
        Client client = mock(Client.class);
        when(client.exchange(any(FluxExchangeable.class))).thenReturn(Flux.error(error));
        return client;
    }

    private static Object newSessionState() throws Exception {
        Class<?> type = Class.forName("io.asyncer.r2dbc.mysql.InitFlow$SessionState");
        Constructor<?> constructor = type.getDeclaredConstructor(IsolationLevel.class, String.class, ZoneId.class);
        constructor.setAccessible(true);
        return constructor.newInstance(IsolationLevel.REPEATABLE_READ, null, null);
    }

    private static Mono<?> load(Object state, Client client, Duration timeout) throws Exception {
        Method method = InitFlow.class.getDeclaredMethod("loadAndInitInnoDbEngineStatus",
            state.getClass(), Client.class, Codecs.class, Duration.class);
        method.setAccessible(true);
        return (Mono<?>) method.invoke(null, state, client, mock(Codecs.class), timeout);
    }
}
