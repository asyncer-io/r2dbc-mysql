/*
 * Copyright 2025 asyncer.io projects
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

import io.asyncer.r2dbc.mysql.authentication.MySqlAuthProvider;
import io.asyncer.r2dbc.mysql.client.Client;
import io.asyncer.r2dbc.mysql.constant.CompressionAlgorithm;
import io.asyncer.r2dbc.mysql.constant.SslMode;
import io.asyncer.r2dbc.mysql.message.client.AuthResponse;
import io.r2dbc.spi.R2dbcPermissionDeniedException;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.ZoneId;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link HandshakeExchangeable#createAuthResponse(String)} covering the behaviour for
 * {@code caching_sha2_password} full authentication over different transports.
 * <p>
 * MySQL treats Unix domain socket connections as a secure transport and accepts plaintext passwords over
 * them, consistent with TLS.  A connection whose {@link ConnectionContext#isUnixSocket()} returns
 * {@code true} must therefore be allowed to complete full authentication without SSL.
 */
class InitFlowUnixSocketTest {

    private static final byte[] SALT = new byte[]{
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20
    };

    @Test
    void fullAuthOverUnixSocketWithoutSslSucceeds() throws Exception {
        Client client = mockClient(true);
        HandshakeExchangeable exchangeable = newExchangeable(client, SslMode.DISABLED, null);
        setAuthProvider(exchangeable, MySqlAuthProvider.build(MySqlAuthProvider.CACHING_SHA2_PASSWORD).next());
        setSalt(exchangeable, SALT);

        AuthResponse response = invokeCreateAuthResponse(exchangeable, "full authentication");

        assertThat(response).isNotNull();
    }

    @Test
    void fullAuthOverTcpWithoutSslThrows() throws Exception {
        Client client = mockClient(false);
        HandshakeExchangeable exchangeable = newExchangeable(client, SslMode.DISABLED, null);
        setAuthProvider(exchangeable, MySqlAuthProvider.build(MySqlAuthProvider.CACHING_SHA2_PASSWORD).next());
        setSalt(exchangeable, SALT);

        assertThatThrownBy(() -> invokeCreateAuthResponse(exchangeable, "full authentication"))
            .hasCauseInstanceOf(R2dbcPermissionDeniedException.class);
    }

    @Test
    void fullAuthOverTlsCompletedSucceeds() throws Exception {
        Client client = mockClient(false);
        HandshakeExchangeable exchangeable = newExchangeable(client, SslMode.REQUIRED, null);
        setAuthProvider(exchangeable, MySqlAuthProvider.build(MySqlAuthProvider.CACHING_SHA2_PASSWORD).next());
        setSalt(exchangeable, SALT);
        setSslCompleted(exchangeable, true);

        AuthResponse response = invokeCreateAuthResponse(exchangeable, "full authentication");

        assertThat(response).isNotNull();
    }


    private static Client mockClient(boolean unixSocket) {
        Client client = mock(Client.class);
        when(client.getContext()).thenReturn(ConnectionContextTest.mock(false, ZoneId.systemDefault(), unixSocket));
        return client;
    }

    private static HandshakeExchangeable newExchangeable(Client client, SslMode sslMode,
        String serverRSAPublicKeyFile) {
        return new HandshakeExchangeable(client, sslMode, "testdb", "testuser", "testpass",
            Collections.singleton(CompressionAlgorithm.UNCOMPRESSED), 3, serverRSAPublicKeyFile);
    }

    private static void setAuthProvider(HandshakeExchangeable target, MySqlAuthProvider provider)
        throws NoSuchFieldException, IllegalAccessException {
        Field field = HandshakeExchangeable.class.getDeclaredField("authProvider");
        field.setAccessible(true);
        field.set(target, provider);
    }

    private static void setSalt(HandshakeExchangeable target, byte[] salt)
        throws NoSuchFieldException, IllegalAccessException {
        Field field = HandshakeExchangeable.class.getDeclaredField("salt");
        field.setAccessible(true);
        field.set(target, salt);
    }

    private static void setSslCompleted(HandshakeExchangeable target, boolean value)
        throws NoSuchFieldException, IllegalAccessException {
        Field field = HandshakeExchangeable.class.getDeclaredField("sslCompleted");
        field.setAccessible(true);
        field.set(target, value);
    }

    private static AuthResponse invokeCreateAuthResponse(HandshakeExchangeable target, String phase)
        throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method method = HandshakeExchangeable.class.getDeclaredMethod("createAuthResponse", String.class);
        method.setAccessible(true);
        return (AuthResponse) method.invoke(target, phase);
    }
}
