/*
 * Copyright 2026 asyncer.io projects
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

package io.asyncer.r2dbc.mysql.client;

import io.asyncer.r2dbc.mysql.ConnectionContextTest;
import io.asyncer.r2dbc.mysql.MySqlConnectionConfigurationTest;
import io.asyncer.r2dbc.mysql.constant.SslMode;
import io.netty.buffer.ByteBufAllocator;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;

class SslBridgeHandlerTest {

    @ParameterizedTest
    @EnumSource(value = SslMode.class,
        names = { "PREFERRED", "REQUIRED", "VERIFY_CA", "VERIFY_IDENTITY", "TUNNEL" })
    void disablesNettyEndpointIdentification(SslMode mode) throws Exception {
        assertThat(SslBridgeHandler.MySqlSslContextSpec
            .forClient(MySqlConnectionConfigurationTest.newSsl(mode), ConnectionContextTest.mock())
            .sslContext().newEngine(ByteBufAllocator.DEFAULT).getSSLParameters()
            .getEndpointIdentificationAlgorithm()).isNull();
    }
}
