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

package io.asyncer.r2dbc.mysql.message.client;

import io.asyncer.r2dbc.mysql.Capability;
import io.asyncer.r2dbc.mysql.ConnectionContextTest;
import io.asyncer.r2dbc.mysql.authentication.MySqlAuthProvider;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link HandshakeResponse41}.
 */
class HandshakeResponse41Test {

    private static final int SSL_REQUEST_41_SIZE = 32;

    @Test
    void writeNullTerminatedAuthForNonSecureConnection() {
        byte[] authentication = "scramble".getBytes(StandardCharsets.US_ASCII);
        HandshakeResponse41 response = new HandshakeResponse41(
            Capability.of(0x271F), 45, "user", authentication, MySqlAuthProvider.MYSQL_OLD_PASSWORD,
            "", Collections.emptyMap(), 0);
        ByteBuf buf = response.encode(UnpooledByteBufAllocator.DEFAULT, ConnectionContextTest.mock()).block();

        try {
            buf.skipBytes(SSL_REQUEST_41_SIZE);
            skipCString(buf);

            byte[] actual = new byte[authentication.length];
            buf.readBytes(actual);

            assertThat(actual).isEqualTo(authentication);
            assertThat(buf.readByte()).isZero();
        } finally {
            buf.release();
        }
    }

    private static void skipCString(ByteBuf buf) {
        int length = buf.bytesBefore((byte) 0);

        buf.skipBytes(length + 1);
    }
}
