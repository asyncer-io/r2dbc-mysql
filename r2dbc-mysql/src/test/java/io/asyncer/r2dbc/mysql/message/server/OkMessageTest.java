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

package io.asyncer.r2dbc.mysql.message.server;

import io.asyncer.r2dbc.mysql.ConnectionContext;
import io.asyncer.r2dbc.mysql.ConnectionContextTest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link OkMessage}.
 */
class OkMessageTest {

    @Test
    void decodeSessionVariables() {
        boolean isMariaDb = "mariadb".equalsIgnoreCase(System.getProperty("test.db.type"));
        ConnectionContext context = ConnectionContextTest.mock(isMariaDb);
        OkMessage message = OkMessage.decode(true, sessionVariablesOk(), context);

        assertThat(message.getAffectedRows()).isOne();
        assertThat(message.getLastInsertId()).isEqualTo(2);
        assertThat(message.getServerStatuses()).isEqualTo((short) 0x4000);
        assertThat(message.getWarnings()).isEqualTo(3);
        assertThat(message.getSystemVariable("autocommit")).isEqualTo("OFF");
    }

    private static ByteBuf sessionVariablesOk() {
        return Unpooled.wrappedBuffer(new byte[] {
            0,
            1, 2, 0, 0x40, 3, 0, 0, 0x11, 0, 0xf, 0xa,
            0x61, 0x75, 0x74, 0x6f, 0x63, 0x6f, 0x6d, 0x6d, 0x69, 0x74, 0x3, 0x4f, 0x46, 0x46,
        });
    }
}
