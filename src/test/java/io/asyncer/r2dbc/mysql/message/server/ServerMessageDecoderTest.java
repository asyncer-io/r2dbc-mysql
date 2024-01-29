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

package io.asyncer.r2dbc.mysql.message.server;

import io.asyncer.r2dbc.mysql.ConnectionContextTest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.assertj.core.api.AbstractObjectAssert;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ServerMessageDecoder}.
 */
class ServerMessageDecoderTest {

    @ParameterizedTest
    @MethodSource(value = { "okLikePayload" })
    void okAndPreparedOk(byte[] okLike) {
        AbstractObjectAssert<?, OkMessage> ok = assertThat(decode(
            Unpooled.wrappedBuffer(okLike), DecodeContext.command()
        )).isExactlyInstanceOf(OkMessage.class).extracting(message -> (OkMessage) message);

        ok.extracting(OkMessage::getAffectedRows).isEqualTo(1L);
        ok.extracting(OkMessage::getLastInsertId).isEqualTo(0x10000L); // 65536
        ok.extracting(OkMessage::getServerStatuses).isEqualTo((short) 0x100); // 256
        ok.extracting(OkMessage::getWarnings).isEqualTo(0);

        AbstractObjectAssert<?, PreparedOkMessage> preparedOk = assertThat(decode(
            Unpooled.wrappedBuffer(okLike), DecodeContext.prepareQuery()
        )).isExactlyInstanceOf(PreparedOkMessage.class).extracting(message -> (PreparedOkMessage) message);

        preparedOk.extracting(PreparedOkMessage::getStatementId).isEqualTo(0xFD01); // 64769
        preparedOk.extracting(PreparedOkMessage::getTotalColumns).isEqualTo(1);
        preparedOk.extracting(PreparedOkMessage::getTotalParameters).isEqualTo(1);
    }

    @Nullable
    private static ServerMessage decode(ByteBuf buf, DecodeContext decodeContext) {
        return new ServerMessageDecoder().decode(buf, ConnectionContextTest.mock(), decodeContext);
    }

    static Stream<byte[]> okLikePayload() {
        return Stream.of(new byte[] {
            0, // Heading both of OK and Prepared OK
            1, // OK: affected rows, Prepared OK: first byte of statement ID
            (byte) 0xFD,
            // OK: VAR_INT_3_BYTE_CODE, last inserted ID is var-int which payload contains 3 bytes
            // Prepared OK: second byte of statement ID
            0, // OK: first byte of last inserted ID payload, Prepared OK: third byte of statement ID
            0, // OK: second byte of last inserted ID payload, Prepared OK: last byte of statement ID
            1, // OK: last byte of last inserted ID payload, Prepared OK: first byte of total columns
            0, // OK: first byte of server statuses, Prepared OK: second byte of total columns
            1, // OK: second byte of server statuses, Prepared OK: first byte of total parameters
            0, // OK: first byte of warnings, Prepared OK: second byte of total parameters
            0 // OK: second byte of warnings, Prepared OK: filter byte for Prepared OK
        });
    }
}
