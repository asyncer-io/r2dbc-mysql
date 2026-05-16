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

package io.asyncer.r2dbc.mysql.message.server;

import io.asyncer.r2dbc.mysql.Capability;
import io.asyncer.r2dbc.mysql.authentication.MySqlAuthProvider;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link HandshakeV10Request}.
 */
class HandshakeV10RequestTest {

    @Test
    void fallbackToNativePasswordWhenPluginAuthIsNotAdvertised() {
        HandshakeRequest request = HandshakeRequest.decode(handshake(0xA71F, ""));

        assertThat(request.getServerCapability().isSaltSecured()).isTrue();
        assertThat(request.getServerCapability().isPluginAuthAllowed()).isFalse();
        assertThat(request.getAuthType()).isEqualTo(MySqlAuthProvider.MYSQL_NATIVE_PASSWORD);
    }

    @Test
    void fallbackToNativePasswordWhenPluginAuthNameIsEmpty() {
        HandshakeRequest request = HandshakeRequest.decode(handshake(0x8A71F, ""));

        assertThat(request.getServerCapability().isSaltSecured()).isTrue();
        assertThat(request.getServerCapability().isPluginAuthAllowed()).isTrue();
        assertThat(request.getAuthType()).isEqualTo(MySqlAuthProvider.MYSQL_NATIVE_PASSWORD);
    }

    @Test
    void fallbackToOldPasswordWhenSecureConnectionIsNotAdvertised() {
        HandshakeRequest request = HandshakeRequest.decode(handshake(0x271F, ""));

        assertThat(request.getServerCapability().isSaltSecured()).isFalse();
        assertThat(request.getServerCapability().isPluginAuthAllowed()).isFalse();
        assertThat(request.getAuthType()).isEqualTo(MySqlAuthProvider.MYSQL_OLD_PASSWORD);
    }

    @Test
    void readAdvertisedAuthPluginName() {
        HandshakeRequest request = HandshakeRequest.decode(handshake(
            0x8A71F, MySqlAuthProvider.MYSQL_NATIVE_PASSWORD));

        assertThat(request.getAuthType()).isEqualTo(MySqlAuthProvider.MYSQL_NATIVE_PASSWORD);
    }

    private static ByteBuf handshake(long capability, String authType) {
        ByteBuf buf = Unpooled.buffer();

        buf.writeByte(10);
        writeCString(buf, "5.5.2");
        buf.writeIntLE(1);
        buf.writeBytes("12345678".getBytes(StandardCharsets.US_ASCII));
        buf.writeByte(0);
        buf.writeShortLE((int) (capability & 0xFFFF));
        buf.writeByte(45);
        buf.writeShortLE(2);
        buf.writeShortLE((int) ((capability >>> Short.SIZE) & 0xFFFF));
        buf.writeByte(21);
        buf.writeZero(10);

        if (Capability.of(capability).isSaltSecured()) {
            buf.writeBytes("abcdefghijkl".getBytes(StandardCharsets.US_ASCII));
            buf.writeByte(0);
        }

        writeCString(buf, authType);

        return buf;
    }

    private static void writeCString(ByteBuf buf, String value) {
        buf.writeBytes(value.getBytes(StandardCharsets.US_ASCII));
        buf.writeByte(0);
    }
}
