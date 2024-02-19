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

package io.asyncer.r2dbc.mysql.message.client;

import io.asyncer.r2dbc.mysql.Capability;
import io.asyncer.r2dbc.mysql.ConnectionContext;
import io.asyncer.r2dbc.mysql.internal.util.VarIntUtils;
import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * A handshake response message for protocol version 4.1.
 *
 * @see SslRequest41 the header of {@link HandshakeResponse41}.
 */
final class HandshakeResponse41 extends ScalarClientMessage implements HandshakeResponse {

    private static final int ONE_BYTE_MAX_INT = 0xFF;

    private final SslRequest41 header;

    private final String user;

    private final byte[] authentication;

    private final String authType;

    private final String database;

    private final Map<String, String> attributes;

    private final int zstdCompressionLevel;

    HandshakeResponse41(Capability capability, int collationId, String user, byte[] authentication,
        String authType, String database, Map<String, String> attributes, int zstdCompressionLevel) {
        this.header = new SslRequest41(capability, collationId);
        this.user = requireNonNull(user, "user must not be null");
        this.authentication = requireNonNull(authentication, "authentication must not be null");
        this.database = requireNonNull(database, "database must not be null");
        this.authType = requireNonNull(authType, "authType must not be null");
        this.attributes = requireNonNull(attributes, "attributes must not be null");
        this.zstdCompressionLevel = zstdCompressionLevel;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HandshakeResponse41)) {
            return false;
        }

        HandshakeResponse41 that = (HandshakeResponse41) o;

        return zstdCompressionLevel == that.zstdCompressionLevel && header.equals(that.header) &&
            user.equals(that.user) && Arrays.equals(authentication, that.authentication) &&
            authType.equals(that.authType) && database.equals(that.database) &&
            attributes.equals(that.attributes);
    }

    @Override
    public int hashCode() {
        int result = header.hashCode();
        result = 31 * result + user.hashCode();
        result = 31 * result + Arrays.hashCode(authentication);
        result = 31 * result + authType.hashCode();
        result = 31 * result + database.hashCode();
        result = 31 * result + attributes.hashCode();
        return 31 * result + zstdCompressionLevel;
    }

    @Override
    public String toString() {
        return "HandshakeResponse41{capability=" + header.getCapability() +
            ", collationId=" + header.getCollationId() + ", user='" + user +
            "', authentication=REDACTED, authType='" + authType +
            "', database='" + database + "', attributes=" + attributes +
            ", zstdCompressionLevel=" + zstdCompressionLevel
            + '}';
    }

    @Override
    protected void writeTo(ByteBuf buf, ConnectionContext context) {
        header.writeTo(buf);

        Capability capability = header.getCapability();
        Charset charset = context.getClientCollation().getCharset();

        HandshakeResponse.writeCString(buf, user, charset);

        if (capability.isVarIntSizedAuthAllowed()) {
            writeVarIntSizedBytes(buf, authentication);
        } else if (authentication.length <= ONE_BYTE_MAX_INT) {
            buf.writeByte(authentication.length).writeBytes(authentication);
        } else {
            // Auth change message will be sent by server.
            buf.writeByte(0);
        }

        if (capability.isConnectWithDatabase()) {
            HandshakeResponse.writeCString(buf, database, charset);
        }

        if (capability.isPluginAuthAllowed()) {
            // This must be a UTF-8 string.
            HandshakeResponse.writeCString(buf, authType, StandardCharsets.UTF_8);
        }

        if (capability.isConnectionAttributesAllowed()) {
            writeAttrs(buf, charset);
        }

        if (capability.isZstdCompression()) {
             buf.writeByte(zstdCompressionLevel);
        }
    }

    private void writeAttrs(ByteBuf buf, Charset charset) {
        if (attributes.isEmpty()) {
            // It is zero of var int, not terminal.
            buf.writeByte(0);
            return;
        }

        final ByteBuf attributesBuf = buf.alloc().buffer();

        try {
            for (Map.Entry<String, String> entry : attributes.entrySet()) {
                writeVarIntString(attributesBuf, entry.getKey(), charset);
                writeVarIntString(attributesBuf, entry.getValue(), charset);
            }

            writeVarIntSizedBytes(buf, attributesBuf);
        } finally {
            attributesBuf.release();
        }
    }

    private static void writeVarIntString(ByteBuf buf, String value, Charset charset) {
        if (value.isEmpty()) {
            // It is zero of var int, not terminal.
            buf.writeByte(0);
            return;
        }

        // NEVER use value.length() in here, size must be bytes' size, not string size.
        // Can not use reserved var integer, because this buffer header has been used.
        writeVarIntSizedBytes(buf, value.getBytes(charset));
    }

    private static void writeVarIntSizedBytes(ByteBuf buf, byte[] value) {
        int size = value.length;

        if (size == 0) {
            // It is zero of var int, not terminal.
            buf.writeByte(0);
            return;
        }

        VarIntUtils.writeVarInt(buf, size);
        buf.writeBytes(value);
    }

    private static void writeVarIntSizedBytes(ByteBuf buf, ByteBuf value) {
        int size = value.readableBytes();

        if (size == 0) {
            // It is zero of var int, not terminal.
            buf.writeByte(0);
            return;
        }

        VarIntUtils.writeVarInt(buf, size);
        buf.writeBytes(value);
    }
}
