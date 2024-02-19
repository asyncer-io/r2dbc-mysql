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
import io.asyncer.r2dbc.mysql.constant.Packets;
import io.netty.buffer.ByteBuf;

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.require;

/**
 * The ssl request message on protocol 4.1. It is also first part of {@link HandshakeResponse41}.
 */
final class SslRequest41 extends SizedClientMessage implements SslRequest {

    private static final int RESERVED_SIZE = 19;

    private static final int MARIA_DB_CAPABILITY_SIZE = Integer.BYTES;

    private static final int BUF_SIZE = Integer.BYTES + Integer.BYTES + Byte.BYTES +
        RESERVED_SIZE + MARIA_DB_CAPABILITY_SIZE;

    private final Capability capability;

    private final int collationId;

    SslRequest41(Capability capability, int collationId) {
        require(collationId > 0, "collationId must be a positive integer");

        this.capability = capability;
        this.collationId = collationId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SslRequest41 that = (SslRequest41) o;

        return collationId == that.collationId &&
            capability.equals(that.capability);
    }

    @Override
    public int hashCode() {
        int result = capability.hashCode();
        return 31 * result + collationId;
    }

    @Override
    public String toString() {
        return "SslRequest41{capability=" + capability + ", collationId=" + collationId + '}';
    }

    @Override
    public Capability getCapability() {
        return capability;
    }

    @Override
    protected int size() {
        return BUF_SIZE;
    }

    @Override
    protected void writeTo(ByteBuf buf) {
        buf.writeIntLE(capability.getBaseBitmap())
            .writeIntLE(Packets.MAX_PAYLOAD_SIZE)
            .writeByte(collationId & 0xFF); // only low 8-bits

        if (capability.isMariaDb()) {
            buf.writeZero(RESERVED_SIZE)
                .writeIntLE(capability.getExtendBitmap());
        } else {
            buf.writeZero(RESERVED_SIZE + MARIA_DB_CAPABILITY_SIZE);
        }
    }

    int getCollationId() {
        return collationId;
    }
}
