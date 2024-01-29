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
 * The ssl request message on protocol 3.20. It is also first part of {@link HandshakeResponse320}.
 */
final class SslRequest320 extends SizedClientMessage implements SslRequest {

    private static final int SIZE = Short.BYTES + Packets.SIZE_FIELD_SIZE;

    private final Capability capability;

    SslRequest320(Capability capability) {
        require(!capability.isProtocol41(), "protocol 4.1 capability should never be set");

        this.capability = capability;
    }

    @Override
    public Capability getCapability() {
        return capability;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SslRequest320 that = (SslRequest320) o;

        return capability.equals(that.capability);
    }

    @Override
    public int hashCode() {
        return capability.hashCode();
    }

    @Override
    public String toString() {
        return "SslRequest320{capability=" + capability + '}';
    }

    @Override
    protected int size() {
        return SIZE;
    }

    @Override
    protected void writeTo(ByteBuf buf) {
        // Protocol 3.20 only allows low 16-bits capabilities.
        buf.writeShortLE(capability.getBaseBitmap() & 0xFFFF)
            .writeMediumLE(Packets.MAX_PAYLOAD_SIZE);
    }
}
