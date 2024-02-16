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

import io.asyncer.r2dbc.mysql.Capability;
import io.netty.buffer.ByteBuf;
import io.r2dbc.spi.R2dbcPermissionDeniedException;

/**
 * A MySQL Handshake Request message for multi-versions.
 */
public interface HandshakeRequest extends ServerMessage {

    /**
     * Gets the handshake request header.
     *
     * @return the header.
     */
    HandshakeHeader getHeader();

    /**
     * Gets the server-side capability.
     *
     * @return the server-side capability.
     */
    Capability getServerCapability();

    /**
     * Gets the authentication plugin type name.
     *
     * @return the authentication plugin type.
     */
    String getAuthType();

    /**
     * Gets the challenge salt for authentication.
     *
     * @return the challenge salt.
     */
    byte[] getSalt();

    /**
     * Decodes a {@link HandshakeRequest} from a payload {@link ByteBuf} of a normal packet.
     *
     * @param buf the {@link ByteBuf}.
     * @return decoded {@link HandshakeRequest}.
     */
    static HandshakeRequest decode(ByteBuf buf) {
        HandshakeHeader header = HandshakeHeader.decode(buf);
        int version = header.getProtocolVersion();

        switch (version) {
            case 10:
                return HandshakeV10Request.decode(buf, header);
            case 9:
                return HandshakeV9Request.decode(buf, header);
        }

        throw new R2dbcPermissionDeniedException("Does not support handshake protocol version " + version);
    }
}
