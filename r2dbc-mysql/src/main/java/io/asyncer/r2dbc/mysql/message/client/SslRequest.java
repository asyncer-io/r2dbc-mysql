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

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.require;

/**
 * An abstraction of {@link ClientMessage} that considers SSL request for handshake.
 */
public interface SslRequest extends SubsequenceClientMessage {

    /**
     * Get current {@link Capability} of the connection.
     *
     * @return the {@link Capability}.
     */
    Capability getCapability();

    /**
     * Construct an instance of {@link SslRequest}, it is implemented by the protocol version that is given by
     * {@link Capability}.
     *
     * @param capability  the current {@link Capability}.
     * @param collationId the {@code CharCollation} ID, or 0 if server does not return a collation ID.
     * @return the instance implemented by the specified protocol version.
     */
    static SslRequest from(Capability capability, int collationId) {
        require(capability.isSslEnabled(), "capability must be SSL enabled");

        if (capability.isProtocol41()) {
            return new SslRequest41(capability, collationId);
        }

        return new SslRequest320(capability);
    }
}
