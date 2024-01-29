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

import io.asyncer.r2dbc.mysql.ConnectionContext;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import reactor.core.CorePublisher;
import reactor.core.publisher.Flux;

/**
 * A message sent from a MySQL client to a MySQL server.
 */
public interface ClientMessage {

    default boolean isSequenceReset() {
        return true;
    }

    /**
     * Returns whether the encoded buffers can be cumulated to maximize the payload size.
     *
     * @return {@code true} if can be cumulated.
     */
    default boolean isCumulative() {
        return true;
    }

    /**
     * Encode a message into {@link ByteBuf}s.
     *
     * @param allocator the {@link ByteBufAllocator} that use to get {@link ByteBuf} to write into.
     * @param context   current MySQL connection context.
     * @return a {@link Flux} that's produces the encoded {@link ByteBuf}s.
     * @throws IllegalArgumentException if {@code allocator} or {@code context} is {@code null}.
     */
    CorePublisher<ByteBuf> encode(ByteBufAllocator allocator, ConnectionContext context);
}
