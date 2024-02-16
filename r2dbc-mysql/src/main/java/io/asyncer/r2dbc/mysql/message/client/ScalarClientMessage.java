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
import reactor.core.publisher.Mono;

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link ClientMessage} considers the message can be encoded as a buffer.
 */
abstract class ScalarClientMessage implements ClientMessage {

    abstract protected void writeTo(ByteBuf buf, ConnectionContext context);

    @Override
    public Mono<ByteBuf> encode(ByteBufAllocator allocator, ConnectionContext context) {
        requireNonNull(allocator, "allocator must not be null");
        requireNonNull(context, "context must not be null");

        return Mono.fromSupplier(() -> {
            ByteBuf buf = allocator.buffer();

            try {
                writeTo(buf, context);
                return buf;
            } catch (Throwable e) {
                buf.release();
                throw e;
            }
        });
    }
}
