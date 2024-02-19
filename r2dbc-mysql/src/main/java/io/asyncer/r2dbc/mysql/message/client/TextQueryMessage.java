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
import java.util.Objects;
import reactor.core.publisher.Mono;

import java.nio.charset.Charset;

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * A plain text SQL query message, it could include multi-statements.
 */
public final class TextQueryMessage implements ClientMessage {

    static final byte QUERY_FLAG = 3;

    private final String sql;

    /**
     * Creates a {@link TextQueryMessage} without parameter.
     *
     * @param sql plain text SQL, should not contain any parameter placeholder.
     * @throws IllegalArgumentException if {@code sql} is {@code null}.
     */
    public TextQueryMessage(String sql) {
        requireNonNull(sql, "sql must not be null");

        this.sql = sql;
    }

    @Override
    public Mono<ByteBuf> encode(ByteBufAllocator allocator, ConnectionContext context) {
        requireNonNull(allocator, "allocator must not be null");
        requireNonNull(context, "context must not be null");

        Charset charset = context.getClientCollation().getCharset();

        return Mono.fromSupplier(() -> {
            ByteBuf buf = allocator.buffer();

            try {
                buf.writeByte(QUERY_FLAG).writeCharSequence(sql, charset);
                return buf;
            } catch (Throwable e) {
                // Maybe IndexOutOfBounds or OOM (too large sql)
                buf.release();
                throw e;
            }
        });
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TextQueryMessage that = (TextQueryMessage) o;
        return Objects.equals(sql, that.sql);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sql);
    }

    @Override
    public String toString() {
        return "TextQueryMessage{sql=REDACTED}";
    }
}
