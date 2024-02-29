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

package io.asyncer.r2dbc.mysql.codec;

import io.asyncer.r2dbc.mysql.MySqlParameter;
import io.asyncer.r2dbc.mysql.ParameterWriter;
import io.asyncer.r2dbc.mysql.api.MySqlReadableMetadata;
import io.asyncer.r2dbc.mysql.codec.lob.LobUtils;
import io.asyncer.r2dbc.mysql.constant.MySqlType;
import io.asyncer.r2dbc.mysql.internal.util.VarIntUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.spi.Clob;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Codec for {@link Clob}.
 */
final class ClobCodec implements MassiveCodec<Clob> {

    static final ClobCodec INSTANCE = new ClobCodec();

    /**
     * It should less than {@link BlobCodec}'s, because we can only make a minimum estimate before writing the
     * string.
     */
    private static final int MAX_MERGE = 1 << 13;

    private ClobCodec() {
    }

    @Override
    public Clob decode(ByteBuf value, MySqlReadableMetadata metadata, Class<?> target, boolean binary,
        CodecContext context) {
        return LobUtils.createClob(value, metadata.getCharCollation(context));
    }

    @Override
    public Clob decodeMassive(List<ByteBuf> value, MySqlReadableMetadata metadata, Class<?> target,
        boolean binary, CodecContext context) {
        return LobUtils.createClob(value, metadata.getCharCollation(context));
    }

    @Override
    public boolean canDecode(MySqlReadableMetadata metadata, Class<?> target) {
        MySqlType type = metadata.getType();

        return (type.isLob() || type == MySqlType.JSON) && target.isAssignableFrom(Clob.class);
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof Clob;
    }

    @Override
    public MySqlParameter encode(Object value, CodecContext context) {
        return new ClobMySqlParameter((Clob) value, context);
    }

    private static final class ClobMySqlParameter extends AbstractLobMySqlParameter {

        private final AtomicReference<Clob> clob;

        private final CodecContext context;

        private ClobMySqlParameter(Clob clob, CodecContext context) {
            this.clob = new AtomicReference<>(clob);
            this.context = context;
        }

        @Override
        public Flux<ByteBuf> publishBinary(final ByteBufAllocator allocator) {
            return Flux.defer(() -> {
                Clob clob = this.clob.getAndSet(null);

                if (clob == null) {
                    return Mono.error(new IllegalStateException("Clob has written, can not write twice"));
                }

                // Must have defaultIfEmpty, try Mono.fromCallable(() -> null).flux().collectList()
                return Flux.from(clob.stream())
                    .collectList()
                    .defaultIfEmpty(Collections.emptyList())
                    .flatMapIterable(list -> {
                        if (list.isEmpty()) {
                            // It is zero of var int, not terminal.
                            return Collections.singletonList(allocator.buffer(Byte.BYTES).writeByte(0));
                        }

                        long bytes = 0;
                        Charset charset = context.getClientCollation().getCharset();
                        List<ByteBuf> buffers = new ArrayList<>();
                        ByteBuf lastBuf = allocator.buffer();

                        try {
                            ByteBuf firstBuf = lastBuf;

                            buffers.add(firstBuf);
                            VarIntUtils.reserveVarInt(firstBuf);

                            for (CharSequence src : list) {
                                int length = src.length();

                                if (length > 0) {
                                    // size + lastBuf.readableBytes() > MAX_MERGE, it just a minimum estimate.
                                    if (length > MAX_MERGE - lastBuf.readableBytes()) {
                                        lastBuf = allocator.buffer();
                                        buffers.add(lastBuf);
                                    }

                                    bytes += lastBuf.writeCharSequence(src, charset);
                                }
                            }

                            VarIntUtils.setReservedVarInt(firstBuf, bytes);

                            return BlobCodec.toList(buffers);
                        } catch (Throwable e) {
                            BlobCodec.releaseAll(buffers, lastBuf);
                            throw e;
                        }
                    });
            });
        }

        @Override
        public Mono<Void> publishText(ParameterWriter writer) {
            return Mono.defer(() -> {
                Clob clob = this.clob.getAndSet(null);

                if (clob == null) {
                    return Mono.error(new IllegalStateException("Clob has written, can not write twice"));
                }

                return Flux.from(clob.stream())
                    .doOnSubscribe(ignored -> writer.startString())
                    .doOnNext(writer::append)
                    .then();
            });
        }

        @Override
        public MySqlType getType() {
            return MySqlType.LONGTEXT;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ClobMySqlParameter)) {
                return false;
            }

            ClobMySqlParameter clobValue = (ClobMySqlParameter) o;

            return Objects.equals(this.clob.get(), clobValue.clob.get());
        }

        @Override
        public int hashCode() {
            Clob clob = this.clob.get();
            return clob == null ? 0 : clob.hashCode();
        }

        @Override
        protected Publisher<Void> getDiscard() {
            Clob clob = this.clob.getAndSet(null);
            return clob == null ? null : clob.discard();
        }

        @Override
        public String toString() {
            Clob clob = this.clob.get();
            return clob == null ? "Clob[MOVED]" : clob.toString();
        }
    }
}
