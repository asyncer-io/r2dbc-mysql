/*
 * Copyright 2025 asyncer.io projects
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
import io.asyncer.r2dbc.mysql.constant.MySqlType;
import io.asyncer.r2dbc.mysql.internal.util.VarIntUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import reactor.core.publisher.Mono;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import static io.asyncer.r2dbc.mysql.internal.util.InternalArrays.EMPTY_BYTES;

/**
 * Codec for {@link InputStream}.
 */
final class ByteArrayInputStreamCodec extends AbstractClassedCodec<ByteArrayInputStream> {

    static final ByteArrayInputStreamCodec INSTANCE = new ByteArrayInputStreamCodec();

    private ByteArrayInputStreamCodec() {
        super(ByteArrayInputStream.class);
    }

    @Override
    public ByteArrayInputStream decode(ByteBuf value, MySqlReadableMetadata metadata, Class<?> target, boolean binary,
        CodecContext context) {
        if (!value.isReadable()) {
            return new ByteArrayInputStream(EMPTY_BYTES);
        }
        return new ByteArrayInputStream(value.array());
    }

    @Override
    protected boolean doCanDecode(MySqlReadableMetadata metadata) {
        return metadata.getType().isBinary();
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof ByteArrayInputStream;
    }

    @Override
    public MySqlParameter encode(Object value, CodecContext context) {
        return new ByteArrayInputStreamMysqlParameter((ByteArrayInputStream) value);
    }

    private static final class ByteArrayInputStreamMysqlParameter extends AbstractMySqlParameter {

        private final ByteArrayInputStream value;

        private ByteArrayInputStreamMysqlParameter(ByteArrayInputStream value) {
            this.value = value;
        }

        @Override
        public Mono<ByteBuf> publishBinary(ByteBufAllocator allocator) {
            return Mono.fromSupplier(() -> {
                int size = value.available();
                if (size == 0) {
                    return allocator.buffer(Byte.BYTES).writeByte(0);
                }

                int addedSize = VarIntUtils.varIntBytes(size);
                ByteBuf buf = allocator.buffer(addedSize + size);

                try {
                    VarIntUtils.writeVarInt(buf, size);
                    int readBytes = buf.writeBytes(value, size);
                    if (readBytes != size) {
                        buf.release();
                        throw new IllegalStateException("Expected to read " + size + " bytes, but got " + readBytes);
                    }

                    return buf;
                } catch (Exception e) {
                    buf.release();
                    throw new RuntimeException(e);
                }
            });
        }

        @Override
        public Mono<Void> publishText(ParameterWriter writer) {
            return Mono.fromRunnable(() -> {
                try {
                    int size = value.available();
                    byte[] byteArray = new byte[size];
                    int readBytes = value.read(byteArray);

                    if (size != 0 && readBytes != size) {
                        throw new IllegalStateException("Expected to read " + size + " bytes, but got " + readBytes);
                    }

                    writer.writeHex(byteArray);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }

        @Override
        public String toString() {
            return value.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ByteArrayInputStreamMysqlParameter)) {
                return false;
            }

            ByteArrayInputStreamMysqlParameter that = (ByteArrayInputStreamMysqlParameter) o;
            return value.equals(that.value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }

        @Override
        public MySqlType getType() {
            return MySqlType.VARBINARY;
        }
    }
}
