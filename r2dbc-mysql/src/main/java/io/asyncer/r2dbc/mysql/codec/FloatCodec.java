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
import io.asyncer.r2dbc.mysql.constant.MySqlType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;

/**
 * Codec for {@code float}.
 */
final class FloatCodec extends AbstractPrimitiveCodec<Float> {

    static final FloatCodec INSTANCE = new FloatCodec();

    private FloatCodec() {
        super(Float.TYPE, Float.class);
    }

    @Override
    public Float decode(ByteBuf value, MySqlReadableMetadata metadata, Class<?> target, boolean binary,
        CodecContext context) {
        MySqlType type = metadata.getType();

        if (binary) {
            return decodeBinary(value, type);
        }

        switch (metadata.getType()) {
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case BIGINT_UNSIGNED:
                return Float.parseFloat(value.toString(StandardCharsets.US_ASCII));
            default:
                return (float) CodecUtils.parseLong(value);
        }
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof Float;
    }

    @Override
    public MySqlParameter encode(Object value, CodecContext context) {
        return new FloatMySqlParameter((Float) value);
    }

    @Override
    public boolean doCanDecode(MySqlReadableMetadata metadata) {
        return metadata.getType().isNumeric();
    }

    private static float decodeBinary(ByteBuf buf, MySqlType type) {
        switch (type) {
            case BIGINT_UNSIGNED:
                long v = buf.readLongLE();

                if (v < 0) {
                    return CodecUtils.unsignedBigInteger(v).floatValue();
                }

                return v;
            case BIGINT:
                return buf.readLongLE();
            case INT_UNSIGNED:
                return buf.readUnsignedIntLE();
            case INT:
            case MEDIUMINT_UNSIGNED:
            case MEDIUMINT:
                return buf.readIntLE();
            case SMALLINT_UNSIGNED:
                return buf.readUnsignedShortLE();
            case SMALLINT:
            case YEAR:
                return buf.readShortLE();
            case TINYINT_UNSIGNED:
                return buf.readUnsignedByte();
            case TINYINT:
                return buf.readByte();
            case DECIMAL:
                return Float.parseFloat(buf.toString(StandardCharsets.US_ASCII));
            case FLOAT:
                return buf.readFloatLE();
            case DOUBLE:
                return (float) buf.readDoubleLE();
        }

        throw new IllegalStateException("Cannot decode type " + type + " as a Float");
    }

    private static final class FloatMySqlParameter extends AbstractMySqlParameter {

        private final float value;

        private FloatMySqlParameter(float value) {
            this.value = value;
        }

        @Override
        public Mono<ByteBuf> publishBinary(final ByteBufAllocator allocator) {
            return Mono.fromSupplier(() -> {
                ByteBuf buf = allocator.buffer(Float.BYTES);
                try {
                    return buf.writeFloatLE(value);
                } catch (Throwable e) {
                    buf.release();
                    throw e;
                }
            });
        }

        @Override
        public Mono<Void> publishText(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.writeFloat(value));
        }

        @Override
        public MySqlType getType() {
            return MySqlType.FLOAT;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof FloatMySqlParameter)) {
                return false;
            }

            FloatMySqlParameter that = (FloatMySqlParameter) o;

            return Float.compare(that.value, value) == 0;
        }

        @Override
        public int hashCode() {
            return (value != 0.0f ? Float.floatToIntBits(value) : 0);
        }

        @Override
        public String toString() {
            return Float.toString(value);
        }
    }
}
