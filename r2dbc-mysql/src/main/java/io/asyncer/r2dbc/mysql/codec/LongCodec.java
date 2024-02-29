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
import io.asyncer.r2dbc.mysql.codec.ByteCodec.ByteMySqlParameter;
import io.asyncer.r2dbc.mysql.codec.IntegerCodec.IntMySqlParameter;
import io.asyncer.r2dbc.mysql.codec.ShortCodec.ShortMySqlParameter;
import io.asyncer.r2dbc.mysql.constant.MySqlType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

/**
 * Codec for {@code long}.
 */
final class LongCodec extends AbstractPrimitiveCodec<Long> {

    static final LongCodec INSTANCE = new LongCodec();

    private LongCodec() {
        super(Long.TYPE, Long.class);
    }

    @Override
    public Long decode(ByteBuf value, MySqlReadableMetadata metadata, Class<?> target, boolean binary,
        CodecContext context) {
        MySqlType type = metadata.getType();

        if (binary) {
            return decodeBinary(value, type);
        }

        switch (type) {
            case FLOAT:
                return (long) Float.parseFloat(value.toString(StandardCharsets.US_ASCII));
            case DOUBLE:
                return (long) Double.parseDouble(value.toString(StandardCharsets.US_ASCII));
            case DECIMAL:
                return decimalLong(value);
            default:
                return CodecUtils.parseLong(value);
        }
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof Long;
    }

    @Override
    public MySqlParameter encode(Object value, CodecContext context) {
        return encodeLong((Long) value);
    }

    @Override
    public boolean canPrimitiveDecode(MySqlReadableMetadata metadata) {
        return metadata.getType().isNumeric();
    }

    static MySqlParameter encodeLong(long v) {
        if ((byte) v == v) {
            return new ByteMySqlParameter((byte) v);
        }
        if ((short) v == v) {
            return new ShortMySqlParameter((short) v);
        }
        if ((int) v == v) {
            return new IntMySqlParameter((int) v);
        }

        return new LongMySqlParameter(v);
    }

    private static long decodeBinary(ByteBuf buf, MySqlType type) {
        switch (type) {
            case BIGINT_UNSIGNED:
            case BIGINT:
                return buf.readLongLE();
            case INT_UNSIGNED:
                return buf.readUnsignedIntLE();
            case INT:
            case MEDIUMINT_UNSIGNED:
            case MEDIUMINT:
                // Note: MySQL return 32-bits two's complement for 24-bits integer
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
                return decimalLong(buf);
            case FLOAT:
                return (long) buf.readFloatLE();
            case DOUBLE:
                return (long) buf.readDoubleLE();
        }

        throw new IllegalStateException("Cannot decode type " + type + " as a Long");
    }

    private static long decimalLong(ByteBuf buf) {
        return new BigDecimal(buf.toString(StandardCharsets.US_ASCII)).longValue();
    }

    private static final class LongMySqlParameter extends AbstractMySqlParameter {

        private final long value;

        private LongMySqlParameter(long value) {
            this.value = value;
        }

        @Override
        public Mono<ByteBuf> publishBinary(final ByteBufAllocator allocator) {
            return Mono.fromSupplier(() -> allocator.buffer(Long.BYTES).writeLongLE(value));
        }

        @Override
        public Mono<Void> publishText(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.writeLong(value));
        }

        @Override
        public MySqlType getType() {
            return MySqlType.BIGINT;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof LongMySqlParameter)) {
                return false;
            }

            LongMySqlParameter longValue = (LongMySqlParameter) o;

            return value == longValue.value;
        }

        @Override
        public int hashCode() {
            return (int) (value ^ (value >>> 32));
        }

        @Override
        public String toString() {
            return Long.toString(value);
        }
    }
}
