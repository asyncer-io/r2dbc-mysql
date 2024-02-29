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
import io.netty.buffer.ByteBufUtil;
import reactor.core.publisher.Mono;

import java.util.BitSet;

import static io.asyncer.r2dbc.mysql.internal.util.InternalArrays.EMPTY_BYTES;

/**
 * Codec for {@link BitSet}.
 */
final class BitSetCodec extends AbstractClassedCodec<BitSet> {

    static final BitSetCodec INSTANCE = new BitSetCodec();

    private BitSetCodec() {
        super(BitSet.class);
    }

    @Override
    public BitSet decode(ByteBuf value, MySqlReadableMetadata metadata, Class<?> target, boolean binary,
        CodecContext context) {
        if (!value.isReadable()) {
            return BitSet.valueOf(EMPTY_BYTES);
        }

        // Result with big-endian, BitSet is using little-endian, need reverse.
        return BitSet.valueOf(reverse(ByteBufUtil.getBytes(value)));
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof BitSet;
    }

    @Override
    public MySqlParameter encode(Object value, CodecContext context) {
        BitSet set = (BitSet) value;
        long bits;
        if (set.isEmpty()) {
            bits = 0;
        } else {
            long[] array = set.toLongArray();

            // The max precision of BIT is 64, so just use the first Long.
            if (array.length == 0) {
                bits = 0;
            } else {
                bits = array[0];
            }
        }

        MySqlType type;

        if (bits < 0) {
            type = MySqlType.BIGINT;
        } else if ((byte) bits == bits) {
            type = MySqlType.TINYINT;
        } else if ((short) bits == bits) {
            type = MySqlType.SMALLINT;
        } else if ((int) bits == bits) {
            type = MySqlType.INT;
        } else {
            type = MySqlType.BIGINT;
        }

        return new BitSetMySqlParameter(bits, type);
    }

    @Override
    protected boolean doCanDecode(MySqlReadableMetadata metadata) {
        return metadata.getType() == MySqlType.BIT;
    }

    private static byte[] reverse(byte[] bytes) {
        int maxIndex = bytes.length - 1;
        int half = bytes.length >>> 1;
        byte b;

        for (int i = 0; i < half; ++i) {
            b = bytes[i];
            bytes[i] = bytes[maxIndex - i];
            bytes[maxIndex - i] = b;
        }

        return bytes;
    }

    private static final class BitSetMySqlParameter extends AbstractMySqlParameter {

        private final long value;

        private final MySqlType type;

        private BitSetMySqlParameter(long value, MySqlType type) {
            this.value = value;
            this.type = type;
        }

        @Override
        public Mono<ByteBuf> publishBinary(final ByteBufAllocator allocator) {
            switch (type) {
                case TINYINT:
                    return Mono.fromSupplier(() -> allocator.buffer(Byte.BYTES).writeByte((int) value));
                case SMALLINT:
                    return Mono.fromSupplier(() -> allocator.buffer(Short.BYTES).writeShortLE((int) value));
                case INT:
                    return Mono.fromSupplier(() -> allocator.buffer(Integer.BYTES).writeIntLE((int) value));
                default: // BIGINT
                    return Mono.fromSupplier(() -> allocator.buffer(Long.BYTES).writeLongLE(value));
            }
        }

        @Override
        public Mono<Void> publishText(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.writeUnsignedLong(value));
        }

        @Override
        public MySqlType getType() {
            return type;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof BitSetMySqlParameter)) {
                return false;
            }

            BitSetMySqlParameter that = (BitSetMySqlParameter) o;

            return value == that.value;
        }

        @Override
        public int hashCode() {
            return (int) (value ^ (value >>> 32));
        }

        @Override
        public String toString() {
            return Long.toBinaryString(value);
        }
    }
}
