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

import java.nio.charset.Charset;

/**
 * Codec for {@code enum class}.
 */
final class EnumCodec implements Codec<Enum<?>> {

    static final EnumCodec INSTANCE = new EnumCodec();

    private EnumCodec() {
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public Enum<?> decode(ByteBuf value, MySqlReadableMetadata metadata, Class<?> target, boolean binary,
        CodecContext context) {
        Charset charset = metadata.getCharCollation(context).getCharset();

        return Enum.valueOf((Class<Enum>) target, value.toString(charset));
    }

    @Override
    public boolean canDecode(MySqlReadableMetadata metadata, Class<?> target) {
        return metadata.getType() == MySqlType.ENUM && target.isEnum();
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof Enum<?>;
    }

    @Override
    public MySqlParameter encode(Object value, CodecContext context) {
        return new EnumMySqlParameter((Enum<?>) value, context);
    }

    private static final class EnumMySqlParameter extends AbstractMySqlParameter {

        private final Enum<?> value;

        private final CodecContext context;

        private EnumMySqlParameter(Enum<?> value, CodecContext context) {
            this.value = value;
            this.context = context;
        }

        @Override
        public Mono<ByteBuf> publishBinary(final ByteBufAllocator allocator) {
            return Mono.fromSupplier(() -> StringCodec.encodeCharSequence(allocator, value.name(), context));
        }

        @Override
        public Mono<Void> publishText(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.write(value.name()));
        }

        @Override
        public MySqlType getType() {
            return MySqlType.VARCHAR;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof EnumMySqlParameter)) {
                return false;
            }

            EnumMySqlParameter enumValue = (EnumMySqlParameter) o;

            return value.equals(enumValue.value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }

        @Override
        public String toString() {
            return value.toString();
        }
    }
}
