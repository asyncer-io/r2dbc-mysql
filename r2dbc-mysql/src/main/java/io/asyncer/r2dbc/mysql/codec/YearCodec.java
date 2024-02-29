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
import io.asyncer.r2dbc.mysql.api.MySqlReadableMetadata;
import io.asyncer.r2dbc.mysql.codec.ByteCodec.ByteMySqlParameter;
import io.asyncer.r2dbc.mysql.codec.IntegerCodec.IntMySqlParameter;
import io.asyncer.r2dbc.mysql.codec.ShortCodec.ShortMySqlParameter;
import io.asyncer.r2dbc.mysql.constant.MySqlType;
import io.netty.buffer.ByteBuf;

import java.time.Year;

/**
 * Codec for {@link Year}.
 * <p>
 * Note: unsupported YEAR(2) because it is deprecated feature in MySQL 5.x.
 */
final class YearCodec extends AbstractClassedCodec<Year> {

    static final YearCodec INSTANCE = new YearCodec();

    private YearCodec() {
        super(Year.class);
    }

    @Override
    public Year decode(ByteBuf value, MySqlReadableMetadata metadata, Class<?> target, boolean binary,
        CodecContext context) {
        return binary ? Year.of(value.readShortLE()) : Year.of(CodecUtils.parseInt(value));
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof Year;
    }

    @Override
    public MySqlParameter encode(Object value, CodecContext context) {
        int year = ((Year) value).getValue();

        if ((byte) year == year) {
            return new ByteMySqlParameter((byte) year);
        }
        if ((short) year == year) {
            return new ShortMySqlParameter((short) year);
        }

        // Server does not support it, but still encodes it.
        return new IntMySqlParameter(year);
    }

    @Override
    public boolean doCanDecode(MySqlReadableMetadata metadata) {
        return metadata.getType() == MySqlType.YEAR;
    }
}
