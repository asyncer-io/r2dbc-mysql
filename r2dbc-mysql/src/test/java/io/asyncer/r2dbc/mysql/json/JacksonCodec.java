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

package io.asyncer.r2dbc.mysql.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.asyncer.r2dbc.mysql.MySqlParameter;
import io.asyncer.r2dbc.mysql.ParameterWriter;
import io.asyncer.r2dbc.mysql.api.MySqlReadableMetadata;
import io.asyncer.r2dbc.mysql.codec.CodecContext;
import io.asyncer.r2dbc.mysql.codec.ParameterizedCodec;
import io.asyncer.r2dbc.mysql.constant.MySqlType;
import io.asyncer.r2dbc.mysql.internal.util.VarIntUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.ParameterizedType;
import java.nio.charset.Charset;

/**
 * A JSON codec based on Jackson.
 */
public final class JacksonCodec implements ParameterizedCodec<Object> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Mode mode;

    public JacksonCodec(Mode mode) {
        this.mode = mode;
    }

    @Override
    public Object decode(ByteBuf value, MySqlReadableMetadata metadata, Class<?> target, boolean binary,
        CodecContext context) {
        Charset charset = metadata.getCharCollation(context).getCharset();

        try (Reader r = new InputStreamReader(new ByteBufInputStream(value), charset)) {
            return MAPPER.readValue(r, target);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object decode(ByteBuf value, MySqlReadableMetadata metadata, ParameterizedType target, boolean binary,
        CodecContext context) {
        Charset charset = metadata.getCharCollation(context).getCharset();

        try (Reader r = new InputStreamReader(new ByteBufInputStream(value), charset)) {
            return MAPPER.readValue(r, MAPPER.constructType(target));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MySqlParameter encode(Object value, CodecContext context) {
        return new JacksonMySqlParameter(value, context);
    }

    @Override
    public boolean canDecode(MySqlReadableMetadata metadata, Class<?> target) {
        return doCanDecode(metadata);
    }

    @Override
    public boolean canDecode(MySqlReadableMetadata metadata, ParameterizedType target) {
        return doCanDecode(metadata);
    }

    @Override
    public boolean canEncode(Object value) {
        return mode.isEncode();
    }

    private boolean doCanDecode(MySqlReadableMetadata metadata) {
        return mode.isDecode() && (metadata.getType() == MySqlType.JSON || metadata.getType() == MySqlType.TEXT);
    }

    private static final class JacksonMySqlParameter implements MySqlParameter {

        private final Object value;

        private final CodecContext context;

        private JacksonMySqlParameter(Object value, CodecContext context) {
            this.value = value;
            this.context = context;
        }

        @Override
        public Mono<ByteBuf> publishBinary(final ByteBufAllocator allocator) {
            return Mono.fromSupplier(() -> {
                int reserved;
                Charset charset = context.getClientCollation().getCharset();
                ByteBuf content = allocator.buffer();

                try (Writer w = new OutputStreamWriter(new ByteBufOutputStream(content), charset)) {
                    VarIntUtils.reserveVarInt(content);
                    reserved = content.readableBytes();
                    MAPPER.writeValue(w, value);
                } catch (IOException e) {
                    content.release();
                    throw new RuntimeException(e);
                } catch (Throwable e) {
                    content.release();
                    throw e;
                }

                try {
                    return VarIntUtils.setReservedVarInt(content, content.readableBytes() - reserved);
                } catch (Throwable e) {
                    content.release();
                    throw e;
                }
            });
        }

        @Override
        public Mono<Void> publishText(ParameterWriter writer) {
            return Mono.fromRunnable(() -> {
                try {
                    MAPPER.writeValue(writer, value);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        @Override
        public MySqlType getType() {
            return MySqlType.VARCHAR;
        }

        @Override
        public String toString() {
            return value.toString();
        }
    }

    public enum Mode {

        ALL {
            @Override
            boolean isEncode() {
                return true;
            }

            @Override
            boolean isDecode() {
                return true;
            }
        },
        ENCODE {
            @Override
            boolean isEncode() {
                return true;
            }

            @Override
            boolean isDecode() {
                return false;
            }
        },
        DECODE {
            @Override
            boolean isEncode() {
                return false;
            }

            @Override
            boolean isDecode() {
                return true;
            }
        };

        abstract boolean isEncode();

        abstract boolean isDecode();
    }
}
