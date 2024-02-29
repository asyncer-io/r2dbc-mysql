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
import io.asyncer.r2dbc.mysql.internal.util.InternalArrays;
import io.asyncer.r2dbc.mysql.message.FieldValue;
import io.asyncer.r2dbc.mysql.message.LargeFieldValue;
import io.asyncer.r2dbc.mysql.message.NormalFieldValue;
import io.r2dbc.spi.Parameter;
import org.jetbrains.annotations.Nullable;

import javax.annotation.concurrent.GuardedBy;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link Codecs}.
 */
final class DefaultCodecs implements Codecs {

    private final Codec<?>[] codecs;

    private final ParametrizedCodec<?>[] parametrizedCodecs;

    private final MassiveCodec<?>[] massiveCodecs;

    private final MassiveParametrizedCodec<?>[] massiveParametrizedCodecs;

    private final Map<Type, PrimitiveCodec<?>> primitiveCodecs;

    private DefaultCodecs(Codec<?>[] codecs) {
        this.codecs = requireNonNull(codecs, "codecs must not be null");

        Map<Type, PrimitiveCodec<?>> primitiveCodecs = new HashMap<>();
        List<ParametrizedCodec<?>> parametrizedCodecs = new ArrayList<>();
        List<MassiveCodec<?>> massiveCodecs = new ArrayList<>();
        List<MassiveParametrizedCodec<?>> massiveParamCodecs = new ArrayList<>();

        for (Codec<?> codec : codecs) {
            if (codec instanceof PrimitiveCodec<?>) {
                // Primitive codec must be class-based codec, cannot support ParameterizedType.
                PrimitiveCodec<?> c = (PrimitiveCodec<?>) codec;
                primitiveCodecs.put(c.getPrimitiveClass(), c);
            } else if (codec instanceof ParametrizedCodec<?>) {
                parametrizedCodecs.add((ParametrizedCodec<?>) codec);
            }

            if (codec instanceof MassiveCodec<?>) {
                massiveCodecs.add((MassiveCodec<?>) codec);

                if (codec instanceof MassiveParametrizedCodec<?>) {
                    massiveParamCodecs.add((MassiveParametrizedCodec<?>) codec);
                }
            }
        }

        this.primitiveCodecs = primitiveCodecs;
        this.massiveCodecs = massiveCodecs.toArray(new MassiveCodec<?>[0]);
        this.massiveParametrizedCodecs = massiveParamCodecs.toArray(new MassiveParametrizedCodec<?>[0]);
        this.parametrizedCodecs = parametrizedCodecs.toArray(new ParametrizedCodec<?>[0]);
    }

    /**
     * Note: this method should NEVER release {@code buf} because of it come from {@code MySqlRow} which will
     * release this buffer.
     */
    @Override
    public <T> T decode(FieldValue value, MySqlReadableMetadata metadata, Class<?> type, boolean binary,
        CodecContext context) {
        requireNonNull(value, "value must not be null");
        requireNonNull(metadata, "info must not be null");
        requireNonNull(context, "context must not be null");
        requireNonNull(type, "type must not be null");

        if (value.isNull()) {
            // T is always an object, so null should be returned even if the type is a primitive class.
            // See also https://github.com/mirromutth/r2dbc-mysql/issues/184 .
            return null;
        }

        Class<?> target = chooseClass(metadata, type);

        // Fast map for primitive classes.
        if (target.isPrimitive()) {
            return decodePrimitive(value, metadata, target, binary, context);
        } else if (value instanceof NormalFieldValue) {
            return decodeNormal((NormalFieldValue) value, metadata, target, binary, context);
        } else if (value instanceof LargeFieldValue) {
            return decodeMassive((LargeFieldValue) value, metadata, target, binary, context);
        }

        throw new IllegalArgumentException("Unknown value " + value.getClass().getSimpleName());
    }

    @Override
    public <T> T decode(FieldValue value, MySqlReadableMetadata metadata, ParameterizedType type,
        boolean binary, CodecContext context) {
        requireNonNull(value, "value must not be null");
        requireNonNull(metadata, "info must not be null");
        requireNonNull(context, "context must not be null");
        requireNonNull(type, "type must not be null");

        if (value.isNull()) {
            return null;
        } else if (value instanceof NormalFieldValue) {
            return decodeNormal((NormalFieldValue) value, metadata, type, binary, context);
        } else if (value instanceof LargeFieldValue) {
            return decodeMassive((LargeFieldValue) value, metadata, type, binary, context);
        }

        throw new IllegalArgumentException("Unknown value " + value.getClass().getSimpleName());
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T decodeLastInsertId(long value, Class<?> type) {
        requireNonNull(type, "type must not be null");

        if (Byte.TYPE == type || Byte.class == type) {
            return (T) Byte.valueOf((byte) value);
        } else if (Short.TYPE == type || Short.class == type) {
            return (T) Short.valueOf((short) value);
        } else if (Integer.TYPE == type || Integer.class == type) {
            return (T) Integer.valueOf((int) value);
        } else if (Long.TYPE == type || Long.class == type) {
            return (T) Long.valueOf(value);
        } else if (BigInteger.class == type) {
            if (value < 0) {
                return (T) CodecUtils.unsignedBigInteger(value);
            }

            return (T) BigInteger.valueOf(value);
        } else if (type.isAssignableFrom(Number.class)) {
            if (value < 0) {
                return (T) CodecUtils.unsignedBigInteger(value);
            }

            return (T) Long.valueOf(value);
        }

        throw new IllegalArgumentException(String.format("Cannot decode %s with last inserted ID %s", type,
            value < 0 ? Long.toUnsignedString(value) : value));
    }

    @Override
    public MySqlParameter encode(Object value, CodecContext context) {
        requireNonNull(value, "value must not be null");
        requireNonNull(context, "context must not be null");

        final Object valueToEncode = getValueToEncode(value);
        if (null == valueToEncode) {
            return encodeNull();
        }

        for (Codec<?> codec : codecs) {
            if (codec.canEncode(valueToEncode)) {
                return codec.encode(valueToEncode, context);
            }
        }

        throw new IllegalArgumentException("Cannot encode " + valueToEncode.getClass());
    }

    @Nullable
    private static Object getValueToEncode(Object value) {
        if (value instanceof Parameter) {
            return ((Parameter) value).getValue();
        }
        return value;
    }

    @Override
    public MySqlParameter encodeNull() {
        return NullMySqlParameter.INSTANCE;
    }

    @Nullable
    private <T> T decodePrimitive(FieldValue value, MySqlReadableMetadata metadata, Class<?> type,
        boolean binary, CodecContext context) {
        @SuppressWarnings("unchecked")
        PrimitiveCodec<T> codec = (PrimitiveCodec<T>) this.primitiveCodecs.get(type);

        if (codec != null && value instanceof NormalFieldValue && codec.canPrimitiveDecode(metadata)) {
            return codec.decode(((NormalFieldValue) value).getBufferSlice(), metadata, type, binary, context);
        }

        // Mismatch, no one else can support this primitive class.
        throw new IllegalArgumentException("Cannot decode " + value.getClass().getSimpleName() + " of " +
            type + " for " + metadata.getType());
    }

    @Nullable
    private <T> T decodeNormal(NormalFieldValue value, MySqlReadableMetadata metadata, Class<?> type,
        boolean binary, CodecContext context) {
        for (Codec<?> codec : codecs) {
            if (codec.canDecode(metadata, type)) {
                @SuppressWarnings("unchecked")
                Codec<T> c = (Codec<T>) codec;
                return c.decode(value.getBufferSlice(), metadata, type, binary, context);
            }
        }

        throw new IllegalArgumentException("Cannot decode " + type + " for " + metadata.getType());
    }

    @Nullable
    private <T> T decodeNormal(NormalFieldValue value, MySqlReadableMetadata metadata, ParameterizedType type,
        boolean binary, CodecContext context) {
        for (ParametrizedCodec<?> codec : parametrizedCodecs) {
            if (codec.canDecode(metadata, type)) {
                @SuppressWarnings("unchecked")
                T result = (T) codec.decode(value.getBufferSlice(), metadata, type, binary, context);
                return result;
            }
        }

        throw new IllegalArgumentException("Cannot decode " + type + " for " + metadata.getType());
    }

    @Nullable
    private <T> T decodeMassive(LargeFieldValue value, MySqlReadableMetadata metadata, Class<?> type,
        boolean binary, CodecContext context) {
        for (MassiveCodec<?> codec : massiveCodecs) {
            if (codec.canDecode(metadata, type)) {
                @SuppressWarnings("unchecked")
                MassiveCodec<T> c = (MassiveCodec<T>) codec;
                return c.decodeMassive(value.getBufferSlices(), metadata, type, binary, context);
            }
        }

        throw new IllegalArgumentException("Cannot decode massive " + type + " for " + metadata.getType());
    }

    @Nullable
    private <T> T decodeMassive(LargeFieldValue value, MySqlReadableMetadata metadata, ParameterizedType type,
        boolean binary, CodecContext context) {
        for (MassiveParametrizedCodec<?> codec : massiveParametrizedCodecs) {
            if (codec.canDecode(metadata, type)) {
                @SuppressWarnings("unchecked")
                T result = (T) codec.decodeMassive(value.getBufferSlices(), metadata, type, binary, context);
                return result;
            }
        }

        throw new IllegalArgumentException("Cannot decode massive  " + type + " for " + metadata.getType());
    }

    private static Class<?> chooseClass(MySqlReadableMetadata metadata, Class<?> type) {
        Class<?> javaType = metadata.getType().getJavaType();
        return type.isAssignableFrom(javaType) ? javaType : type;
    }

    private static Codec<?>[] defaultCodecs() {
        return new Codec<?>[] {
            ByteCodec.INSTANCE,
            ShortCodec.INSTANCE,
            IntegerCodec.INSTANCE,
            LongCodec.INSTANCE,
            BigIntegerCodec.INSTANCE,

            BigDecimalCodec.INSTANCE, // Only all decimals
            FloatCodec.INSTANCE, // Decimal (precision < 7) or float
            DoubleCodec.INSTANCE, // Decimal (precision < 16) or double or float

            BooleanCodec.INSTANCE,
            BitSetCodec.INSTANCE,

            ZonedDateTimeCodec.INSTANCE,
            LocalDateTimeCodec.INSTANCE,
            InstantCodec.INSTANCE,
            OffsetDateTimeCodec.INSTANCE,

            LocalDateCodec.INSTANCE,

            LocalTimeCodec.INSTANCE,
            DurationCodec.INSTANCE,
            OffsetTimeCodec.INSTANCE,

            YearCodec.INSTANCE,

            StringCodec.INSTANCE,

            EnumCodec.INSTANCE,
            SetCodec.INSTANCE,

            ClobCodec.INSTANCE,
            BlobCodec.INSTANCE,

            ByteBufferCodec.INSTANCE,
            ByteArrayCodec.INSTANCE
        };
    }

    static final class Builder implements CodecsBuilder {

        @GuardedBy("lock")
        private final ArrayList<Codec<?>> codecs = new ArrayList<>();

        private final ReentrantLock lock = new ReentrantLock();

        @Override
        public CodecsBuilder addFirst(Codec<?> codec) {
            lock.lock();
            try {
                if (codecs.isEmpty()) {
                    Codec<?>[] defaultCodecs = defaultCodecs();

                    codecs.ensureCapacity(defaultCodecs.length + 1);
                    // Add first.
                    codecs.add(codec);
                    codecs.addAll(InternalArrays.asImmutableList(defaultCodecs));
                } else {
                    codecs.add(0, codec);
                }
            } finally {
                lock.unlock();
            }
            return this;
        }

        @Override
        public CodecsBuilder addLast(Codec<?> codec) {
            lock.lock();
            try {
                if (codecs.isEmpty()) {
                    codecs.addAll(InternalArrays.asImmutableList(defaultCodecs()));
                }
                codecs.add(codec);
            } finally {
                lock.unlock();
            }
            return this;
        }

        @Override
        public Codecs build() {
            lock.lock();
            try {
                try {
                    if (codecs.isEmpty()) {
                        return new DefaultCodecs(defaultCodecs());
                    }
                    return new DefaultCodecs(codecs.toArray(new Codec<?>[0]));
                } finally {
                    codecs.clear();
                    codecs.trimToSize();
                }
            } finally {
                lock.unlock();
            }
        }
    }
}
