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

package io.asyncer.r2dbc.mysql;

import io.asyncer.r2dbc.mysql.api.MySqlResult;
import io.asyncer.r2dbc.mysql.api.MySqlRow;
import io.asyncer.r2dbc.mysql.client.Client;
import io.asyncer.r2dbc.mysql.codec.Codecs;
import io.asyncer.r2dbc.mysql.internal.util.NettyBufferUtils;
import io.asyncer.r2dbc.mysql.internal.util.OperatorUtils;
import io.asyncer.r2dbc.mysql.message.FieldValue;
import io.asyncer.r2dbc.mysql.message.server.DefinitionMetadataMessage;
import io.asyncer.r2dbc.mysql.message.server.ErrorMessage;
import io.asyncer.r2dbc.mysql.message.server.OkMessage;
import io.asyncer.r2dbc.mysql.message.server.RowMessage;
import io.asyncer.r2dbc.mysql.message.server.ServerMessage;
import io.asyncer.r2dbc.mysql.message.server.SyntheticMetadataMessage;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.Readable;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.jetbrains.annotations.Nullable;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link MySqlResult} representing the results of a query against the MySQL database.
 * <p>
 * A {@link Segment} provided by this implementation may be both {@link UpdateCount} and {@link RowSegment}, see also
 * {@link MySqlOkSegment}.
 */
final class MySqlSegmentResult implements MySqlResult {

    private final Flux<Segment> segments;

    private MySqlSegmentResult(Flux<Segment> segments) {
        this.segments = segments;
    }

    @Override
    public Mono<Long> getRowsUpdated() {
        return segments.<Long>handle((segment, sink) -> {
            if (segment instanceof UpdateCount) {
                sink.next(((UpdateCount) segment).value());
            } else if (segment instanceof Message) {
                sink.error(((Message) segment).exception());
            } else if (segment instanceof ReferenceCounted) {
                ReferenceCountUtil.safeRelease(segment);
            }
        }).reduce(Long::sum);
    }

    @Override
    public <T> Flux<T> map(BiFunction<Row, RowMetadata, ? extends T> f) {
        requireNonNull(f, "mapping function must not be null");

        return segments.handle((segment, sink) -> {
            if (segment instanceof RowSegment) {
                MySqlRow row = ((RowSegment) segment).row();

                try {
                    sink.next(f.apply(row, row.getMetadata()));
                } finally {
                    ReferenceCountUtil.safeRelease(segment);
                }
            } else if (segment instanceof Message) {
                sink.error(((Message) segment).exception());
            } else if (segment instanceof ReferenceCounted) {
                ReferenceCountUtil.safeRelease(segment);
            }
        });
    }

    @Override
    public <T> Flux<T> map(Function<? super Readable, ? extends T> f) {
        requireNonNull(f, "mapping function must not be null");

        return segments.handle((segment, sink) -> {
            if (segment instanceof RowSegment) {
                try {
                    sink.next(f.apply(((RowSegment) segment).row()));
                } finally {
                    ReferenceCountUtil.safeRelease(segment);
                }
            } else if (segment instanceof Message) {
                sink.error(((Message) segment).exception());
            } else if (segment instanceof ReferenceCounted) {
                ReferenceCountUtil.safeRelease(segment);
            }
        });
    }

    @Override
    public MySqlResult filter(Predicate<Result.Segment> filter) {
        requireNonNull(filter, "filter must not be null");

        return new MySqlSegmentResult(segments.filter(segment -> {
            if (filter.test(segment)) {
                return true;
            }

            if (segment instanceof ReferenceCounted) {
                ReferenceCountUtil.safeRelease(segment);
            }

            return false;
        }));
    }

    @Override
    public <T> Flux<T> flatMap(Function<Result.Segment, ? extends Publisher<? extends T>> f) {
        requireNonNull(f, "mapping function must not be null");

        return segments.flatMap(segment -> {
            Publisher<? extends T> ret = f.apply(segment);

            if (ret == null) {
                return Mono.error(new IllegalStateException("The mapper returned a null Publisher"));
            }

            // doAfterTerminate to not release resources before they had a chance to get emitted.
            if (ret instanceof Mono) {
                @SuppressWarnings("unchecked")
                Mono<T> mono = (Mono<T>) ret;
                return mono.doAfterTerminate(() -> ReferenceCountUtil.release(segment));
            }

            return Flux.from(ret).doAfterTerminate(() -> ReferenceCountUtil.release(segment));
        });
    }

    static MySqlResult toResult(boolean binary, Client client, Codecs codecs,
        @Nullable String syntheticKeyName, Flux<ServerMessage> messages) {
        requireNonNull(client, "client must not be null");
        requireNonNull(codecs, "codecs must not be null");
        requireNonNull(messages, "messages must not be null");

        return new MySqlSegmentResult(OperatorUtils.discardOnCancel(messages)
            .doOnDiscard(ReferenceCounted.class, ReferenceCounted::release)
            .handle(new MySqlSegments(binary, client, codecs, syntheticKeyName)));
    }

    private static final class MySqlMessage implements Message {

        private final ErrorMessage message;

        private MySqlMessage(ErrorMessage message) {
            this.message = message;
        }

        @Override
        public R2dbcException exception() {
            return message.toException();
        }

        @Override
        public int errorCode() {
            return message.getCode();
        }

        @Override
        public String sqlState() {
            return message.getSqlState();
        }

        @Override
        public String message() {
            return message.getMessage();
        }
    }

    private static final class MySqlRowSegment extends AbstractReferenceCounted implements RowSegment {

        private final MySqlRow row;

        private final FieldValue[] fields;

        private MySqlRowSegment(FieldValue[] fields, MySqlRowDescriptor metadata, Codecs codecs, boolean binary,
            ConnectionContext context) {
            this.row = new MySqlDataRow(fields, metadata, codecs, binary, context);
            this.fields = fields;
        }

        @Override
        public MySqlRow row() {
            return row;
        }

        @Override
        public ReferenceCounted touch(Object hint) {
            for (FieldValue field : this.fields) {
                field.touch(hint);
            }

            return this;
        }

        @Override
        protected void deallocate() {
            NettyBufferUtils.releaseAll(fields);
        }
    }

    @SuppressWarnings("checkstyle:FinalClass")
    private static class MySqlUpdateCount implements UpdateCount {

        private final long rows;

        private MySqlUpdateCount(long rows) {
            this.rows = rows;
        }

        @Override
        public long value() {
            return rows;
        }
    }

    private static final class MySqlOkSegment extends MySqlUpdateCount implements RowSegment {

        private final long lastInsertId;

        private final Codecs codecs;

        private final String keyName;

        private MySqlOkSegment(long rows, long lastInsertId, Codecs codecs, String keyName) {
            super(rows);

            this.lastInsertId = lastInsertId;
            this.codecs = codecs;
            this.keyName = keyName;
        }

        @Override
        public MySqlRow row() {
            return new InsertSyntheticRow(codecs, keyName, lastInsertId);
        }
    }

    private static final class MySqlSegments implements BiConsumer<ServerMessage, SynchronousSink<Segment>> {

        private final boolean binary;

        private final Client client;

        private final Codecs codecs;

        @Nullable
        private final String syntheticKeyName;

        private final AtomicLong rowCount = new AtomicLong(0);

        private MySqlRowDescriptor rowMetadata;

        private MySqlSegments(boolean binary, Client client, Codecs codecs, @Nullable String syntheticKeyName) {
            this.binary = binary;
            this.client = client;
            this.codecs = codecs;
            this.syntheticKeyName = syntheticKeyName;
        }

        @Override
        public void accept(ServerMessage message, SynchronousSink<Segment> sink) {
            if (message instanceof RowMessage) {
                // Updated rows can be identified either by OK or rows in case of RETURNING
                rowCount.getAndIncrement();

                MySqlRowDescriptor metadata = this.rowMetadata;

                if (metadata == null) {
                    ReferenceCountUtil.safeRelease(message);
                    sink.error(new IllegalStateException("No metadata available"));
                    return;
                }

                FieldValue[] fields;

                try {
                    fields = ((RowMessage) message).decode(binary, metadata.unwrap());
                } finally {
                    ReferenceCountUtil.safeRelease(message);
                }

                sink.next(new MySqlRowSegment(fields, metadata, codecs, binary, client.getContext()));
            } else if (message instanceof SyntheticMetadataMessage) {
                DefinitionMetadataMessage[] metadataMessages = ((SyntheticMetadataMessage) message).unwrap();

                if (metadataMessages.length == 0) {
                    return;
                }

                this.rowMetadata = MySqlRowDescriptor.create(metadataMessages);
            } else if (message instanceof OkMessage) {
                OkMessage msg = (OkMessage) message;

                if (MySqlStatementSupport.supportReturning(client.getContext()) && msg.isEndOfRows()) {
                    sink.next(new MySqlUpdateCount(rowCount.getAndSet(0)));
                } else {
                    long rows = msg.getAffectedRows();
                    Segment segment = syntheticKeyName == null ? new MySqlUpdateCount(rows) :
                        new MySqlOkSegment(rows, msg.getLastInsertId(), codecs, syntheticKeyName);

                    sink.next(segment);
                }
            } else if (message instanceof ErrorMessage) {
                sink.next(new MySqlMessage((ErrorMessage) message));
            } else {
                ReferenceCountUtil.safeRelease(message);
            }
        }
    }
}
