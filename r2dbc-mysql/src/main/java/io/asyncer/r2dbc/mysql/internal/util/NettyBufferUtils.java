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

package io.asyncer.r2dbc.mysql.internal.util;

import io.asyncer.r2dbc.mysql.message.FieldValue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import reactor.core.publisher.Flux;

import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.require;
import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * An internal utility considers the use of safe release buffers (array or {@link List}). It uses standard
 * netty {@link ReferenceCountUtil#safeRelease} to suppress release errors.
 */
public final class NettyBufferUtils {

    /**
     * Reads all bytes from a file asynchronously.
     *
     * @param path       The path of the file want to be read.
     * @param allocator  The {@link ByteBufAllocator} used to allocate {@link ByteBuf}s.
     * @param bufferSize The size of the buffer used to read the file.
     * @return A {@link Flux} emits {@link ByteBuf}s read from the file.
     */
    public static Flux<ByteBuf> readFile(Path path, ByteBufAllocator allocator, int bufferSize) {
        requireNonNull(path, "path must not be null");
        requireNonNull(allocator, "allocator must not be null");
        require(bufferSize > 0, "bufferSize must be positive");

        return Flux.<ByteBuf>create(sink -> {
            ReadCompletionHandler handler;

            try {
                // AsynchronousFileChannel can only be opened in blocking mode :(
                @SuppressWarnings("BlockingMethodInNonBlockingContext")
                AsynchronousFileChannel channel = AsynchronousFileChannel.open(path, StandardOpenOption.READ);

                handler = new ReadCompletionHandler(channel, allocator, bufferSize, sink);
            } catch (Throwable e) {
                sink.error(e);
                return;
            }

            sink.onCancel(handler::cancel);
            sink.onRequest(handler::request);
        }).doOnDiscard(ByteBuf.class, ReferenceCountUtil::safeRelease);
    }

    /**
     * Combine {@link ByteBuf}s through composite buffer.
     * <p>
     * This method would release all {@link ByteBuf}s when any exception throws.
     *
     * @param parts The {@link ByteBuf}s want to be wrap, it can not be empty, and it will be cleared.
     * @return A {@link ByteBuf} holds the all bytes of given {@code parts}, it may be a read-only buffer.
     */
    public static ByteBuf composite(final List<ByteBuf> parts) {
        final int size = parts.size();

        switch (size) {
        case 0:
            throw new IllegalStateException("No buffer available");
        case 1:
            try {
                return parts.get(0);
            } finally {
                parts.clear();
            }
        default:
            CompositeByteBuf composite = null;

            try {
                composite = parts.get(0).alloc().compositeBuffer(size);
                // Auto-releasing failed parts
                return composite.addComponents(true, parts);
            } catch (Throwable e) {
                if (composite == null) {
                    // Alloc failed, release parts.
                    releaseAll(parts);
                } else {
                    // Also release success parts.
                    composite.release();
                }
                throw e;
            } finally {
                parts.clear();
            }
        }
    }

    public static void releaseAll(ReferenceCounted[] parts) {
        for (ReferenceCounted counted : parts) {
            if (counted != null) {
                ReferenceCountUtil.safeRelease(counted);
            }
        }
    }

    public static void releaseAll(FieldValue[] fields) {
        for (FieldValue field : fields) {
            if (field != null && !field.isNull()) {
                ReferenceCountUtil.safeRelease(field);
            }
        }
    }

    public static void releaseAll(List<? extends ReferenceCounted> parts) {
        release0(parts, parts.size());
    }

    public static void releaseAll(FieldValue[] fields, int bound) {
        int size = Math.min(bound, fields.length);

        for (int i = 0; i < size; ++i) {
            FieldValue field = fields[i];
            if (field != null && !field.isNull()) {
                ReferenceCountUtil.safeRelease(field);
            }
        }
    }

    public static void releaseAll(List<? extends ReferenceCounted> parts, int bound) {
        release0(parts, Math.min(bound, parts.size()));
    }

    private static void release0(List<? extends ReferenceCounted> parts, int size) {
        for (int i = 0; i < size; ++i) {
            ReferenceCounted counted = parts.get(i);
            if (counted != null) {
                ReferenceCountUtil.safeRelease(counted);
            }
        }
    }

    private NettyBufferUtils() { }
}
