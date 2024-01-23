/*
 * Copyright 2024 asyncer.io projects
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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import reactor.core.publisher.FluxSink;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * TODO: add javadoc here
 */
final class ReadCompletionHandler implements CompletionHandler<Integer, ByteBuf> {

    private final AsynchronousFileChannel channel;

    private final ByteBufAllocator allocator;

    private final int bufferSize;

    private final FluxSink<ByteBuf> sink;

    private final AtomicLong position;

    private final AtomicReference<State> state = new AtomicReference<>(State.IDLE);

    ReadCompletionHandler(
        AsynchronousFileChannel channel,
        ByteBufAllocator allocator,
        int bufferSize,
        FluxSink<ByteBuf> sink
    ) {
        this.channel = channel;
        this.allocator = allocator;
        this.bufferSize = bufferSize;
        this.sink = sink;
        this.position = new AtomicLong(0);
    }

    public void request(long ignored) {
        tryRead();
    }

    public void cancel() {
        this.state.getAndSet(State.DISPOSED);

        // According java.nio.channels.AsynchronousChannel "if an I/O operation is outstanding
        // on the channel and the channel's close method is invoked, then the I/O operation
        // fails with the exception AsynchronousCloseException". That should invoke the failed
        // callback below and the current ByteBuf should be released.

        tryCloseChannel();
    }

    private void tryRead() {
        if (this.sink.requestedFromDownstream() > 0 && this.state.compareAndSet(State.IDLE, State.READING)) {
            read();
        }
    }

    private void read() {
        ByteBuf buf = this.allocator.buffer(this.bufferSize);
        ByteBuffer byteBuffer = buf.nioBuffer(buf.writerIndex(), buf.writableBytes());

        this.channel.read(byteBuffer, this.position.get(), buf, this);
    }

    @Override
    public void completed(Integer read, ByteBuf buf) {
        if (State.DISPOSED.equals(this.state.get())) {
            buf.release();
            tryCloseChannel();
            return;
        }

        if (read == -1) {
            buf.release();
            tryCloseChannel();
            this.state.set(State.DISPOSED);
            this.sink.complete();
            return;
        }

        this.position.addAndGet(read);
        buf.writerIndex(read);
        this.sink.next(buf);

        // Stay in READING mode if there is demand
        if (this.sink.requestedFromDownstream() > 0) {
            read();
            return;
        }

        // Release READING mode and then try again in case of concurrent "request"
        if (this.state.compareAndSet(State.READING, State.IDLE)) {
            tryRead();
        }
    }

    @Override
    public void failed(Throwable exc, ByteBuf buf) {
        buf.release();

        tryCloseChannel();
        this.state.set(State.DISPOSED);
        this.sink.error(exc);
    }

    private enum State {
        IDLE, READING, DISPOSED
    }

    void tryCloseChannel() {
        if (channel.isOpen()) {
            try {
                channel.close();
            } catch (IOException ignored) {
            }
        }
    }
}
