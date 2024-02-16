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

package io.asyncer.r2dbc.mysql.client;

import io.asyncer.r2dbc.mysql.constant.Packets;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import org.jetbrains.annotations.Nullable;

import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A codec that compresses and decompresses packets.
 * <ul>
 * <li>Read: compression {@link ByteBuf} -&gt; compression-framed {@link ByteBuf} -&gt;
 * decompressed {@link ByteBuf}</li>
 * <li>Write: uncompressed-framed {@link ByteBuf} -&gt; compression-framed {@link ByteBuf}</li>
 * </ul>
 */
final class CompressionDuplexCodec extends ByteToMessageDecoder implements ChannelOutboundHandler {

    static final String NAME = "R2dbcMysqlCompressionDuplexCodec";

    private static final InternalLogger logger =
        InternalLoggerFactory.getInstance(CompressionDuplexCodec.class);

    private static final int MIN_COMPRESS_LENGTH = 50;

    /**
     * Compression packet sequence id, incremented independently of the normal sequence id.
     */
    private final AtomicInteger sequenceId = new AtomicInteger(0);

    private final Compressor compressor;

    @Nullable
    private ByteBuf writeCumulated;

    private final Cumulator writeCumulator = MERGE_CUMULATOR;

    private int frameLength = -1;

    CompressionDuplexCodec(Compressor compressor) {
        this.compressor = compressor;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        if (msg instanceof ByteBuf) {
            ByteBuf cumulated = this.writeCumulated == null ? ctx.alloc().buffer(0, 0) :
                this.writeCumulated;

            this.writeCumulated = cumulated = writeCumulator.cumulate(ctx.alloc(), cumulated, (ByteBuf) msg);

            while (cumulated.readableBytes() >= Packets.MAX_PAYLOAD_SIZE) {
                logger.trace("Accumulated to the maximum payload, compressing");

                ByteBuf slice = cumulated.readSlice(Packets.MAX_PAYLOAD_SIZE);
                ByteBuf compressed = compressor.compress(slice);

                if (compressed.readableBytes() >= slice.readableBytes()) {
                    logger.trace("Sending uncompressed due to compressed payload is larger than original");
                    compressed.release();
                    ctx.write(buildHeader(ctx, slice.readableBytes(), 0));
                    ctx.write(slice.retain());
                } else {
                    logger.trace("Sending compressed payload");
                    ctx.write(buildHeader(ctx, compressed.readableBytes(), Packets.MAX_PAYLOAD_SIZE));
                    ctx.write(compressed);
                }
            }

            if (!cumulated.isReadable()) {
                this.writeCumulated = null;
                cumulated.release();
            } else {
                logger.trace("Accumulated writing buffers, waiting for flush");
            }
        } else {
            ctx.write(msg, promise);
        }
    }

    private ByteBuf buildHeader(ChannelHandlerContext ctx, int compressedSize, int uncompressedSize) {
        return ctx.alloc().ioBuffer(Packets.COMPRESS_HEADER_SIZE)
            .writeMediumLE(compressedSize)
            .writeByte(sequenceId.getAndIncrement())
            .writeMediumLE(uncompressedSize);
    }

    @Override
    public void flush(ChannelHandlerContext ctx) {
        ByteBuf cumulated = this.writeCumulated;

        this.writeCumulated = null;

        if (cumulated == null) {
            ctx.flush();
            return;
        }

        int uncompressedSize = cumulated.readableBytes();

        if (uncompressedSize < MIN_COMPRESS_LENGTH) {
            logger.trace("flushing, payload is too small to compress, sending uncompressed");
            ctx.write(buildHeader(ctx, uncompressedSize, 0));
            ctx.writeAndFlush(cumulated);
        } else {
            try {
                logger.trace("flushing, compressing payload");

                ByteBuf compressed = compressor.compress(cumulated);

                if (compressed.readableBytes() >= uncompressedSize) {
                    logger.trace("Sending uncompressed due to compressed payload is larger than original");
                    compressed.release();
                    ctx.write(buildHeader(ctx, uncompressedSize, 0));
                    ctx.writeAndFlush(cumulated.retain());
                } else {
                    logger.trace("Sending compressed payload");
                    ctx.write(buildHeader(ctx, compressed.readableBytes(), uncompressedSize));
                    ctx.writeAndFlush(compressed);
                }
            } finally {
                cumulated.release();
            }
        }
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        ByteBuf frame = decode(in);

        if (frame != null) {
            out.add(frame);
        }
    }

    @Nullable
    private ByteBuf decode(ByteBuf in) {
        if (frameLength == -1) {
            // New frame
            if (in.readableBytes() < Packets.SIZE_FIELD_SIZE) {
                return null;
            }

            frameLength = in.getUnsignedMediumLE(in.readerIndex()) + Packets.COMPRESS_HEADER_SIZE;
        }

        if (in.readableBytes() < frameLength) {
            return null;
        }

        in.skipBytes(Packets.SIZE_FIELD_SIZE);

        int sequenceId = in.readUnsignedByte();
        int uncompressedSize = in.readUnsignedMediumLE();
        ByteBuf frame = in.readRetainedSlice(frameLength - Packets.COMPRESS_HEADER_SIZE);

        logger.trace("Decoded frame with sequence id: {}, total size: {}, uncompressed size: {}",
            sequenceId, frameLength, uncompressedSize);
        this.frameLength = -1;
        this.sequenceId.set(sequenceId + 1);

        if (uncompressedSize == 0) {
            return frame;
        } else {
            try {
                return compressor.decompress(frame, uncompressedSize);
            } finally {
                frame.release();
            }
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (PacketEvent.RESET_SEQUENCE == evt) {
            logger.debug("Reset sequence id");
            this.sequenceId.set(0);
        }

        ctx.fireUserEventTriggered(evt);
    }

    @Override
    public void bind(ChannelHandlerContext ctx, SocketAddress localAddress,
        ChannelPromise promise) {
        ctx.bind(localAddress, promise);
    }

    @Override
    public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress,
        ChannelPromise promise) {
        ctx.connect(remoteAddress, localAddress, promise);
    }

    @Override
    public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) {
        ctx.disconnect(promise);
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) {
        ctx.close(promise);
    }

    @Override
    public void deregister(ChannelHandlerContext ctx, ChannelPromise promise) {
        ctx.deregister(promise);
    }

    @Override
    public void read(ChannelHandlerContext ctx) {
        ctx.read();
    }

    @Override
    protected void handlerRemoved0(ChannelHandlerContext ctx) {
        this.compressor.dispose();
    }
}
