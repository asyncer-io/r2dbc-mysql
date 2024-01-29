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

package io.asyncer.r2dbc.mysql.client;

import io.asyncer.r2dbc.mysql.ConnectionContext;
import io.asyncer.r2dbc.mysql.constant.Packets;
import io.asyncer.r2dbc.mysql.internal.util.OperatorUtils;
import io.asyncer.r2dbc.mysql.message.client.ClientMessage;
import io.asyncer.r2dbc.mysql.message.client.PrepareQueryMessage;
import io.asyncer.r2dbc.mysql.message.client.PreparedFetchMessage;
import io.asyncer.r2dbc.mysql.message.client.SslRequest;
import io.asyncer.r2dbc.mysql.message.server.ColumnCountMessage;
import io.asyncer.r2dbc.mysql.message.server.CompleteMessage;
import io.asyncer.r2dbc.mysql.message.server.DecodeContext;
import io.asyncer.r2dbc.mysql.message.server.ErrorMessage;
import io.asyncer.r2dbc.mysql.message.server.PreparedOkMessage;
import io.asyncer.r2dbc.mysql.message.server.ServerMessage;
import io.asyncer.r2dbc.mysql.message.server.ServerMessageDecoder;
import io.asyncer.r2dbc.mysql.message.server.ServerStatusMessage;
import io.asyncer.r2dbc.mysql.message.server.SyntheticMetadataMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;

import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * A codec that encodes and decodes MySQL messages.
 * <ul>
 * <li>Read: {@link ByteBuf} -&gt; framed {@link ByteBuf} -&gt; {@link ServerMessage}</li>
 * <li>Write: {@link ClientMessage} -&gt; framed {@link ByteBuf} with last flush</li>
 * </ul>
 */
final class MessageDuplexCodec extends ByteToMessageDecoder implements ChannelOutboundHandler {

    static final String NAME = "R2dbcMySqlMessageDuplexCodec";

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(MessageDuplexCodec.class);

    private final AtomicInteger sequenceId = new AtomicInteger(0);

    private DecodeContext decodeContext = DecodeContext.login();

    private final ConnectionContext context;

    private final ServerMessageDecoder decoder = new ServerMessageDecoder();

    private int frameLength = -1;

    MessageDuplexCodec(ConnectionContext context) {
        this.context = requireNonNull(context, "context must not be null");
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        ByteBuf frame = decode(in);

        if (frame != null) {
            DecodeContext context = this.decodeContext;
            ServerMessage message = this.decoder.decode(frame, this.context, context);

            if (message != null) {
                handleDecoded(out, message);
            }
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        if (msg instanceof ClientMessage) {
            ByteBufAllocator allocator = ctx.alloc();
            ClientMessage message = (ClientMessage) msg;
            Flux<ByteBuf> encoded = Flux.from(message.encode(allocator, this.context));

            OperatorUtils.envelope(encoded, allocator, sequenceId, message.isCumulative())
                .subscribe(new WriteSubscriber(ctx, promise));

            if (msg instanceof PrepareQueryMessage) {
                setDecodeContext(DecodeContext.prepareQuery());
            } else if (msg instanceof PreparedFetchMessage) {
                setDecodeContext(DecodeContext.fetch());
            } else if (msg instanceof SslRequest) {
                ctx.channel().pipeline().fireUserEventTriggered(SslState.BRIDGING);
            }
        } else {
            if (logger.isWarnEnabled()) {
                logger.warn("Unknown message type {} on writing", msg.getClass());
            }
            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof PacketEvent) {
            switch ((PacketEvent) evt) {
                case RESET_SEQUENCE:
                    logger.trace("Reset sequence id");
                    this.sequenceId.set(0);
                    break;
                case USE_COMPRESSION:
                    logger.trace("Reset sequence id");
                    this.sequenceId.set(0);

                    if (context.getCapability().isZstdCompression()) {
                        enableZstdCompression(ctx);
                    } else if (context.getCapability().isZlibCompression()) {
                        enableZlibCompression(ctx);
                    } else {
                        logger.warn("Unexpected event compression triggered, no capability found");
                    }
                    break;
                default:
                    // Ignore unknown event
                    break;
            }
        }

        ctx.fireUserEventTriggered(evt);
    }

    @Override
    public void flush(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        decoder.dispose();
        ctx.fireChannelInactive();
    }

    @Nullable
    private ByteBuf decode(ByteBuf in) {
        if (frameLength == -1) {
            // New frame
            if (in.readableBytes() < Packets.SIZE_FIELD_SIZE) {
                return null;
            }

            frameLength = in.getUnsignedMediumLE(in.readerIndex()) + Packets.NORMAL_HEADER_SIZE;
        }

        if (in.readableBytes() < frameLength) {
            return null;
        }

        in.skipBytes(Packets.SIZE_FIELD_SIZE);

        int sequenceId = in.readUnsignedByte();
        ByteBuf frame = in.readRetainedSlice(frameLength - Packets.NORMAL_HEADER_SIZE);

        logger.trace("Decoded frame with sequence id: {}, total size: {}", sequenceId, frameLength);
        this.sequenceId.set(sequenceId + 1);
        this.frameLength = -1;

        return frame;
    }

    private void handleDecoded(List<Object> out, ServerMessage msg) {
        if (msg instanceof ServerStatusMessage) {
            this.context.setServerStatuses(((ServerStatusMessage) msg).getServerStatuses());
        }

        if (msg instanceof CompleteMessage) {
            // Metadata EOF message will be not receive in here.
            setDecodeContext(DecodeContext.command());
        } else if (msg instanceof SyntheticMetadataMessage) {
            if (((SyntheticMetadataMessage) msg).isCompleted()) {
                setDecodeContext(DecodeContext.command());
            }
        } else if (msg instanceof ColumnCountMessage) {
            setDecodeContext(DecodeContext.result(this.context.getCapability().isEofDeprecated(),
                ((ColumnCountMessage) msg).getTotalColumns()));
            return; // Done, no need use generic handle.
        } else if (msg instanceof PreparedOkMessage) {
            PreparedOkMessage message = (PreparedOkMessage) msg;
            int columns = message.getTotalColumns();
            int parameters = message.getTotalParameters();

            // For supports use server-preparing query for simple statements. The count of columns and
            // parameters may all be 0. All is 0 means no EOF message following.
            // columns + parameters > 0
            if (columns > -parameters) {
                setDecodeContext(DecodeContext.preparedMetadata(this.context.getCapability()
                    .isEofDeprecated(), columns, parameters));
            } else {
                setDecodeContext(DecodeContext.command());
            }
        } else if (msg instanceof ErrorMessage) {
            setDecodeContext(DecodeContext.command());
        }

        // Generic handle.
        out.add(msg);
    }

    private void setDecodeContext(DecodeContext context) {
        this.decodeContext = context;
        if (logger.isDebugEnabled()) {
            logger.debug("Decode context change to {}", context);
        }
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

    private static void enableZstdCompression(ChannelHandlerContext ctx) {
        CompressionDuplexCodec handler = new CompressionDuplexCodec(
            new ZstdCompressor(3));

        if (ctx.pipeline().get(CompressionDuplexCodec.NAME) != null) {
            logger.warn("Unexpected event, compression already enabled");
        } else {
            logger.debug("Compression zstd enabled for subsequent packets");
            ctx.pipeline().addBefore(NAME, CompressionDuplexCodec.NAME, handler);
        }
    }

    private static void enableZlibCompression(ChannelHandlerContext ctx) {
        CompressionDuplexCodec handler = new CompressionDuplexCodec(new ZlibCompressor());

        if (ctx.pipeline().get(CompressionDuplexCodec.NAME) != null) {
            logger.warn("Unexpected event, compression already enabled");
        } else {
            logger.debug("Compression zlib enabled for subsequent packets");
            ctx.pipeline().addBefore(NAME, CompressionDuplexCodec.NAME, handler);
        }
    }
}
