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
import io.asyncer.r2dbc.mysql.internal.util.OperatorUtils;
import io.asyncer.r2dbc.mysql.message.client.ClientMessage;
import io.asyncer.r2dbc.mysql.message.client.SubsequenceClientMessage;
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
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import reactor.core.publisher.Flux;

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * Client/server messages encode/decode logic.
 */
final class MessageDuplexCodec extends ChannelDuplexHandler {

    static final String NAME = "R2dbcMySqlMessageDuplexCodec";

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(MessageDuplexCodec.class);

    private DecodeContext decodeContext = DecodeContext.login();

    private final ConnectionContext context;

    private final ServerMessageDecoder decoder = new ServerMessageDecoder();

    MessageDuplexCodec(ConnectionContext context) {
        this.context = requireNonNull(context, "context must not be null");
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof ByteBuf) {
            DecodeContext context = this.decodeContext;
            ServerMessage message = this.decoder.decode((ByteBuf) msg, this.context, context);

            if (message != null) {
                handleDecoded(ctx, message);
            }
        } else if (msg instanceof ServerMessage) {
            ctx.fireChannelRead(msg);
        } else {
            if (logger.isWarnEnabled()) {
                logger.warn("Unknown message type {} on reading", msg.getClass());
            }
            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        if (msg instanceof ClientMessage) {
            ByteBufAllocator allocator = ctx.alloc();
            Flux<ByteBuf> encoded;

            if (msg instanceof SubsequenceClientMessage) {
                SubsequenceClientMessage message = (SubsequenceClientMessage) msg;

                encoded = Flux.from(message.encode(allocator, this.context));
                int envelopeId = message.getEnvelopeId();

                OperatorUtils.envelope(encoded, allocator, envelopeId, false)
                    .subscribe(new WriteSubscriber(ctx, promise));
            } else {
                encoded = Flux.from(((ClientMessage) msg).encode(allocator, this.context));

                OperatorUtils.envelope(encoded, allocator, 0, true)
                    .subscribe(new WriteSubscriber(ctx, promise));
            }

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
    public void channelInactive(ChannelHandlerContext ctx) {
        decoder.dispose();
        ctx.fireChannelInactive();
    }

    private void handleDecoded(ChannelHandlerContext ctx, ServerMessage msg) {
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
        ctx.fireChannelRead(msg);
    }

    private void setDecodeContext(DecodeContext context) {
        this.decodeContext = context;
        if (logger.isDebugEnabled()) {
            logger.debug("Decode context change to {}", context);
        }
    }
}
