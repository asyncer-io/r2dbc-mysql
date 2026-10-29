/*
 * Copyright 2026 asyncer.io projects
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

import io.asyncer.r2dbc.mysql.ConnectionContextTest;
import io.asyncer.r2dbc.mysql.MySqlConnectionConfigurationTest;
import io.asyncer.r2dbc.mysql.constant.SslMode;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.handler.ssl.AbstractSniHandler;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class SslBridgeHandlerTest {

    private static final String HOST = "mysql.example.com";

    private static final int PORT = 3306;

    @ParameterizedTest
    @EnumSource(value = SslMode.class,
        names = { "PREFERRED", "REQUIRED", "VERIFY_CA", "VERIFY_IDENTITY", "TUNNEL" })
    void disablesNettyEndpointIdentification(SslMode mode) throws Exception {
        assertThat(SslBridgeHandler.MySqlSslContextSpec
            .forClient(MySqlConnectionConfigurationTest.newSsl(mode), ConnectionContextTest.mock())
            .sslContext().newEngine(ByteBufAllocator.DEFAULT).getSSLParameters()
            .getEndpointIdentificationAlgorithm()).isNull();
    }

    @ParameterizedTest
    @EnumSource(value = SslMode.class,
        names = { "PREFERRED", "REQUIRED", "VERIFY_CA", "VERIFY_IDENTITY", "TUNNEL" })
    void sendsSniForInetSocketAddress(SslMode mode) {
        EmbeddedChannel channel = new EmbeddedChannel();
        SslHandler sslHandler = bridge(channel, mode, InetSocketAddress.createUnresolved(HOST, PORT));

        try {
            assertThat(sslHandler.engine().getPeerHost()).isEqualTo(HOST);
            assertThat(sslHandler.engine().getPeerPort()).isEqualTo(PORT);
            assertThat(sniOf(readOutbound(channel))).isEqualTo(HOST);
        } finally {
            finish(channel);
        }
    }

    @Test
    void omitsSniForNonInetSocketAddress() {
        EmbeddedChannel channel = new EmbeddedChannel();
        SslHandler sslHandler = bridge(channel, SslMode.REQUIRED, new DomainSocketAddress("/tmp/mysql.sock"));

        try {
            assertThat(sslHandler.engine().getPeerHost()).isNull();
            assertThat(sniOf(readOutbound(channel))).isNull();
        } finally {
            finish(channel);
        }
    }

    private static SslHandler bridge(EmbeddedChannel channel, SslMode mode, SocketAddress address) {
        channel.pipeline().addLast(SslBridgeHandler.NAME, new SslBridgeHandler(ConnectionContextTest.mock(),
            MySqlConnectionConfigurationTest.newSsl(mode), address));

        if (mode != SslMode.TUNNEL) {
            // TUNNEL bridges on handlerAdded, other modes bridge after the server accepts SSL.
            channel.pipeline().fireUserEventTriggered(SslState.BRIDGING);
        }

        SslHandler sslHandler = channel.pipeline().get(SslHandler.class);

        assertThat(sslHandler).isNotNull();

        return sslHandler;
    }

    private static void finish(EmbeddedChannel channel) {
        // Closing the channel fails the pending handshake, which the bridge would turn into an exception.
        channel.pipeline().remove(SslBridgeHandler.NAME);
        channel.finishAndReleaseAll();
    }

    /**
     * Reads the ClientHello, which is written as soon as the {@link SslHandler} is added to an active channel.
     */
    private static ByteBuf readOutbound(EmbeddedChannel channel) {
        ByteBuf outbound = Unpooled.buffer();

        for (ByteBuf buf = channel.readOutbound(); buf != null; buf = channel.readOutbound()) {
            outbound.writeBytes(buf);
            buf.release();
        }

        return outbound;
    }

    /**
     * Extracts the server_name extension from a ClientHello with Netty's own TLS parser, {@code null} if the
     * ClientHello does not carry one. Takes ownership of {@code clientHello}.
     */
    private static String sniOf(ByteBuf clientHello) {
        AtomicBoolean parsed = new AtomicBoolean();
        AtomicReference<String> sni = new AtomicReference<>();
        EmbeddedChannel server = new EmbeddedChannel(new AbstractSniHandler<Object>() {

            @Override
            protected Future<Object> lookup(ChannelHandlerContext ctx, String hostname) {
                parsed.set(true);
                sni.set(hostname);
                return ctx.executor().newSucceededFuture(null);
            }

            @Override
            protected void onLookupComplete(ChannelHandlerContext ctx, String hostname, Future<Object> future) {
            }
        });

        try {
            server.writeInbound(clientHello);

            assertThat(parsed).as("ClientHello parsed").isTrue();

            return sni.get();
        } finally {
            server.finishAndReleaseAll();
        }
    }
}
