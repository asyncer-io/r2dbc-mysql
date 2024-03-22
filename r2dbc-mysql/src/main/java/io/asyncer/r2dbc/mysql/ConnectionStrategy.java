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

package io.asyncer.r2dbc.mysql;

import io.asyncer.r2dbc.mysql.client.Client;
import io.asyncer.r2dbc.mysql.client.ReactorNettyClient;
import io.asyncer.r2dbc.mysql.constant.CompressionAlgorithm;
import io.asyncer.r2dbc.mysql.constant.SslMode;
import io.netty.channel.ChannelOption;
import io.netty.resolver.AddressResolver;
import io.netty.resolver.AddressResolverGroup;
import io.netty.resolver.DefaultNameResolver;
import io.netty.resolver.RoundRobinInetAddressResolver;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpClient;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * An interface of a connection strategy that considers how to obtain a MySQL {@link Client} object.
 *
 * @since 1.2.0
 */
@FunctionalInterface
interface ConnectionStrategy {

    InternalLogger logger = InternalLoggerFactory.getInstance(ConnectionStrategy.class);

    /**
     * Establish a connection to a target server that is determined by this connection strategy.
     *
     * @return a logged-in {@link Client} object.
     */
    Mono<? extends Client> connect();

    /**
     * Creates a general-purpose {@link TcpClient} with the given {@link SocketClientConfiguration}.
     * <p>
     * Note: Unix Domain Socket also uses this method to create a general-purpose {@link TcpClient client}.
     *
     * @param configuration socket client configuration.
     * @return a general-purpose {@link TcpClient client}.
     */
    static TcpClient createTcpClient(SocketClientConfiguration configuration, boolean balancedDns) {
        LoopResources loopResources = configuration.getLoopResources();
        Duration connectTimeout = configuration.getConnectTimeout();
        TcpClient client = TcpClient.newConnection();

        if (loopResources != null) {
            client = client.runOn(loopResources);
        }

        if (connectTimeout != null) {
            client = client.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, Math.toIntExact(connectTimeout.toMillis()));
        }

        if (balancedDns) {
            client = client.resolver(BalancedResolverGroup.INSTANCE);
        }

        return client;
    }

    /**
     * Logins to a MySQL server with the given {@link TcpClient}, {@link Credential} and configurations.
     *
     * @param tcpClient     a TCP client to connect to a MySQL server.
     * @param credential    user and password to log in to a MySQL server.
     * @param configuration a configuration that affects login behavior.
     * @return a logged-in {@link Client} object.
     */
    static Mono<ReactorNettyClient> login(
        TcpClient tcpClient,
        Credential credential,
        MySqlConnectionConfiguration configuration
    ) {
        MySqlSslConfiguration ssl = configuration.getSsl();
        SslMode sslMode = ssl.getSslMode();
        boolean createDbIfNotExist = configuration.isCreateDatabaseIfNotExist();
        String database = configuration.getDatabase();
        String loginDb = createDbIfNotExist ? "" : database;
        Set<CompressionAlgorithm> compressionAlgorithms = configuration.getCompressionAlgorithms();
        int zstdLevel = configuration.getZstdCompressionLevel();
        ConnectionContext context = new ConnectionContext(
            configuration.getZeroDateOption(),
            configuration.getLoadLocalInfilePath(),
            configuration.getLocalInfileBufferSize(),
            configuration.isPreserveInstants(),
            configuration.retrieveConnectionZoneId()
        );

        return ReactorNettyClient.connect(tcpClient, ssl, context).flatMap(client ->
            QueryFlow.login(client, sslMode, loginDb, credential, compressionAlgorithms, zstdLevel));
    }

    /**
     * Creates an exception that indicates a retry failure.
     *
     * @param message the message of the exception.
     * @param cause   the last exception that caused the retry.
     * @return a retry failure exception.
     */
    static R2dbcNonTransientResourceException retryFail(String message, @Nullable Throwable cause) {
        return new R2dbcNonTransientResourceException(
            message,
            "H1000",
            9000,
            cause
        );
    }

    /**
     * Connect and login to a MySQL server with a specific TCP socket address.
     *
     * @since 1.2.0
     */
    final class InetConnectFunction implements Function<Supplier<InetSocketAddress>, Mono<ReactorNettyClient>> {

        private final boolean balancedDns;

        private final boolean tcpKeepAlive;

        private final boolean tcpNoDelay;

        private final Credential credential;

        private final MySqlConnectionConfiguration configuration;

        InetConnectFunction(
            boolean balancedDns,
            boolean tcpKeepAlive,
            boolean tcpNoDelay,
            Credential credential,
            MySqlConnectionConfiguration configuration
        ) {
            this.balancedDns = balancedDns;
            this.tcpKeepAlive = tcpKeepAlive;
            this.tcpNoDelay = tcpNoDelay;
            this.credential = credential;
            this.configuration = configuration;
        }

        @Override
        public Mono<ReactorNettyClient> apply(Supplier<InetSocketAddress> address) {
            TcpClient cc = ConnectionStrategy.createTcpClient(configuration.getClient(), balancedDns)
                .option(ChannelOption.SO_KEEPALIVE, tcpKeepAlive)
                .option(ChannelOption.TCP_NODELAY, tcpNoDelay)
                .remoteAddress(address);

            return ConnectionStrategy.login(cc, credential, configuration);
        }
    }

    /**
     * Resolves the {@link InetSocketAddress} to IP address, randomly pick one if it resolves to multiple IP addresses.
     *
     * @since 1.2.0
     */
    final class BalancedResolverGroup extends AddressResolverGroup<InetSocketAddress> {

        BalancedResolverGroup() {
        }

        public static final BalancedResolverGroup INSTANCE;

        static {
            INSTANCE = new BalancedResolverGroup();
            Runtime.getRuntime().addShutdownHook(new Thread(
                INSTANCE::close,
                "R2DBC-MySQL-BalancedResolverGroup-ShutdownHook"
            ));
        }

        @Override
        protected AddressResolver<InetSocketAddress> newResolver(EventExecutor executor) {
            return new RoundRobinInetAddressResolver(executor, new DefaultNameResolver(executor)).asAddressResolver();
        }
    }
}
