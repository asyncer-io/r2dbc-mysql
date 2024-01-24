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

import io.asyncer.r2dbc.mysql.cache.Caches;
import io.asyncer.r2dbc.mysql.cache.PrepareCache;
import io.asyncer.r2dbc.mysql.cache.QueryCache;
import io.asyncer.r2dbc.mysql.client.Client;
import io.asyncer.r2dbc.mysql.codec.Codecs;
import io.asyncer.r2dbc.mysql.codec.CodecsBuilder;
import io.asyncer.r2dbc.mysql.constant.SslMode;
import io.asyncer.r2dbc.mysql.extension.CodecRegistrar;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.unix.DomainSocketAddress;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import org.jetbrains.annotations.Nullable;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link ConnectionFactory} for creating connections to a MySQL database.
 */
public final class MySqlConnectionFactory implements ConnectionFactory {

    private final Mono<MySqlConnection> client;

    private MySqlConnectionFactory(Mono<MySqlConnection> client) {
        this.client = client;
    }

    @Override
    public Mono<MySqlConnection> create() {
        return client;
    }

    @Override
    public ConnectionFactoryMetadata getMetadata() {
        return MySqlConnectionFactoryMetadata.INSTANCE;
    }

    /**
     * Creates a {@link MySqlConnectionFactory} with a {@link MySqlConnectionConfiguration}.
     *
     * @param configuration the {@link MySqlConnectionConfiguration}.
     * @return configured {@link MySqlConnectionFactory}.
     */
    public static MySqlConnectionFactory from(MySqlConnectionConfiguration configuration) {
        requireNonNull(configuration, "configuration must not be null");

        LazyQueryCache queryCache = new LazyQueryCache(configuration.getQueryCacheSize());

        return new MySqlConnectionFactory(Mono.defer(() -> {
            MySqlSslConfiguration ssl;
            SocketAddress address;

            if (configuration.isHost()) {
                ssl = configuration.getSsl();
                address = InetSocketAddress.createUnresolved(configuration.getDomain(),
                    configuration.getPort());
            } else {
                ssl = MySqlSslConfiguration.disabled();
                address = new DomainSocketAddress(configuration.getDomain());
            }

            String database = configuration.getDatabase();
            boolean createDbIfNotExist = configuration.isCreateDatabaseIfNotExist();
            String user = configuration.getUser();
            CharSequence password = configuration.getPassword();
            SslMode sslMode = ssl.getSslMode();
            ConnectionContext context = new ConnectionContext(
                configuration.getZeroDateOption(),
                configuration.getLoadLocalInfilePath(),
                configuration.getLocalInfileBufferSize(),
                configuration.getServerZoneId()
            );
            Extensions extensions = configuration.getExtensions();
            Predicate<String> prepare = configuration.getPreferPrepareStatement();
            int prepareCacheSize = configuration.getPrepareCacheSize();
            Publisher<String> passwordPublisher = configuration.getPasswordPublisher();

            if (Objects.nonNull(passwordPublisher)) {
                return Mono.from(passwordPublisher).flatMap(token -> getMySqlConnection(
                    configuration, queryCache,
                    ssl, address,
                    database, createDbIfNotExist,
                    user, sslMode, context,
                    extensions, prepare,
                    prepareCacheSize, token
                ));
            }

            return getMySqlConnection(
                configuration, queryCache,
                ssl, address,
                database, createDbIfNotExist,
                user, sslMode, context,
                extensions, prepare,
                prepareCacheSize, password
            );
        }));
    }

    private static Mono<MySqlConnection> getMySqlConnection(
            final MySqlConnectionConfiguration configuration,
            final LazyQueryCache queryCache,
            final MySqlSslConfiguration ssl,
            final SocketAddress address,
            final String database,
            final boolean createDbIfNotExist,
            final String user,
            final SslMode sslMode,
            final ConnectionContext context,
            final Extensions extensions,
            @Nullable final Predicate<String> prepare,
            final int prepareCacheSize,
            @Nullable final CharSequence password) {
        return Client.connect(ssl, address, configuration.isTcpKeepAlive(), configuration.isTcpNoDelay(),
                context, configuration.getConnectTimeout(), configuration.getSocketTimeout())
            .flatMap(client -> {
                // Lazy init database after handshake/login
                String db = createDbIfNotExist ? "" : database;
                return QueryFlow.login(client, sslMode, db, user, password, context);
            })
            .flatMap(client -> {
                ByteBufAllocator allocator = client.getByteBufAllocator();
                CodecsBuilder builder = Codecs.builder(allocator);
                PrepareCache prepareCache = Caches.createPrepareCache(prepareCacheSize);
                String db = createDbIfNotExist ? database : "";

                extensions.forEach(CodecRegistrar.class, registrar ->
                    registrar.register(allocator, builder));

                return MySqlConnection.init(client, builder.build(), context, db, queryCache.get(),
                    prepareCache, prepare);
            });
    }

    private static final class LazyQueryCache {

        private final int capacity;

        private final ReentrantLock lock = new ReentrantLock();

        @Nullable
        private volatile QueryCache cache;

        private LazyQueryCache(int capacity) {
            this.capacity = capacity;
        }

        public QueryCache get() {
            QueryCache cache = this.cache;
            if (cache == null) {
                lock.lock();
                try {
                    if ((cache = this.cache) == null) {
                        this.cache = cache = Caches.createQueryCache(capacity);
                    }
                    return cache;
                } finally {
                    lock.unlock();
                }
            }
            return cache;
        }
    }
}
