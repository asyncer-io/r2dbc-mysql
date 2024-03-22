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

import io.asyncer.r2dbc.mysql.api.MySqlConnection;
import io.asyncer.r2dbc.mysql.cache.Caches;
import io.asyncer.r2dbc.mysql.cache.PrepareCache;
import io.asyncer.r2dbc.mysql.cache.QueryCache;
import io.asyncer.r2dbc.mysql.codec.Codecs;
import io.asyncer.r2dbc.mysql.codec.CodecsBuilder;
import io.asyncer.r2dbc.mysql.extension.CodecRegistrar;
import io.asyncer.r2dbc.mysql.internal.util.StringUtils;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link ConnectionFactory} for creating connections to a MySQL database.
 */
public final class MySqlConnectionFactory implements ConnectionFactory {

    private final Mono<? extends MySqlConnection> client;

    private MySqlConnectionFactory(Mono<? extends MySqlConnection> client) {
        this.client = client;
    }

    @Override
    public Mono<? extends MySqlConnection> create() {
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

        return new MySqlConnectionFactory(Mono.defer(() -> connectWithInit(configuration, queryCache)));
    }

    private static Mono<MySqlConnection> connectWithInit(
        MySqlConnectionConfiguration configuration,
        LazyQueryCache queryCache
    ) {
        return configuration.getSocket()
            .strategy(configuration)
            .connect()
            .flatMap(client -> {
                String database = configuration.getDatabase();
                boolean createDbIfNotExist = configuration.isCreateDatabaseIfNotExist();
                ZoneId connectionTimeZone = retrieveZoneId(configuration.getConnectionTimeZone());
                Predicate<String> prepare = configuration.getPreferPrepareStatement();
                int prepareCacheSize = configuration.getPrepareCacheSize();
                boolean forceTimeZone = configuration.isForceConnectionTimeZoneToSession();
                List<String> sessionVariables = forceTimeZone && connectionTimeZone != null ?
                    mergeSessionVariables(configuration.getSessionVariables(), connectionTimeZone) :
                    configuration.getSessionVariables();
                ByteBufAllocator allocator = client.getByteBufAllocator();
                CodecsBuilder builder = Codecs.builder();
                PrepareCache prepareCache = Caches.createPrepareCache(prepareCacheSize);
                String db = createDbIfNotExist ? database : "";
                Extensions extensions = configuration.getExtensions();

                extensions.forEach(CodecRegistrar.class, registrar ->
                    registrar.register(allocator, builder));

                Mono<MySqlConnection> c = MySqlSimpleConnection.init(client, builder.build(), db, queryCache.get(),
                    prepareCache, sessionVariables, prepare);

                if (configuration.getLockWaitTimeout() != null) {
                    c = c.flatMap(connection -> connection.setLockWaitTimeout(configuration.getLockWaitTimeout())
                        .thenReturn(connection));
                }

                if (configuration.getStatementTimeout() != null) {
                    c = c.flatMap(connection -> connection.setStatementTimeout(configuration.getStatementTimeout())
                        .thenReturn(connection));
                }

                return c;
            });
    }

    @Nullable
    private static ZoneId retrieveZoneId(String timeZone) {
        if ("LOCAL".equalsIgnoreCase(timeZone)) {
            return ZoneId.systemDefault().normalized();
        } else if ("SERVER".equalsIgnoreCase(timeZone)) {
            return null;
        }

        return StringUtils.parseZoneId(timeZone);
    }

    private static List<String> mergeSessionVariables(List<String> sessionVariables, ZoneId timeZone) {
        List<String> res = new ArrayList<>(sessionVariables.size() + 1);

        String offerStr = timeZone instanceof ZoneOffset && "Z".equalsIgnoreCase(timeZone.getId()) ?
            "+00:00" : timeZone.getId();

        res.addAll(sessionVariables);
        res.add("time_zone='" + offerStr + "'");

        return res;
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
