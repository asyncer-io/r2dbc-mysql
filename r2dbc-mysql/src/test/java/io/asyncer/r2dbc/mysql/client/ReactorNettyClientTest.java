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
import io.asyncer.r2dbc.mysql.ExchangeableTestSupport;
import io.asyncer.r2dbc.mysql.MySqlConnectionConfigurationTest;
import io.asyncer.r2dbc.mysql.constant.SslMode;
import io.asyncer.r2dbc.mysql.message.client.ClientMessage;
import io.asyncer.r2dbc.mysql.message.client.PingMessage;
import io.asyncer.r2dbc.mysql.message.server.CompleteMessage;
import io.asyncer.r2dbc.mysql.message.server.ServerMessage;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.SynchronousSink;
import reactor.netty.Connection;
import reactor.netty.NettyInbound;
import reactor.netty.NettyOutbound;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ReactorNettyClient}.
 */
class ReactorNettyClientTest {

    private static final CompleteMessage DONE = () -> true;

    private final Sinks.Many<Object> inbound = Sinks.many().unicast().onBackpressureBuffer();

    private final AtomicInteger sent = new AtomicInteger();

    private volatile CompletableFuture<Void> responseGate = CompletableFuture.completedFuture(null);

    private ExecutorService server;

    private EmbeddedChannel channel;

    private ReactorNettyClient client;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void startFakeServer() {
        server = Executors.newSingleThreadExecutor(r -> new Thread(r, "fake-server"));

        channel = new EmbeddedChannel();
        Connection connection = mock(Connection.class);
        NettyInbound nettyInbound = mock(NettyInbound.class);
        NettyOutbound nettyOutbound = mock(NettyOutbound.class);

        when(connection.channel()).thenReturn(channel);
        when(connection.addHandlerLast(any(String.class), any())).thenReturn(connection);
        when(connection.inbound()).thenReturn(nettyInbound);
        doReturn(inbound.asFlux()).when(nettyInbound).receiveObject();
        when(connection.outbound()).thenReturn(nettyOutbound);
        when(nettyOutbound.sendObject(any(ClientMessage.class))).thenAnswer(invocation -> {
            sent.incrementAndGet();
            responseGate.thenRunAsync(() ->
                inbound.emitNext(DONE, Sinks.EmitFailureHandler.FAIL_FAST), server);
            return nettyOutbound;
        });
        doAnswer(invocation -> {
            Mono.<Void>empty().subscribe(invocation.<Subscriber<Void>>getArgument(0));
            return null;
        }).when(nettyOutbound).subscribe(any());

        client = new ReactorNettyClient(connection,
            MySqlConnectionConfigurationTest.newSsl(SslMode.DISABLED), ConnectionContextTest.mock());
    }

    @AfterEach
    void stopServer() {
        server.shutdownNow();
        channel.finishAndReleaseAll();
    }

    /**
     * Cancels a subscribed query after a random amount of yields to try and trigger a race which could cause
     * connections to be unusable.
     * Runs 5000 times since it's a non-deterministic race.
     */
    @Test
    void cancellingExchangeDuringStartupDoesNotStallRequestQueue() throws Exception {
        ExecutorService canceller = Executors.newSingleThreadExecutor(r -> new Thread(r, "canceller"));
        try {
            for (int i = 0; i < 5_000; i++) {
                CompletableFuture<Subscription> subscription = new CompletableFuture<>();
                long yields = ThreadLocalRandom.current().nextLong(400);

                Flux<ServerMessage> query = client.exchange(ExchangeableTestSupport.simpleQuery("SELECT 1"))
                    .doOnSubscribe(subscription::complete);

                Future<?> cancellation = canceller.submit(() -> {
                    for (long j = 0; j < yields; j++) {
                        Thread.yield();
                    }

                    subscription.get(2, TimeUnit.SECONDS).cancel();
                    return null;
                });

                query.subscribe();
                cancellation.get(2, TimeUnit.SECONDS);

                assertQueryCompletes(i + 1);
            }
        } finally {
            canceller.shutdownNow();
        }
    }

    @Test
    void subscribedToInboundFirst() {
        CompletableFuture<Void> responses = pauseResponses();
        ServerFirstExchangeable exchangeable = new ServerFirstExchangeable();

        StepVerifier.create(client.exchange(exchangeable))
            .then(() -> {
                assertThat(sent).hasValue(0);
                inbound.emitNext(DONE, Sinks.EmitFailureHandler.FAIL_FAST);
                responses.complete(null);
            })
            .expectComplete()
            .verify(Duration.ofSeconds(2));

        assertThat(sent).hasValue(1);
    }

    @Test
    void cancellingQueuedExchangeDisposesItImmediately() {
        CompletableFuture<Void> responses = pauseResponses();
        DisposableExchangeable exchangeable = new DisposableExchangeable();

        StepVerifier.create(client.exchange(ExchangeableTestSupport.simpleQuery("SELECT 1")).then())
            .then(() -> {
                assertThat(sent).hasValue(1);
                client.exchange(exchangeable).subscribe().dispose();
                assertThat(exchangeable.isDisposed()).isTrue();
                responses.complete(null);
            })
            .expectComplete()
            .verify(Duration.ofSeconds(2));

        StepVerifier.create(client.exchange(ExchangeableTestSupport.simpleQuery("SELECT 1")).then())
            .expectComplete()
            .verify(Duration.ofSeconds(2));
    }

    private void assertQueryCompletes(int iteration) {
        assertThatCode(() -> client.exchange(ExchangeableTestSupport.simpleQuery("SELECT 1"))
            .then()
            .block(Duration.ofSeconds(2)))
            .as("probe query after cancellation %s; sent=%s", iteration, sent)
            .doesNotThrowAnyException();
    }

    private CompletableFuture<Void> pauseResponses() {
        CompletableFuture<Void> responses = new CompletableFuture<>();
        responseGate = responses;
        return responses;
    }

    private static final class ServerFirstExchangeable extends FluxExchangeable<Void> {

        private final Sinks.Many<ClientMessage> requests = Sinks.many().unicast().onBackpressureBuffer();

        private final AtomicBoolean started = new AtomicBoolean();

        @Override
        public void subscribe(CoreSubscriber<? super ClientMessage> actual) {
            requests.asFlux().subscribe(actual);
        }

        @Override
        public void accept(ServerMessage message, SynchronousSink<Void> sink) {
            if (started.compareAndSet(false, true)) {
                requests.emitNext(PingMessage.INSTANCE, Sinks.EmitFailureHandler.FAIL_FAST);
            } else {
                sink.complete();
            }
        }

        @Override
        public void dispose() {
            requests.tryEmitComplete();
        }
    }

    private static final class DisposableExchangeable extends FluxExchangeable<Void> {

        private final AtomicBoolean disposed = new AtomicBoolean();

        @Override
        public void subscribe(CoreSubscriber<? super ClientMessage> actual) {
            Flux.<ClientMessage>never().subscribe(actual);
        }

        @Override
        public void accept(ServerMessage message, SynchronousSink<Void> sink) {
            sink.error(new AssertionError("Unexpected response"));
        }

        @Override
        public void dispose() {
            disposed.set(true);
        }

        @Override
        public boolean isDisposed() {
            return disposed.get();
        }
    }
}
