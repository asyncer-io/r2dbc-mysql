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

import io.asyncer.r2dbc.mysql.constant.HaMode;
import io.asyncer.r2dbc.mysql.constant.SslMode;
import io.asyncer.r2dbc.mysql.constant.TlsVersions;
import io.asyncer.r2dbc.mysql.constant.ZeroDateOption;
import io.asyncer.r2dbc.mysql.extension.Extension;
import io.netty.handler.ssl.SslContextBuilder;
import org.assertj.core.api.ObjectAssert;
import org.assertj.core.api.ThrowableTypeAssert;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

/**
 * Unit tests for {@link MySqlConnectionConfiguration}.
 */
class MySqlConnectionConfigurationTest {

    private static final String SINGLE_HOST = "localhost";

    private static final String MULTIPLE_HOST = "host0:3307,host1,host2:3308";

    private static final List<HostAndPort> HOSTS_SINGLE = new ArrayList<HostAndPort>() {{
        add(new HostAndPort("localhost", 3306));
    }};

    private static final List<HostAndPort> HOSTS_MULTIPLE = new ArrayList<HostAndPort>() {{
        add(new HostAndPort("host0", 3307));
        add(new HostAndPort("host1", 3306));
        add(new HostAndPort("host2", 3308));
    }};

    private static final String UNIX_SOCKET = "/path/to/mysql.sock";

    private static final String USER = "root";

    private static final String SSL_CA = "/path/to/mysql/ca.pem";

    @Test
    void invalid() {
        ThrowableTypeAssert<?> asserted = assertThatIllegalArgumentException();

        asserted.isThrownBy(() -> MySqlConnectionConfiguration.builder().build());
        asserted.isThrownBy(() -> MySqlConnectionConfiguration.builder().build());
        asserted.isThrownBy(() -> MySqlConnectionConfiguration.builder().host(SINGLE_HOST).build());
        asserted.isThrownBy(() -> MySqlConnectionConfiguration.builder().unixSocket(UNIX_SOCKET).build());
        asserted.isThrownBy(() -> MySqlConnectionConfiguration.builder().user(USER).build());
        asserted.isThrownBy(() -> MySqlConnectionConfiguration.builder().user(USER).unixSocket(UNIX_SOCKET).haMode("loadbalance").build());
        asserted.isThrownBy(() -> MySqlConnectionConfiguration.builder().user(USER).host(SINGLE_HOST).haMode("fallback").build());
    }

    @Test
    void unixSocket() {
        for (SslMode mode : SslMode.values()) {
            if (mode.startSsl()) {
                assertThatIllegalArgumentException().isThrownBy(() -> unixSocketSslMode(mode))
                    .withMessageContaining("sslMode");
            } else {
                assertThat(unixSocketSslMode(SslMode.DISABLED)).isNotNull();
            }
        }

        for (HaMode mode: HaMode.values()) {
            if (mode != HaMode.NONE) {
                assertThatIllegalArgumentException().isThrownBy(() -> unixSocketHaMode(mode.toString()))
                                                    .withMessageContaining("haMode");
            } else {
                assertThat(unixSocketHaMode(HaMode.NONE.toString())).isNotNull();
            }
        }

        MySqlConnectionConfiguration configuration = MySqlConnectionConfiguration.builder()
            .unixSocket(UNIX_SOCKET)
            .user(USER)
            .build();
        ObjectAssert<MySqlConnectionConfiguration> asserted = assertThat(configuration);
        asserted.extracting(MySqlConnectionConfiguration::isHost).isEqualTo(false);
        asserted.extracting(MySqlConnectionConfiguration::getUnixSocket).isEqualTo(UNIX_SOCKET);
        asserted.extracting(MySqlConnectionConfiguration::getHaMode).isEqualTo(HaMode.NONE);
        asserted.extracting(MySqlConnectionConfiguration::getUser).isEqualTo(USER);
        asserted.extracting(MySqlConnectionConfiguration::getSsl)
            .extracting(MySqlSslConfiguration::getSslMode).isEqualTo(SslMode.DISABLED);
    }

    @Test
    void singleHosted() {
        MySqlConnectionConfiguration configuration = MySqlConnectionConfiguration.builder()
            .host(SINGLE_HOST)
            .user(USER)
            .build();
        ObjectAssert<MySqlConnectionConfiguration> asserted = assertThat(configuration);
        asserted.extracting(MySqlConnectionConfiguration::isHost).isEqualTo(true);
        asserted.extracting(MySqlConnectionConfiguration::getUnixSocket).isNull();
        asserted.extracting(MySqlConnectionConfiguration::getHosts).asList().isEqualTo(HOSTS_SINGLE);
        asserted.extracting(MySqlConnectionConfiguration::getHaMode).isEqualTo(HaMode.NONE);
        asserted.extracting(MySqlConnectionConfiguration::getUser).isEqualTo(USER);
        asserted.extracting(MySqlConnectionConfiguration::getSsl)
            .extracting(MySqlSslConfiguration::getSslMode).isEqualTo(SslMode.PREFERRED);
    }

    @Test
    void multiHostedDefaultHaMode() {
        MySqlConnectionConfiguration configuration = MySqlConnectionConfiguration.builder()
                                                                                 .host(MULTIPLE_HOST)
                                                                                 .user(USER)
                                                                                 .build();
        ObjectAssert<MySqlConnectionConfiguration> asserted = assertThat(configuration);
        asserted.extracting(MySqlConnectionConfiguration::isHost).isEqualTo(true);
        asserted.extracting(MySqlConnectionConfiguration::getUnixSocket).isNull();
        asserted.extracting(MySqlConnectionConfiguration::getHosts).asList().isEqualTo(HOSTS_MULTIPLE);
        asserted.extracting(MySqlConnectionConfiguration::getHaMode).isEqualTo(HaMode.FALLBACK);
        asserted.extracting(MySqlConnectionConfiguration::getUser).isEqualTo(USER);
        asserted.extracting(MySqlConnectionConfiguration::getSsl)
                .extracting(MySqlSslConfiguration::getSslMode).isEqualTo(SslMode.PREFERRED);
    }

    @Test
    void multiHostedLoadBalance() {
        MySqlConnectionConfiguration configuration = MySqlConnectionConfiguration.builder()
                                                                                 .host(MULTIPLE_HOST)
                                                                                 .haMode("loadbalance")
                                                                                 .user(USER)
                                                                                 .build();
        ObjectAssert<MySqlConnectionConfiguration> asserted = assertThat(configuration);
        asserted.extracting(MySqlConnectionConfiguration::isHost).isEqualTo(true);
        asserted.extracting(MySqlConnectionConfiguration::getUnixSocket).isNull();
        asserted.extracting(MySqlConnectionConfiguration::getHosts).asList().isEqualTo(HOSTS_MULTIPLE);
        asserted.extracting(MySqlConnectionConfiguration::getHaMode).isEqualTo(HaMode.LOADBALANCE);
        asserted.extracting(MySqlConnectionConfiguration::getUser).isEqualTo(USER);
        asserted.extracting(MySqlConnectionConfiguration::getSsl)
                .extracting(MySqlSslConfiguration::getSslMode).isEqualTo(SslMode.PREFERRED);
    }

    @Test
    void defaultPort() {
        final List<HostAndPort> hosts = new ArrayList<HostAndPort>();
        hosts.add(new HostAndPort("host0", 3307));
        hosts.add(new HostAndPort("host1", 7777));
        hosts.add(new HostAndPort("host2", 3308));

        MySqlConnectionConfiguration configuration = MySqlConnectionConfiguration.builder()
                                                                                 .host(MULTIPLE_HOST)
                                                                                 .port(7777)
                                                                                 .haMode("loadbalance")
                                                                                 .user(USER)
                                                                                 .build();

        ObjectAssert<MySqlConnectionConfiguration> asserted = assertThat(configuration);
        asserted.extracting(MySqlConnectionConfiguration::isHost).isEqualTo(true);
        asserted.extracting(MySqlConnectionConfiguration::getUnixSocket).isNull();
        asserted.extracting(MySqlConnectionConfiguration::getHosts).asList().isEqualTo(hosts);
        asserted.extracting(MySqlConnectionConfiguration::getHaMode).isEqualTo(HaMode.LOADBALANCE);
        asserted.extracting(MySqlConnectionConfiguration::getUser).isEqualTo(USER);
        asserted.extracting(MySqlConnectionConfiguration::getSsl)
                .extracting(MySqlSslConfiguration::getSslMode).isEqualTo(SslMode.PREFERRED);
    }

    @Test
    void allSslModeHosted() {
        String sslCa = "/path/to/ca.pem";

        for (SslMode mode : SslMode.values()) {
            ObjectAssert<MySqlConnectionConfiguration> asserted = assertThat(hostedSslMode(mode, sslCa));

            asserted.extracting(MySqlConnectionConfiguration::isHost).isEqualTo(true);
            asserted.extracting(MySqlConnectionConfiguration::getUnixSocket).isNull();
            asserted.extracting(MySqlConnectionConfiguration::getHosts).asList().isEqualTo(HOSTS_SINGLE);
            asserted.extracting(MySqlConnectionConfiguration::getUser).isEqualTo(USER);
            asserted.extracting(MySqlConnectionConfiguration::getSsl)
                .extracting(MySqlSslConfiguration::getSslMode).isEqualTo(mode);

            if (mode.startSsl()) {
                asserted.extracting(MySqlConnectionConfiguration::getSsl)
                    .extracting(MySqlSslConfiguration::getSslCa).isSameAs(sslCa);
            }
        }
    }

    @Test
    void invalidPort() {
        ThrowableTypeAssert<?> asserted = assertThatIllegalArgumentException();

        asserted.isThrownBy(() -> MySqlConnectionConfiguration.builder().port(-1));
        asserted.isThrownBy(() -> MySqlConnectionConfiguration.builder().port(65536));
    }

    @Test
    void allFillUp() {
        assertThat(filledUp()).extracting(MySqlConnectionConfiguration::getSsl).isNotNull();
    }

    @Test
    void isEquals() {
        assertThat(filledUp()).isEqualTo(filledUp()).extracting(Objects::hashCode)
            .isEqualTo(filledUp().hashCode());
    }

    @Test
    void sslContextBuilderCustomizer() {
        String message = "Worked!";
        Function<SslContextBuilder, SslContextBuilder> customizer = ignored -> {
            throw new IllegalStateException(message);
        };
        MySqlConnectionConfiguration configuration = MySqlConnectionConfiguration.builder()
            .host(SINGLE_HOST)
            .user(USER)
            .sslMode(SslMode.REQUIRED)
            .sslContextBuilderCustomizer(customizer)
            .build();
        MySqlSslConfiguration ssl = configuration.getSsl();

        assertThatIllegalStateException()
            .isThrownBy(() -> ssl.customizeSslContext(SslContextBuilder.forClient()))
            .withMessage(message);
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    void invalidSslContextBuilderCustomizer() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> MySqlConnectionConfiguration.builder().sslContextBuilderCustomizer(null))
            .withMessageContaining("sslContextBuilderCustomizer");
    }

    @Test
    void autodetectExtensions() {
        List<Extension> list = new ArrayList<>();
        MySqlConnectionConfiguration.builder()
            .host(SINGLE_HOST)
            .user(USER)
            .build()
            .getExtensions()
            .forEach(Extension.class, list::add);
        assertThat(list).isNotEmpty();
    }

    @Test
    void nonAutodetectExtensions() {
        List<Extension> list = new ArrayList<>();
        MySqlConnectionConfiguration.builder()
            .host(SINGLE_HOST)
            .user(USER)
            .autodetectExtensions(false)
            .build()
            .getExtensions()
            .forEach(Extension.class, list::add);
        assertThat(list).isEmpty();
    }

    private static MySqlConnectionConfiguration unixSocketSslMode(SslMode sslMode) {
        return MySqlConnectionConfiguration.builder()
            .unixSocket(UNIX_SOCKET)
            .user(USER)
            .sslMode(sslMode)
            .build();
    }

    private static MySqlConnectionConfiguration unixSocketHaMode(String haMode) {
        return MySqlConnectionConfiguration.builder()
            .unixSocket(UNIX_SOCKET)
            .user(USER)
            .haMode(haMode)
            .build();
    }

    private static MySqlConnectionConfiguration hostedSslMode(SslMode sslMode, @Nullable String sslCa) {
        return MySqlConnectionConfiguration.builder()
            .host(SINGLE_HOST)
            .user(USER)
            .sslMode(sslMode)
            .sslCa(sslCa)
            .build();
    }

    private static MySqlConnectionConfiguration filledUp() {
        return MySqlConnectionConfiguration.builder()
            .host(SINGLE_HOST)
            .user(USER)
            .port(3306)
            .password("database-password-in-here")
            .database("r2dbc")
            .tcpKeepAlive(true)
            .tcpNoDelay(true)
            .connectTimeout(Duration.ofSeconds(3))
            .socketTimeout(Duration.ofSeconds(4))
            .sslMode(SslMode.VERIFY_IDENTITY)
            .sslCa(SSL_CA)
            .sslCert("/path/to/mysql/client-cert.pem")
            .sslKey("/path/to/mysql/client-key.pem")
            .sslKeyPassword("pem-password-in-here")
            .tlsVersion(TlsVersions.TLS1_1, TlsVersions.TLS1_2, TlsVersions.TLS1_3)
            .serverZoneId(ZoneId.systemDefault())
            .zeroDateOption(ZeroDateOption.USE_NULL)
            .sslHostnameVerifier((host, s) -> true)
            .queryCacheSize(128)
            .prepareCacheSize(0)
            .autodetectExtensions(false)
            .build();
    }
}
