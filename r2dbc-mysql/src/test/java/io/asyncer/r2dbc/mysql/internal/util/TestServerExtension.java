package io.asyncer.r2dbc.mysql.internal.util;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;

public class TestServerExtension implements BeforeAllCallback {

    public static final TestServer server;

    static {
        final Boolean useTestContainer = TestUtil.enableTestContainer();
        final String dbType = TestUtil.getDbType();
        final String DbVersion = TestUtil.getDbVersion();
        if (useTestContainer) {
            server = new MySqlTestContainerServer(dbType, DbVersion);
        } else {
            server = new ProvidedServer();
        }
    }

    private static void init() {
        server.start();
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws Exception {
        init();
    }

    private static final class MySqlTestContainerServer implements TestServer {

        private final JdbcDatabaseContainer<?> container;

        @SuppressWarnings("resource")
        private MySqlTestContainerServer(final String dbType, final String dbVersion) {
            if ("mariadb".equals(dbType)) {
                container = new MariaDBContainer<>(dbType + ':' + dbVersion)
                        .withUsername("root")
                        .withPassword("")
                        .withNetwork(Network.newNetwork())
                        .withCommand("--character-set-server=utf8mb4",
                                     "--collation-server=utf8mb4_unicode_ci");
            } else {
                container = new MySQLContainer<>(dbType + ':' + dbVersion)
                        .withUsername("root")
                        .withNetwork(Network.newNetwork())
                        .withCommand("--local-infile=true",
                                     "--character-set-server=utf8mb4",
                                     "--collation-server=utf8mb4_unicode_ci");
                if (dbVersion.startsWith("5.5")) {
                    ((MySQLContainer<?>)container).withConfigurationOverride("testcontainer/mysql-5.5");
                }
            }
        }


        @Override
        public void start() {
            container.start();
        }

        @Override
        public void stop() {
            container.stop();
        }

        @Override
        public String getHost() {
            return container.getHost();
        }

        @Override
        public int getPort() {
            return container.getMappedPort(3306);
        }

        @Override
        public String getDatabase() {
            return container.getDatabaseName();
        }

        @Override
        public String getUsername() {
            return container.getUsername();
        }

        @Override
        public String getPassword() {
            return container.getPassword();
        }
    }

    private static final class ProvidedServer implements TestServer {

        @Override
        public void start() {
            // NOOP
        }

        @Override
        public void stop() {
            // NOOP
        }

        @Override
        public String getHost() {
            return "127.0.0.1";
        }

        @Override
        public int getPort() {
            return 3306;
        }

        @Override
        public String getDatabase() {
            return "test";
        }

        @Override
        public String getUsername() {
            return "root";
        }

        @Override
        public String getPassword() {
            return "test";
        }
    }
}