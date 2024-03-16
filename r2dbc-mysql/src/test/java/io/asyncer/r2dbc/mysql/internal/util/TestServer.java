package io.asyncer.r2dbc.mysql.internal.util;

import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

public interface TestServer {

    InternalLogger logger = InternalLoggerFactory.getInstance(TestServer.class);

    void start();

    void stop();

    String getHost();

    int getPort();

    String getDatabase();

    String getUsername();

    String getPassword();


}
