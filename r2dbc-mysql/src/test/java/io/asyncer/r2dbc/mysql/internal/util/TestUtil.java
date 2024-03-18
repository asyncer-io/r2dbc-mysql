package io.asyncer.r2dbc.mysql.internal.util;

import io.netty.util.internal.SystemPropertyUtil;

public final class TestUtil {
    private TestUtil() {
    }

    public static Boolean enableTestContainer() {
        return SystemPropertyUtil.getBoolean("test.testcontainer", true);
    }

    public static String getDbType() {
        return System.getProperty("test.db.type", "mysql");
    }

    public static String getDbVersion() {
        return System.getProperty("test.db.version", "5.7.44");
    }

}
