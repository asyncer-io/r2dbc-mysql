package io.asyncer.r2dbc.mysql;

import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class HostAndPort {
    private final String host;
    private final int port;

    public HostAndPort(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public static List<HostAndPort> from(@Nullable String hosts, int portFallback) {
        if (null == hosts) {
            return Collections.emptyList();
        }
        List<HostAndPort> hostAddresses = new ArrayList<>();
        String[] tmpHosts = hosts.split(",");
        for (String tmpHost : tmpHosts) {
            if (tmpHost.contains(":")) {
                hostAddresses.add(
                        new HostAndPort(
                                tmpHost.substring(0, tmpHost.indexOf(":")),
                                Integer.parseInt(tmpHost.substring(tmpHost.indexOf(":") + 1))));
            } else {
                hostAddresses.add(new HostAndPort(tmpHost, portFallback));
            }
        }
        return Collections.unmodifiableList(hostAddresses);
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HostAndPort)) {
            return false;
        }

        HostAndPort that = (HostAndPort) o;

        return port == that.port && host.equals(that.host);
    }

    @Override
    public String toString() {
        return "HostAndPort{" +
               "host='" + host + '\'' +
               ", port=" + port +
               '}';
    }
}
