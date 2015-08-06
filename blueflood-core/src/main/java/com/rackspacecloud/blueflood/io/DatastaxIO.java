package com.rackspacecloud.blueflood.io;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DatastaxIO {
    private static Session session;
    protected static final Configuration config = Configuration.getInstance();

    static {
        connect();
    }
    protected DatastaxIO() {
    }
    private static void connect()
    {
        final Cluster cluster = Cluster.builder()
                .withLoadBalancingPolicy(new DCAwareRoundRobinPolicy("datacenter1"))
                .withPoolingOptions(getPoolingOptions())
                .addContactPoints(config.getStringProperty(CoreConfig.CASSANDRA_HOSTS))
                .withPort(9042) //TODO: Change default port in blueflood configuration to 9042 and pass here.
                .build();
        final Metadata metadata = cluster.getMetadata();
        System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());
        for (final Host host : metadata.getAllHosts())
        {
            System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
                    host.getDatacenter(), host.getAddress(), host.getRack());
        }
        session = cluster.connect();
    }

    private static PoolingOptions getPoolingOptions(){
        final PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions
                .setCoreConnectionsPerHost(HostDistance.LOCAL,  4)
                .setMaxConnectionsPerHost(HostDistance.LOCAL, 10)
                .setCoreConnectionsPerHost(HostDistance.REMOTE, 2)
                .setMaxConnectionsPerHost(HostDistance.REMOTE, 4)
                .setHeartbeatIntervalSeconds(60); //Time after which driver will send a dummy request to the host so that the connection is not dropped by intermediate network devices (routers, firewalls…). The heartbeat interval should be set higher than SocketOptions.readTimeoutMillis
        return poolingOptions;
    }

    private void monitorConnection() { //TODO: Report the Connection Metrics to Graphite/Riemann
        ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1);
        scheduled.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                Session.State state = getSession().getState();
                for (Host host : state.getConnectedHosts()) {
                    int connections = state.getOpenConnections(host);
                    int inFlightQueries = state.getInFlightQueries(host);
                    System.out.printf("%s connections=%d current load=%d max load=%d%n",
                            host, connections, inFlightQueries, connections * 128);
                }
            }
        }, 1, 1, TimeUnit.MINUTES);
    }

    protected Session getSession() {
        return session;
    }
}