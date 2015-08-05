package com.rackspacecloud.blueflood.io;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DatastaxIO {
    private Cluster cluster;
    private Session session;
    private PoolingOptions poolingOptions;

    public void connect(final String node, final int port)
    {
        this.cluster = Cluster.builder()
                .withLoadBalancingPolicy(new DCAwareRoundRobinPolicy("datacenter1"))
                .withPoolingOptions(getPoolingOptions())
                .addContactPoint(node)
                .withPort(port)
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

    private PoolingOptions getPoolingOptions(){
        poolingOptions = new PoolingOptions();
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

    public void close() {
        cluster.close();
    }

    //TODO: Remove this method after successful tests
    public static void main(final String[] args) {
        final DatastaxIO client = new DatastaxIO();
        final String ipAddress = args.length > 0 ? args[0] : "localhost";
        final int port = args.length > 1 ? Integer.parseInt(args[1]) : 9042;
        System.out.println("Connecting to IP Address " + ipAddress + ":" + port + "...");
        client.connect(ipAddress, port);
        //client.monitorConnection();
        client.getSession().execute(CQLQueryBuilder.addDummyMetric());
        client.close();
    }
}
