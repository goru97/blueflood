package com.rackspacecloud.blueflood.io;

import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Metric;

import java.io.IOException;
import java.util.Collection;

public class DatastaxMetricsWriter implements IMetricsWriter{
    @Override
    public void insertFullMetrics(Collection<Metric> metrics) throws IOException {
        try {
            DatastaxWriter.getInstance().insertFull(metrics);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void insertPreaggreatedMetrics(Collection<IMetric> metrics) throws IOException {
        try {
            //DatastaxWriter.getInstance().insertMetrics(metrics, CassandraModel.CF_METRICS_PREAGGREGATED_FULL);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}
