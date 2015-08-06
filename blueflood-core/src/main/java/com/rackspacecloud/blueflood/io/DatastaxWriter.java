package com.rackspacecloud.blueflood.io;

import com.codahale.metrics.Timer;
import com.datastax.driver.core.querybuilder.Batch;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.types.DataType;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.utils.TimeValue;
import com.rackspacecloud.blueflood.utils.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class DatastaxWriter extends DatastaxIO{

    private static final Logger log = LoggerFactory.getLogger(DatastaxWriter.class);
    private static final DatastaxWriter instance = new DatastaxWriter();
    private boolean areStringMetricsDropped = Configuration.getInstance().getBooleanProperty(CoreConfig.STRING_METRICS_DROPPED);
    private List<String> tenantIdsKept = Configuration.getInstance().getListProperty(CoreConfig.TENANTIDS_TO_KEEP);
    private Set<String> keptTenantIdsSet = new HashSet<String>(tenantIdsKept);

    private static final int LOCATOR_TTL = 604800;  // in seconds (7 days)
    private static final TimeValue STRING_TTL = new TimeValue(730, TimeUnit.DAYS); // 2 years

    private static final Cache<String, Boolean> insertedLocators = CacheBuilder.newBuilder().expireAfterAccess(10,
            TimeUnit.MINUTES).concurrencyLevel(16).build();

    public static DatastaxWriter getInstance(){
        return instance;
    }

    private boolean shouldPersistStringMetric(Metric metric) {
        String tenantId = metric.getLocator().getTenantId();

        if(areStringMetricsDropped && !keptTenantIdsSet.contains(tenantId) ) {
            return false;
        }
        else {
            String currentValue = String.valueOf(metric.getMetricValue());
            final String lastValue = AstyanaxReader.getInstance().getLastStringValue(metric.getLocator());

            return lastValue == null || !currentValue.equals(lastValue);
        }
    }

    private boolean shouldPersist(Metric metric) {
        boolean shouldPersistMetric = true;
        try {
            final DataType metricType = metric.getDataType();
            if (metricType.equals(DataType.STRING) || metricType.equals(DataType.BOOLEAN)) {
                shouldPersistMetric = shouldPersistStringMetric(metric);
            }
        } catch (Exception e) {
            // If we hit any exception, just persist the metric
            shouldPersistMetric = true;
        }

        return shouldPersistMetric;
    }

    public void insertFull(Collection<Metric> metrics) throws Exception {
        Timer.Context ctx = Instrumentation.getWriteTimerContext(CassandraModel.CF_METRICS_FULL);

        try {
            Batch fullMetricsBatch = CQLQueryBuilder.getFullMetricsBatch();

            for (Metric metric: metrics) {
                final Locator locator = metric.getLocator();
                final boolean isString = metric.isString();
                final boolean isBoolean = metric.isBoolean();

                if (!shouldPersist(metric)) {
                    log.trace("Metric shouldn't be persisted, skipping insert", metric.getLocator().toString());
                    continue;
                }

                // key = shard
                // col = locator (acct + entity + check + dimension.metric)
                // value = <nothing>
                // do not do it for string or boolean metrics though.
                if (!DatastaxWriter.isLocatorCurrent(locator)) {
                    if (!isString && !isBoolean && fullMetricsBatch != null)
                        insertLocator(locator, fullMetricsBatch);
                    DatastaxWriter.setLocatorCurrent(locator);
                }
                insertMetric(metric, fullMetricsBatch);
                Instrumentation.markFullResMetricWritten();

            }
            getSession().execute(fullMetricsBatch);
        } finally {
            ctx.stop();
        }
    }

    private final void insertLocator(Locator locator, Batch fullMetricsBatch) {
        CQLQueryBuilder.addLocator((long) Util.getShard(locator.toString()), locator, fullMetricsBatch, LOCATOR_TTL);
    }

    private void insertMetric(Metric metric, Batch fullMetricsBatch) {
        final boolean isString = metric.isString();
        final boolean isBoolean = metric.isBoolean();

        if (isString || isBoolean) {
            metric.setTtl(STRING_TTL);
            String persist;
            if (isString) {
                persist = (String) metric.getMetricValue();
            } else { //boolean
                persist = String.valueOf(metric.getMetricValue());
            }

            CQLQueryBuilder.addStringMetric(metric, persist, fullMetricsBatch);
        }

        else {
            CQLQueryBuilder.addMetric(metric, fullMetricsBatch);
        }
    }
    private static void setLocatorCurrent(Locator loc) {
        insertedLocators.put(loc.toString(), Boolean.TRUE);
    }

    public static boolean isLocatorCurrent(Locator loc) {
        return insertedLocators.getIfPresent(loc.toString()) != null;
    }

}
