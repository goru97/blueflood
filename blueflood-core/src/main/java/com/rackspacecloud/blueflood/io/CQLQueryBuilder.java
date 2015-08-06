package com.rackspacecloud.blueflood.io;

import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.rackspacecloud.blueflood.io.serializers.DatastaxSerializer;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Metric;

import java.nio.ByteBuffer;

public final class CQLQueryBuilder{

    private static final String keyspace = String.format("\"%s\"", CoreConfig.ROLLUP_KEYSPACE.getDefaultValue());
    public static Batch getFullMetricsBatch(){
        return QueryBuilder.batch();
    }


    public static Batch addLocator(long shard, Locator locator, Batch batch, int ttl){
        Insert insertLocator = QueryBuilder
                .insertInto(keyspace, CassandraModel.CF_METRICS_LOCATOR.getName())
                .value("key", shard)
                .value("column1", locator);
        if(ttl != 0){
            Insert.Options options =  insertLocator.using(QueryBuilder.ttl(ttl));
            batch.add(insertLocator).add(options);
        }
        else
            batch.add(insertLocator);
        return batch;
    }

    public static Batch addStringMetric(Metric metric, String persist, Batch batch){
        int ttl = metric.getTtlInSeconds();
        Insert insertString = QueryBuilder
                .insertInto(keyspace, CassandraModel.CF_METRICS_STRING.getName())
                .value("key", metric.getLocator())
                .value("column1", metric.getCollectionTime())
                .value("value", persist);

        Insert.Options options =  insertString.using(QueryBuilder.ttl(ttl));
        batch.add(insertString).add(options);
        return batch;
    }
    public static Batch addMetric(Metric metric, Batch batch){

        int ttl = metric.getTtlInSeconds();
        ByteBuffer serializedMetricValue = DatastaxSerializer.RawSerializer.serialize(metric.getMetricValue());
        Insert insertMetric = QueryBuilder
                .insertInto(keyspace, CassandraModel.CF_METRICS_FULL.getName())
                .value("key", metric.getLocator())
                .value("column1", metric.getCollectionTime())
                .value("value", serializedMetricValue);

        Insert.Options options =  insertMetric.using(QueryBuilder.ttl(ttl));
        batch.add(insertMetric).add(options);
        return batch;
    }

    //TODO: Remove this method after successful tests
    public static Batch addDummyMetric(){

        Batch batch = getFullMetricsBatch();
        int ttl = 30;
        ByteBuffer serializedMetricValue = DatastaxSerializer.RawSerializer.serialize(Double.valueOf(97));
        Insert insertMetric = QueryBuilder
                .insertInto(keyspace, CassandraModel.CF_METRICS_FULL.getName())
                .value("key", "15581.int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.99")
                .value("column1", 1438634153)
                .value("value", serializedMetricValue);

        Insert.Options options =  insertMetric.using(QueryBuilder.ttl(ttl));
        batch.add(insertMetric);
        System.out.println(batch.toString());
        return batch;
    }
}
