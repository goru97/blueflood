package com.rackspacecloud.blueflood.io;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Session;
import com.rackspacecloud.blueflood.io.astyanax.AstyanaxMetadataIO;
import com.rackspacecloud.blueflood.io.astyanax.AstyanaxShardStateIO;
import com.rackspacecloud.blueflood.io.datastax.DatastaxIO;
import com.rackspacecloud.blueflood.io.datastax.DMetadataIO;
import com.rackspacecloud.blueflood.io.datastax.DShardStateIO;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

/**
 * This is the test class for {@link IOContainer}
 */
// we ignore MBeans and Metrics stuff because a lot of them have static
// initializer that don't work well when the caller of those classes
// (such as AstyanaxIO) are being mocked
@PowerMockIgnore({"javax.management.*",
        "com.rackspacecloud.blueflood.utils.Metrics",
        "com.codahale.metrics.*"})
@PrepareForTest({Configuration.class})
@SuppressStaticInitializationFor( "com.rackspacecloud.blueflood.io.datastax.DatastaxIO" )
@RunWith(PowerMockRunner.class)
public class IOContainerTest {

    private static Configuration mockConfiguration;

    @BeforeClass
    public static void setupClass() throws Exception {
        // this is how you mock singleton whose instance is declared
        // as private static final, as the case for Configuration.class
        PowerMockito.suppress(PowerMockito.constructor(Configuration.class));
        mockConfiguration = PowerMockito.mock(Configuration.class);
        PowerMockito.mockStatic(Configuration.class);
        when(Configuration.getInstance()).thenReturn(mockConfiguration);
    }

    @Before
    public void setup() throws Exception {

        // mock DatastaxIO.getSession() & Session
        PowerMockito.mockStatic( DatastaxIO.class );
        Session mockSession = mock( Session.class );
        when( DatastaxIO.getSession()).thenReturn( mockSession );
        PreparedStatement mockPreparedStatement = mock( PreparedStatement.class );
        when( mockSession.prepare( any( RegularStatement.class ) ) ).thenReturn( mockPreparedStatement );
    }

    @Test
    public void testNullDriverConfig() throws Exception {
        when(mockConfiguration.getStringProperty(eq(CoreConfig.CASSANDRA_DRIVER))).thenReturn(null);
        IOContainer.resetInstance();
        IOContainer ioContainer = IOContainer.fromConfig();
        ShardStateIO shardStateIO = ioContainer.getShardStateIO();
        assertTrue("ShardStateIO instance is Astyanax", shardStateIO instanceof AstyanaxShardStateIO);
        MetadataIO metadataIO = ioContainer.getMetadataIO();
        assertTrue("MetadataIO instance is Astyanax", metadataIO instanceof AstyanaxMetadataIO );
    }

    @Test
    public void testEmptyStringDriverConfig() throws Exception {
        when(mockConfiguration.getStringProperty(eq(CoreConfig.CASSANDRA_DRIVER))).thenReturn("");
        IOContainer.resetInstance();
        IOContainer ioContainer = IOContainer.fromConfig();
        ShardStateIO shardStateIO = ioContainer.getShardStateIO();
        assertTrue("ShardStateIO instance is Astyanax", shardStateIO instanceof AstyanaxShardStateIO);
        MetadataIO metadataIO = ioContainer.getMetadataIO();
        assertTrue("MetadataIO instance is Astyanax", metadataIO instanceof AstyanaxMetadataIO );
    }

    @Test
    public void testAstyanaxDriverConfig() throws Exception {
        when(mockConfiguration.getStringProperty(eq(CoreConfig.CASSANDRA_DRIVER))).thenReturn("astyanax");
        IOContainer.resetInstance();
        IOContainer ioContainer = IOContainer.fromConfig();
        ShardStateIO shardStateIO = ioContainer.getShardStateIO();
        assertTrue("ShardStateIO instance is Astyanax", shardStateIO instanceof AstyanaxShardStateIO);
        MetadataIO metadataIO = ioContainer.getMetadataIO();
        assertTrue("MetadataIO instance is Astyanax", metadataIO instanceof AstyanaxMetadataIO );
    }

    @Test
    public void testDatastaxDriverConfig() {

        when(mockConfiguration.getStringProperty(eq(CoreConfig.CASSANDRA_DRIVER))).thenReturn("datastax");
        IOContainer.resetInstance();
        IOContainer ioContainer = IOContainer.fromConfig();
        ShardStateIO shardStateIO = ioContainer.getShardStateIO();
        assertTrue("ShardStateIO instance is Datastax", shardStateIO instanceof DShardStateIO );
        MetadataIO metadataIO = ioContainer.getMetadataIO();
        assertTrue("MetadataIO instance is Datastax", metadataIO instanceof DMetadataIO );
    }

    /**
     * This class is the test class for {@link com.rackspacecloud.blueflood.io.IOContainer.DriverType}
     */
    public static class DriverTypeTest {

        @Test
        public void testNullDriver() {
            IOContainer.DriverType driver = IOContainer.DriverType.getDriverType(null);
            assertEquals("null driver config means astyanax", IOContainer.DriverType.ASTYANAX, driver);
        }

        @Test
        public void testEmptyStringDriver() {
            IOContainer.DriverType driver = IOContainer.DriverType.getDriverType(null);
            assertEquals("empty string driver config means astyanax", IOContainer.DriverType.ASTYANAX, driver);
        }

        @Test
        public void testAstyanaxDriver() {
            IOContainer.DriverType driver = IOContainer.DriverType.getDriverType("astyanax");
            assertEquals("astyanax driver config", IOContainer.DriverType.ASTYANAX, driver);
        }

        @Test
        public void testDatastaxDriver() {
            IOContainer.DriverType driver = IOContainer.DriverType.getDriverType("datastax");
            assertEquals("datastax driver config", IOContainer.DriverType.DATASTAX, driver);
        }
    }
}
