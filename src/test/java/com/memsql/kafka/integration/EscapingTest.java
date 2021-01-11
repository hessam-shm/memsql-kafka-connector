package com.memsql.kafka.integration;

import com.memsql.kafka.sink.MemSQLDialect;
import com.memsql.kafka.sink.MemSQLSinkConfig;
import com.memsql.kafka.sink.MemSQLSinkTask;
import com.memsql.kafka.utils.ConfigHelper;
import com.memsql.kafka.utils.JdbcHelper;
import com.memsql.kafka.utils.SQLHelper;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.memsql.kafka.utils.SinkRecordCreator.createRecord;
import static org.junit.Assert.*;

public class EscapingTest extends IntegrationBase {
    private static final String weirdName = "\\....1234567890,,,\\\\\t!@#$%^&*()-+=;;```~`[]{};:\"\"qwerty";
    private static final String weirdMetadataName = "metadata" + weirdName;
    private static final String weirdReferenceName = "ref" + weirdName;
    private static MemSQLSinkConfig conf;

    @Before
    public void createTables() {
        try {
            executeQuery("USING testdb DROP TABLE IF EXISTS "+ MemSQLDialect.quoteIdentifier(weirdName));
            executeQuery("USING testdb DROP TABLE IF EXISTS "+MemSQLDialect.quoteIdentifier(weirdMetadataName));
            executeQuery("USING testdb DROP TABLE IF EXISTS "+MemSQLDialect.quoteIdentifier(weirdReferenceName));

            Map<String, String> props = ConfigHelper.getMinimalRequiredParameters();
            props.put(MemSQLSinkConfig.METADATA_TABLE_NAME, weirdMetadataName);
            props.put("tableKey.primary."+weirdName, MemSQLDialect.quoteIdentifier(weirdName));
            conf = new MemSQLSinkConfig(props);

            List<SinkRecord> records = new ArrayList<>();
            Schema schema = SchemaBuilder.struct().field(weirdName, Schema.STRING_SCHEMA);
            records.add(createRecord(schema, new Struct(schema).put(weirdName, weirdName), weirdName));

            MemSQLSinkTask task = new MemSQLSinkTask();
            task.start(props);
            // create table, create metadata table, insert row into table, insert row into metadata table
            task.put(records);
            task.stop();
        } catch (Exception e) {
            log.error("", e);
            fail("Should not have thrown any exception");
        }
    }

    @Test
    public void checkTableContent() {
        try {
            ResultSet res = SQLHelper.executeQuery(conf, "USING testdb SELECT * FROM "+MemSQLDialect.quoteIdentifier(weirdName));
            while(res.next()) {
                assertEquals(res.getString(weirdName), weirdName);
            }
        } catch (Exception e) {
            log.error("", e);
            fail("Should not have thrown any exception");
        }
    }

    @Test
    public void checkMetadataTableContent() {
        try {
            ResultSet res = SQLHelper.executeQuery(conf, "USING testdb SELECT * FROM "+MemSQLDialect.quoteIdentifier(weirdMetadataName));
            while(res.next()) {
                assertEquals(res.getString("id"), weirdName + "-0-0");
                assertEquals(res.getInt("count"), 1);
            }
        } catch (Exception e) {
            log.error("", e);
            fail("Should not have thrown any exception");
        }
    }

    @Test
    public void metadataRecordExists() {
        Connection conn = JdbcHelper.getDDLConnection(conf);
        try {
            assertTrue(JdbcHelper.metadataRecordExists(conn, weirdName + "-0-0", conf));
            assertFalse(JdbcHelper.metadataRecordExists(conn, "nonexistent metadata record", conf));
        } catch (Exception e) {
            log.error("", e);
            fail("Should not have thrown any exception");
        } finally {
            JdbcHelper.releaseDDLConnection(conn);
        }
    }

    @Test
    public void tableExists() {
        Connection conn = JdbcHelper.getDDLConnection(conf);
        try {
            assertTrue(JdbcHelper.tableExists(conn, weirdName));
            assertTrue(JdbcHelper.tableExists(conn, weirdMetadataName));
            assertFalse(JdbcHelper.tableExists(conn, "nonexistent table"));
        } catch (Exception e) {
            log.error("", e);
            fail("Should not have thrown any exception");
        } finally {
            JdbcHelper.releaseDDLConnection(conn);
        }
    }

    @Test
    public void isRefereceTable() {
        try {
            assertFalse(JdbcHelper.isReferenceTable(conf, weirdName));
            assertFalse(JdbcHelper.isReferenceTable(conf, weirdMetadataName));
            executeQuery(String.format("USING testdb CREATE REFERENCE TABLE %s (a int, PRIMARY KEY(a))", MemSQLDialect.quoteIdentifier(weirdReferenceName)));
            assertTrue(JdbcHelper.isReferenceTable(conf, weirdReferenceName));
        } catch (Exception e) {
            log.error("", e);
            fail("Should not have thrown any exception");
        }
    }
}
