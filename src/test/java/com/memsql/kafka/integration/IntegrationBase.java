package com.memsql.kafka.integration;

import com.memsql.kafka.sink.MemSQLDbWriter;
import com.memsql.kafka.sink.MemSQLDialect;
import com.memsql.kafka.sink.MemSQLSinkConfig;
import com.memsql.kafka.sink.MemSQLSinkTask;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

public class IntegrationBase {
    protected static final Logger log = LoggerFactory.getLogger(MemSQLDbWriter.class);

    public static Connection jdbcConnection;

    @BeforeClass
    public static void setupDatabase() throws SQLException {
        // override global JVM timezone to GMT
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));

        Properties connProperties = new Properties();
        connProperties.put("user", "root");
        String password;
        if ((password = System.getenv("MEMSQL_PASSWORD")) != null) {
            connProperties.put("password", password);
        }
        jdbcConnection = DriverManager.getConnection("jdbc:mysql://localhost:5506/memsql",
                connProperties);

        // make memsql use less memory
        executeQuery("SET GLOBAL default_partitions_per_leaf = 2");

        executeQuery("DROP DATABASE IF EXISTS testdb");
        executeQuery("CREATE DATABASE testdb");
    }

    public static void executeQuery(String sql) throws SQLException{
        log.trace("Executing SQL:\n{}", sql);
        try (Statement stmt = jdbcConnection.createStatement()) {
            stmt.execute(sql);
        }
    }

    public static void put(Map<String, String> props, List<SinkRecord> records) throws SQLException {
        props.put(MemSQLSinkConfig.DDL_ENDPOINT, "localhost:5506");
        props.put(MemSQLSinkConfig.CONNECTION_DATABASE, "testdb");
        String password;
        if ((password = System.getenv("MEMSQL_PASSWORD")) != null) {
            props.put(MemSQLSinkConfig.CONNECTION_PASSWORD, password);
        }
        props.put(MemSQLSinkConfig.METADATA_TABLE_ALLOW, "false");

        executeQuery(String.format("DROP TABLE IF EXISTS testdb.%s", MemSQLDialect.quoteIdentifier(records.iterator().next().topic())));

        MemSQLSinkTask task = new MemSQLSinkTask();
        task.start(props);
        task.put(records);
        task.stop();
    }
}
