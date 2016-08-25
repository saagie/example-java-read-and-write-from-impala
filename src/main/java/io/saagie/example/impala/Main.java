package io.saagie.example.impala;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.logging.Logger;

public class Main {


    private static final Logger logger = Logger.getLogger("io.saagie.example.impala.Main");
    private static final String JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    private static String connectionUrl;

    public static void main(String[] args) throws IOException {

        if (args.length < 1) {
            logger.severe("1 arg is required :\n\t- connectionurl ex: jdbc:hive2://impalahost:21050/;auth=noSasl");
            System.err.println("1 arg is required :\n\t-connectionurl  ex: jdbc:hive2://impalahost:21050/;auth=noSasl");
            System.exit(128);
        }
        // Get url Connection
        connectionUrl = args[0];

        // Init Connection
        Connection con = null;
        String sqlStatementDrop = "DROP TABLE IF EXISTS helloworld";
        String sqlStatementCreate = "CREATE TABLE helloworld (message String) STORED AS PARQUET";
        String sqlStatementInsert = "INSERT INTO helloworld VALUES (\"helloworld\")";
        String sqlStatementSelect = "SELECT * from helloworld";
        String sqlStatementInvalidate = "INVALIDATE METADATA";
        try {
            // Set JDBC Impala Driver
            Class.forName(JDBC_DRIVER_NAME);
            // Connect to Impala
            con = DriverManager.getConnection(connectionUrl,"hdfs","");
            // Init Statement
            Statement stmt = con.createStatement();
            // Invalidate metadata to update changes
            stmt.execute(sqlStatementInvalidate);
            // Execute DROP TABLE Query
            stmt.execute(sqlStatementDrop);
            logger.info("Drop Impala table with security : OK");
            // Execute CREATE Query
            stmt.execute(sqlStatementCreate);
            logger.info("Create Impala table with security : OK");
            // Execute INSERT Query
            stmt.execute(sqlStatementInsert);
            logger.info("Insert into Impala table with security : OK");
            // Execute SELECT Query
            ResultSet rs = stmt.executeQuery(sqlStatementSelect);
            while(rs.next()) {
                logger.info(rs.getString(1));
            }
            logger.info("Select from Impala table with security : OK");
            // Invalidate metadata to update changes
            stmt.execute(sqlStatementInvalidate);

        } catch (Exception e) {
            logger.severe(e.getMessage());
        } finally {
            try {
                con.close();
            } catch (Exception e) {
                // swallow
            }
        }


    }
}
