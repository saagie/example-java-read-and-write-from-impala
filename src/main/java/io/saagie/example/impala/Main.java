package io.saagie.example.impala;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
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

        //==== Select data from Impala Table
        String sqlStatement = "SELECT * FROM helloworld";
        try {
            // Set JDBC Driver
            Class.forName(JDBC_DRIVER_NAME);
            // Connect to Hive/Impala
            con = DriverManager.getConnection(connectionUrl);
            // Init Statement
            Statement stmt = con.createStatement();
            // Execute Query
            stmt.executeQuery(sqlStatement);

            logger.info("Select Impala : OK");

            //==== Insert data into Impala Table
            sqlStatement = "INSERT INTO helloworld2 SELECT * FROM helloworld";
            // Execute Query
            stmt.execute(sqlStatement);
            logger.info("Insert Impala : OK");
            // Invalidate metadata to update changes
            stmt.execute("INVALIDATE METADATA");

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
