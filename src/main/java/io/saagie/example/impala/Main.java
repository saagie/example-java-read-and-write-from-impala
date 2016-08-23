package io.saagie.example.impala;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

public class Main {


	private static final Logger logger = Logger.getLogger("io.saagie.example.impala.ReadWrite");
	private static final String JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
	private static String connectionUrl;

	public static void main(String[] args) throws IOException {

		if (args.length < 1) {
			logger.severe("1 arg is required :\n\t- connectionurl ex: jdbc:hive2://impalahost:21050/;auth=noSasl");
			System.err.println("1 arg is required :\n\t-connectionurl  ex: jdbc:hive2://impalahost:21050/;auth=noSasl");
			System.exit(128);
		}
		connectionUrl = args[0];

		Connection con = null;
		String sqlStatement = "SELECT * FROM helloworld";
		try {
			// Set JDBC Driver
			Class.forName(JDBC_DRIVER_NAME);
			// Connect to Hive/Impala
			con = DriverManager.getConnection(connectionUrl);
			// Init Statement
			Statement stmt = con.createStatement();
			// Execute Query
			ResultSet rs = stmt.executeQuery(sqlStatement);

		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				con.close();
			} catch (Exception e) {
				// swallow
			}
		}

		sqlStatement = "INSERT INTO helloworld2 SELECT * FROM helloworld";
		try {
			// Set JDBC Driver
			Class.forName(JDBC_DRIVER_NAME);
			// Connect to Hive/Impala
			con = DriverManager.getConnection(connectionUrl);
			// Init Statement
			Statement stmt = con.createStatement();
			// Execute Query
			stmt.execute(sqlStatement);
			// Invalidate metadata to update changes
			stmt.execute("INVALIDATE METADATA");

		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				con.close();
			} catch (Exception e) {
				// swallow
			}
		}


	}
}
