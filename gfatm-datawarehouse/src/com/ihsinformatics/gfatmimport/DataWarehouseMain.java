/* Copyright(C) 2016 Interactive Health Solutions, Pvt. Ltd.

This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License as
published by the Free Software Foundation; either version 3 of the License (GPLv3), or any later version.
This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

See the GNU General Public License for more details. You should have received a copy of the GNU General Public License along with this program; if not, write to the Interactive Health Solutions, info@ihsinformatics.com
You can also access the license on the internet at the address: http://www.gnu.org/licenses/gpl-3.0.html

Interactive Health Solutions, hereby disclaims all copyright interest in this program written by the contributors.
 */
package com.ihsinformatics.gfatmimport;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Date;
import java.util.Properties;
import java.util.logging.Logger;

import com.ihsinformatics.util.DatabaseUtil;
import com.ihsinformatics.util.DateTimeUtil;

/**
 * @author owais.hussain@ihsinformatics.com
 *
 */
public class DataWarehouseMain {

	private static final Logger log = Logger.getLogger(Class.class.getName());
	public static String resourceFilePath = System.getProperty("user.home")
			+ File.separatorChar + "gfatm" + File.separatorChar
			+ "gfatm-sync.properties";
	private DatabaseUtil dwDb;
	private Properties props;
	private String dwSchema;

	private DataWarehouseMain() {
		dwDb = new DatabaseUtil();
		props = new Properties();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		// Check arguments first
		if (args[0] == null || args.length == 0) {
			System.out
					.println("Arguments are invalid. Arguments must be provided as:");
			System.out.println("-u path to resource directory");
			System.out.println("-p path to database properties file");
			System.out
					.println("-r to reset warehouse (ETL > Dimensional modeling > Fact modeling)");
			System.out
					.println("-d to create data warehouse dimentions and fact tables");
			System.out.println("-u to update data warehouse (nightly run)");
			return;
		}
		boolean doReset = false, doUpdate = false;
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-p")) {
				resourceFilePath = args[i + 1];
			} else if (args[i].equalsIgnoreCase("-r")) {
				doReset = true;
			} else if (args[i].equalsIgnoreCase("-u")) {
				doUpdate = true;
			}
		}
		if (!(doReset | doUpdate)) {
			System.out.println("No valid parameters are defined. Exiting");
			System.exit(-1);
		}
		// Read properties file
		DataWarehouseMain dwObj = new DataWarehouseMain();
		dwObj.readProperties(resourceFilePath);
		// Fetch source databases from _implementation table
		Object[][] sources = dwObj.dwDb
				.getTableData("SELECT implementation_id,connection_url,driver,db_name,username,password,date_added,last_updated FROM _implementation WHERE active=1 AND status<>'RUNNING'");
		if (sources.length == 0) {
			log.warning("Another instance is already running. Please check the _implementation table for confirmation.");
			System.exit(-1);
		}
		// Import data from each source
		for (int i = 0; i < sources.length; i++) {
			Object[] source = sources[i];
			int implementationId = Integer.parseInt(source[0].toString());
			String url = source[1].toString();
			String driverName = source[2].toString();
			String dbName = source[3].toString();
			String username = source[4].toString();
			String password = source[5].toString();
			if (source[7] == null) {
				source[7] = new String("2000-01-01 00:00:00");
			}
			DatabaseUtil openbMrsDb = new DatabaseUtil(url, dbName, driverName,
					username, password);
			DatabaseUtil gfatmMrsDb = new DatabaseUtil(url, "gfatm",
					driverName, username, password);
			try {
				// Date dateCreated =
				// DateTimeUtil.getDateFromString(source[6].toString(),
				// DateTimeUtil.SQL_DATETIME);
				Date lastUpdated = DateTimeUtil.fromSqlDateTimeString(source[7]
						.toString());
				OpenMrsImportController openMrsImportController = new OpenMrsImportController(
						openbMrsDb, dwObj.dwDb, lastUpdated, new Date());
				GfatmImportController gfatmImportController = new GfatmImportController(
						gfatmMrsDb, dwObj.dwDb, lastUpdated, new Date());
				// Update status of implementation record
				dwObj.dwDb.updateRecord("_implementation",
						new String[] { "status" }, new String[] { "RUNNING" },
						"implementation_id='" + implementationId + "'");
				if (doReset) {
					dwObj.destroyDatawarehouse();
					dwObj.createDatawarehouse();
					gfatmImportController.importData(implementationId);
					openMrsImportController.importData(implementationId);
				} else if (doUpdate) {
					dwObj.createDatawarehouse();
					gfatmImportController.importData(implementationId);
					openMrsImportController.importData(implementationId);
				}
				DimensionController dimController = new DimensionController(
						dwObj.dwDb);
				dimController.modelDimensions();
				FactController factController = new FactController(dwObj.dwDb);
				factController.modelFacts();
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				try {
					// Update the status in _implementation table
					dwObj.dwDb.updateRecord(
							"_implementation",
							new String[] { "status", "last_updated" },
							new String[] { "STOPPED",
									DateTimeUtil.toSqlDateString(new Date()) },
							"implementation_id='" + implementationId + "'");
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		System.exit(0);
	}

	/**
	 * Read properties from properties file
	 */
	public void readProperties(String propertiesFile) {
		try {
			InputStream propFile = new FileInputStream(propertiesFile);
			props.load(propFile);
			String url = props.getProperty("local.connection.url");
			String driverName = props
					.getProperty("local.connection.driver_class");
			dwSchema = props.getProperty("local.connection.database");
			String username = props
					.getProperty("local.connection.username");
			String password = props
					.getProperty("local.connection.password");
			dwDb.setConnection(url, dwSchema, driverName, username,
					password);
			System.out.println("Local DB settings...");
			System.out.println("URL: " + dwDb.getUrl());
			System.out.println("DB Name: " + dwDb.getDbName());
			System.out.println("Driver: " + dwDb.getDriverName());
			System.out.println("Username: " + dwDb.getUsername());
			System.out.println("Trying to connect... "
					+ dwDb.tryConnection());
			if (!dwDb.tryConnection()) {
				log.severe("Unable to connect with database. Exiting...");
				System.exit(-1);
			}
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	/**
	 * Delete all data warehouse tables
	 */
	public void destroyDatawarehouse() {
		try {
			dwDb.runStoredProcedure("destroy_datawarehouse", null);
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Create all tables from SQL script
	 */
	public void createDatawarehouse() {
		try {
			dwDb.runStoredProcedure("create_datawarehouse", null);
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
