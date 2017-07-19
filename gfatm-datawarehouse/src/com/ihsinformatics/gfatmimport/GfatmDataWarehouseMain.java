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
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

import com.ihsinformatics.util.DatabaseUtil;
import com.ihsinformatics.util.VersionUtil;

/**
 * @author owais.hussain@ihsinformatics.com
 *
 */
public class GfatmDataWarehouseMain {

	private static final Logger log = Logger.getLogger(Class.class.getName());
	private static final String createWarehouseFile = "create_datawarehouse.sql";
	private static final String destroyWarehouseFile = "destroy_datawarehouse.sql";
	private static final VersionUtil version = new VersionUtil(true, false,
			false, 0, 1, 1);
	private static String propertiesFile = "gfatm-sync.properties";
	private DatabaseUtil localDb;
	private Properties props;
	private String dataPath;
	private String dwSchema;

	private GfatmDataWarehouseMain() {
		dataPath = System.getProperty("user.home") + File.separatorChar;
		localDb = new DatabaseUtil();
		props = new Properties();
		// Create data directory if not
		File file = new File(dataPath + File.separatorChar + dwSchema);
		if (!(file.exists() && file.isDirectory())) {
			file.mkdir();
			file.setWritable(true, false);
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		// Check arguments first
		if (args[0] == null || args.length == 0) {
			System.out
					.println("Arguments are invalid. Arguments must be provided as:");
			System.out.println("-p path to properties file");
			System.out
					.println("-r to hard reset warehouse (Extract/Load > Transform > Dimensional modeling > Fact tables)");
			System.out
					.println("-d to create data warehouse dimentions and facts");
			System.out.println("-u to update data warehouse (nightly run)");
			return;
		}
		System.out.println(version.toString());
		boolean doReset = false, doUpdateDw = false;
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-p")) {
				propertiesFile = args[i + 1];
			} else if (args[i].equalsIgnoreCase("-r")) {
				doReset = true;
			} else if (args[i].equalsIgnoreCase("-u")) {
				doUpdateDw = true;
			}
		}
		if (!(doReset | doUpdateDw)) {
			System.out.println("No valid parameters are defined. Exiting");
			System.exit(-1);
		}
		// Read properties file
		GfatmDataWarehouseMain gfatm = new GfatmDataWarehouseMain();
		gfatm.readProperties(propertiesFile);
		ImportController importController = new ImportController(gfatm.localDb);
		try {
			if (doReset) {
				gfatm.destroyDatawarehouse();
			}
			gfatm.createDatawarehouse();
			importController.importData();
			DimensionController dimController = new DimensionController(
					gfatm.localDb);
			dimController.modelDimensions();
			FactController factController = new FactController(
					gfatm.localDb);
			factController.modelFacts();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(0);
	}

	/**
	 * Read properties from properties file
	 */
	public void readProperties(String propertiesFile) {
		try {
			InputStream propFile = Thread.currentThread().getContextClassLoader().getResourceAsStream(propertiesFile);
			if (propFile != null) {
				props.load(propFile);
				String url = props.getProperty("local.connection.url",
						"jdbc:mysql://localhost:3306/gfatm_dw");
				String driverName = props.getProperty(
						"local.connection.driver_class",
						"com.mysql.jdbc.Driver");
				dwSchema = props.getProperty("local.connection.database",
						"gfatm_dw");
				String username = props.getProperty(
						"local.connection.username", "root");
				String password = props
						.getProperty("local.connection.password");
				localDb.setConnection(url, dwSchema, driverName, username,
						password);
				System.out.println(localDb.tryConnection());
			}
		} catch (IOException e) {
			e.printStackTrace();
			log.severe("Properties file not found in class path.");
		}
	}

	/**
	 * Delete all data warehouse tables
	 */
	public void destroyDatawarehouse() {
		try {
			SqlExecuteUtil sqlUtil = new SqlExecuteUtil(localDb.getUrl(), localDb.getDriverName(), localDb.getUsername(), localDb.getPassword());
			sqlUtil.execute(destroyWarehouseFile);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Create all tables from SQL script
	 */
	public void createDatawarehouse() {
		try {
			SqlExecuteUtil sqlUtil = new SqlExecuteUtil(localDb.getUrl(), localDb.getDriverName(), localDb.getUsername(), localDb.getPassword());
			sqlUtil.execute(createWarehouseFile);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
