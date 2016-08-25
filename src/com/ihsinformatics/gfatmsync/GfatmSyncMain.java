/* Copyright(C) 2016 Interactive Health Solutions, Pvt. Ltd.

This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License as
published by the Free Software Foundation; either version 3 of the License (GPLv3), or any later version.
This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

See the GNU General Public License for more details. You should have received a copy of the GNU General Public License along with this program; if not, write to the Interactive Health Solutions, info@ihsinformatics.com
You can also access the license on the internet at the address: http://www.gnu.org/licenses/gpl-3.0.html

Interactive Health Solutions, hereby disclaims all copyright interest in this program written by the contributors.
 */
package com.ihsinformatics.gfatmsync;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Logger;

import com.ihsinformatics.util.DatabaseUtil;

/**
 * @author owais.hussain@ihsinformatics.com
 *
 */
public class GfatmSyncMain {

	public static final Logger log = Logger.getLogger(Class.class.getName());
	public static final String propertiesFilePath = "gfatm-sync.properties";
	public static final String version = "1.0.0";
	public String dataPath;
	public String dwSchema;
	public DatabaseUtil dwDb;
	public Properties props;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		GfatmSyncMain gfatm = new GfatmSyncMain();
		gfatm.readProperties();

		System.exit(0);
	}

	public GfatmSyncMain() {
		dataPath = System.getProperty("user.home") + File.separator;
		dwDb = new DatabaseUtil();
	}

	/**
	 * Read properties from properties file
	 */
	public void readProperties() {
		try {
			InputStream propFile = GfatmSyncMain.class
					.getResourceAsStream(File.separator + propertiesFilePath);
			if (propFile != null) {
				props.load(propFile);
				String url = props.getProperty("dw.connection.url",
						"jdbc:mysql://localhost:3306/gfatm_dw");
				String driverName = props.getProperty(
						"dw.connection.driver_class", "com.mysql.jdbc.Driver");
				dwSchema = props.getProperty("dw.connection.database",
						"gfatm_dw");
				String username = props.getProperty("dw.connection.username",
						"root");
				String password = props.getProperty("dw.connection.password");
				dwDb.setConnection(url, dwSchema, driverName, username,
						password);
			}
		} catch (IOException e) {
			log.warning("Properties file not found in class path.");
		}
	}

	public void resetWarehouse() {
		String[] dimTables = { "dim_concept", "dim_datetime", "dim_encounter",
				"dim_location", "dim_obs", "dim_patient", "dim_systems",
				"dim_user" };
		try {
			// Delete data from all tables
			for (String table : dimTables) {
				dwDb.truncateTable(table);
			}
		} catch (InstantiationException | IllegalAccessException
				| ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}
