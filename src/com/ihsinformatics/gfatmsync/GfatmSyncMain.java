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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;
import java.util.logging.Logger;

import com.ihsinformatics.util.CommandType;
import com.ihsinformatics.util.DatabaseUtil;

/**
 * @author owais.hussain@ihsinformatics.com
 *
 */
public class GfatmSyncMain {

	public static final Logger log = Logger.getLogger(Class.class.getName());
	public static final String propertiesFilePath = "res/gfatm-sync.properties";
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
		gfatm.resetWarehouse();
		gfatm.loadData();

		System.exit(0);
	}

	public GfatmSyncMain() {
		dataPath = System.getProperty("user.home") + File.separatorChar;
		dwDb = new DatabaseUtil();
		props = new Properties();
		// Create data directory if not
		File file = new File(dataPath + File.separatorChar + dwSchema);
		if (!(file.exists() && file.isDirectory())) {
			file.mkdir();
			file.setWritable(true, false);
		}
	}

	/**
	 * Read properties from properties file
	 */
	public void readProperties() {
		try {
			InputStream propFile = new FileInputStream(propertiesFilePath);
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
				System.out.println(dwDb.tryConnection());
			}
		} catch (IOException e) {
			e.printStackTrace();
			log.warning("Properties file not found in class path.");
		}
	}

	/**
	 * Load data from all sources into data warehouse
	 */
	public void loadData() {
		// Fetch source databases from _system table
		Object[][] data = dwDb.getTableData("_system",
				"implementation_id,database_name", "active=1");
		ArrayList<Integer> implementationIds = new ArrayList<Integer>();
		ArrayList<String> databases = new ArrayList<String>();
		for (Object[] record : data) {
			implementationIds.add(Integer.parseInt(record[0].toString()));
			databases.add(record[1].toString());
		}
		// Read all OpenMRS databases one-by-one into data warehouse
		for (int i = 0; i < databases.size(); i++) {
			try {
				Integer implementationId = implementationIds.get(i);
				loadPeopleData(implementationId, databases.get(i));
				loadUserData(implementationId, databases.get(i));
				loadLocationData(implementationId, databases.get(i));
				loadConceptData(implementationId, databases.get(i));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Load data from person-related tables into data warehouse
	 * 
	 * @param implementationId
	 * @param database
	 * @throws ClassNotFoundException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	public void loadPeopleData(Integer implementationId, String database)
			throws InstantiationException, IllegalAccessException,
			ClassNotFoundException {
		String[] tables = { "person_attribute_type", "person",
				"person_address", "person_attribute", "person_name" };
		for (String table : tables) {
			StringBuilder query = new StringBuilder();
			query.append("INSERT INTO " + table + " ");
			query.append("SELECT 0,'" + implementationId + "', t.* FROM " + database
					+ "." + table + " AS t ");
			query.append("WHERE uuid NOT IN (SELECT uuid FROM " + table + ")");
			dwDb.runCommand(CommandType.INSERT, query.toString());
			log.info("Inserted data from " + database + "." + table
					+ " into data warehouse");
		}
	}

	/**
	 * Load data from user-related tables into data warehouse
	 * 
	 * @param implementationId
	 * @param database
	 * @throws ClassNotFoundException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	public void loadUserData(Integer implementationId, String database)
			throws InstantiationException, IllegalAccessException,
			ClassNotFoundException {
		String[] tables = { "role", "privilege", "users", "provider" };
		for (String table : tables) {
			StringBuilder query = new StringBuilder();
			query.append("INSERT INTO " + table + " ");
			query.append("SELECT 0,'" + implementationId + "', t.* FROM " + database + "." + table + " AS t ");
			query.append("WHERE uuid NOT IN (SELECT uuid FROM " + table + ")");
			dwDb.runCommand(CommandType.INSERT, query.toString());
			log.info("Inserted data from " + database + "." + table + " into data warehouse");
		}
		// Deal with tables with no UUID and foreign relationship
		StringBuilder query = new StringBuilder();
		query.append("INSERT INTO role_role ");
		query.append("SELECT 0,'" + implementationId + "', t.* FROM " + database + ".role_role AS t ");
		query.append("WHERE CONCAT(parent_role, child_role) NOT IN (SELECT CONCAT(parent_role, child_role) FROM role_role WHERE implementation_id=" + implementationId + ")");
		dwDb.runCommand(CommandType.INSERT, query.toString());
		log.info("Inserted data from " + database + ".role_role into data warehouse");
		query = new StringBuilder();
		query.append("INSERT INTO role_privilege ");
		query.append("SELECT 0,'" + implementationId + "', t.* FROM " + database + ".role_privilege AS t ");
		query.append("WHERE CONCAT(role, privilege) NOT IN (SELECT CONCAT(role, privilege) FROM role_privilege WHERE implementation_id=" + implementationId + ")");
		dwDb.runCommand(CommandType.INSERT, query.toString());
		log.info("Inserted data from " + database + ".role_privilege into data warehouse");
		query = new StringBuilder();
		query.append("INSERT INTO user_property ");
		query.append("SELECT 0,'" + implementationId + "', t.* FROM " + database + ".user_property AS t ");
		query.append("WHERE CONCAT(user_id, property) NOT IN (SELECT CONCAT(user_id, property) FROM user_role WHERE implementation_id=" + implementationId + ")");
		dwDb.runCommand(CommandType.INSERT, query.toString());
		log.info("Inserted data from " + database + ".user_property into data warehouse");
		query = new StringBuilder();
		query.append("INSERT INTO user_role ");
		query.append("SELECT 0,'" + implementationId + "', t.* FROM " + database + ".user_role AS t ");
		query.append("WHERE CONCAT(user_id, role) NOT IN (SELECT CONCAT(user_id, role) FROM user_role WHERE implementation_id=" + implementationId + ")");
		dwDb.runCommand(CommandType.INSERT, query.toString());
		log.info("Inserted data from " + database + ".user_role into data warehouse");
	}
	
	/**
	 * Load data from location-related tables into data warehouse
	 * 
	 * @param implementationId
	 * @param database
	 * @throws ClassNotFoundException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	public void loadLocationData(Integer implementationId, String database)
			throws InstantiationException, IllegalAccessException,
			ClassNotFoundException {
		String[] tables = { "location_attribute_type", "location",
				"location_attribute", "location_tag" };
		StringBuilder query = new StringBuilder();
		for (String table : tables) {
			query = new StringBuilder();
			query.append("INSERT INTO " + table + " ");
			query.append("SELECT 0,'" + implementationId + "', t.* FROM " + database
					+ "." + table + " AS t ");
			query.append("WHERE uuid NOT IN (SELECT uuid FROM " + table + ")");
			dwDb.runCommand(CommandType.INSERT, query.toString());
			log.info("Inserted data from " + database + "." + table
					+ " into data warehouse");
		}
		query = new StringBuilder();
		query.append("INSERT INTO location_tag_map ");
		query.append("SELECT 0,'" + implementationId + "', t.* FROM " + database + ".location_tag_map AS t ");
		query.append("WHERE CONCAT(location_id, location_tag_id) NOT IN (SELECT CONCAT(location_id, location_tag_id) FROM user_role WHERE implementation_id=" + implementationId + ")");
		dwDb.runCommand(CommandType.INSERT, query.toString());
		log.info("Inserted data from " + database + ".location_tag_map into data warehouse");
	}

	/**
	 * Load data from concept-related tables into data warehouse
	 * 
	 * @param implementationId
	 * @param database
	 * @throws ClassNotFoundException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	public void loadConceptData(Integer implementationId, String database)
			throws InstantiationException, IllegalAccessException,
			ClassNotFoundException {
		String[] tables = { "concept_class", "concept_set",
				"concept_datatype", "concept_map_type", "concept_stop_word", "concept", "concept_name", "concept_numeric", "concept_description",
				"concept_answer", "concept_word", "concept_reference_map", "concept_reference_term", "concept_reference_term_map", "concept_reference_source"};
		StringBuilder query = new StringBuilder();
		for (String table : tables) {
			query = new StringBuilder();
			query.append("INSERT INTO " + table + " ");
			query.append("SELECT 0,'" + implementationId + "', t.* FROM " + database
					+ "." + table + " AS t ");
			query.append("WHERE uuid NOT IN (SELECT uuid FROM " + table + ")");
			dwDb.runCommand(CommandType.INSERT, query.toString());
			log.info("Inserted data from " + database + "." + table
					+ " into data warehouse");
		}
		query = new StringBuilder();
		query.append("INSERT INTO concept_set_derived ");
		query.append("SELECT 0,'" + implementationId + "', t.* FROM " + database + ".concept_set_derived AS t ");
		query.append("WHERE CONCAT(concept_id, concept_set) NOT IN (SELECT CONCAT(concept_id, concept_set) FROM user_role WHERE implementation_id=" + implementationId + ")");
		dwDb.runCommand(CommandType.INSERT, query.toString());
		log.info("Inserted data from " + database + ".concept_set_derived into data warehouse");
	}

	/**
	 * Remove all data from warehouse and regenerate
	 */
	public void resetWarehouse() {
		String[] dimTables = { "dim_concept", "dim_datetime", "dim_encounter",
				"dim_location", "dim_obs", "dim_patient", "dim_systems",
				"dim_user" };
		String[] factTables = {};
		try {
			// Delete data from all tables
			for (String table : dimTables) {
				dwDb.truncateTable(table);
			}
			for (String table : factTables) {
				dwDb.truncateTable(table);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
