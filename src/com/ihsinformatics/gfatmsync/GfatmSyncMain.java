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
		// Synchronize all OpenMRS databases one-by-one into data warehouse
		for (int i = 0; i < databases.size(); i++) {
			try {
				Integer implementationId = implementationIds.get(i);
				loadPeopleData(implementationId, databases.get(i));
				loadUserData(implementationId, databases.get(i));
				loadLocationData(implementationId, databases.get(i));
				loadConceptData(implementationId, databases.get(i));
				loadPatientData(implementationId, databases.get(i));
				loadEncounterData(implementationId, databases.get(i));
				updatePeopleData(implementationId, databases.get(i));
				updateUserData(implementationId, databases.get(i));
				updateLocationData(implementationId, databases.get(i));
				updateConceptData(implementationId, databases.get(i));
				updatePatientData(implementationId, databases.get(i));
				updateEncounterData(implementationId, databases.get(i));
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
			query.append("SELECT 0,'" + implementationId + "', t.* FROM "
					+ database + "." + table + " AS t ");
			query.append("WHERE t.uuid NOT IN (SELECT uuid FROM " + table + ")");
			log.info("Inserting data from " + database + "." + table
					+ " into data warehouse");
			dwDb.runCommand(CommandType.INSERT, query.toString());
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
			query.append("SELECT 0,'" + implementationId + "', t.* FROM "
					+ database + "." + table + " AS t ");
			query.append("WHERE t.uuid NOT IN (SELECT uuid FROM " + table + ")");
			log.info("Inserting data from " + database + "." + table
					+ " into data warehouse");
			dwDb.runCommand(CommandType.INSERT, query.toString());
		}
		// Deal with tables with no UUID and foreign relationship
		StringBuilder query = new StringBuilder();
		query.append("INSERT INTO role_role ");
		query.append("SELECT 0,'" + implementationId + "', t.* FROM "
				+ database + ".role_role AS t ");
		query.append("WHERE CONCAT(t.parent_role, t.child_role) NOT IN (SELECT CONCAT(parent_role, child_role) FROM role_role WHERE implementation_id="
				+ implementationId + ")");
		log.info("Inserting data from " + database
				+ ".role_role into data warehouse");
		dwDb.runCommand(CommandType.INSERT, query.toString());
		query = new StringBuilder();
		query.append("INSERT INTO role_privilege ");
		query.append("SELECT 0,'" + implementationId + "', t.* FROM "
				+ database + ".role_privilege AS t ");
		query.append("WHERE CONCAT(t.role, t.privilege) NOT IN (SELECT CONCAT(role, privilege) FROM role_privilege WHERE implementation_id="
				+ implementationId + ")");
		log.info("Inserting data from " + database
				+ ".role_privilege into data warehouse");
		dwDb.runCommand(CommandType.INSERT, query.toString());
		query = new StringBuilder();
		query.append("INSERT INTO user_property ");
		query.append("SELECT 0,'" + implementationId + "', t.* FROM "
				+ database + ".user_property AS t ");
		query.append("WHERE CONCAT(t.user_id, t.property) NOT IN (SELECT CONCAT(user_id, property) FROM user_role WHERE implementation_id="
				+ implementationId + ")");
		log.info("Inserting data from " + database
				+ ".user_property into data warehouse");
		dwDb.runCommand(CommandType.INSERT, query.toString());
		query = new StringBuilder();
		query.append("INSERT INTO user_role ");
		query.append("SELECT 0,'" + implementationId + "', t.* FROM "
				+ database + ".user_role AS t ");
		query.append("WHERE CONCAT(t.user_id, t.role) NOT IN (SELECT CONCAT(user_id, role) FROM user_role WHERE implementation_id="
				+ implementationId + ")");
		log.info("Inserting data from " + database
				+ ".user_role into data warehouse");
		dwDb.runCommand(CommandType.INSERT, query.toString());
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
			query.append("SELECT 0,'" + implementationId + "', t.* FROM "
					+ database + "." + table + " AS t ");
			query.append("WHERE t.uuid NOT IN (SELECT uuid FROM " + table + ")");
			log.info("Inserting data from " + database + "." + table
					+ " into data warehouse");
			dwDb.runCommand(CommandType.INSERT, query.toString());
		}
		query = new StringBuilder();
		query.append("INSERT INTO location_tag_map ");
		query.append("SELECT 0,'" + implementationId + "', t.* FROM "
				+ database + ".location_tag_map AS t ");
		query.append("WHERE CONCAT(t.location_id, t.location_tag_id) NOT IN (SELECT CONCAT(location_id, location_tag_id) FROM user_role WHERE implementation_id="
				+ implementationId + ")");
		log.info("Inserting data from " + database
				+ ".location_tag_map into data warehouse");
		dwDb.runCommand(CommandType.INSERT, query.toString());
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
		String[] tables = { "concept_class", "concept_set", "concept_datatype",
				"concept_map_type", "concept_stop_word", "concept",
				"concept_name", "concept_description",
				"concept_answer", "concept_reference_map",
				"concept_reference_term", "concept_reference_term_map",
				"concept_reference_source" };
		StringBuilder query = new StringBuilder();
		for (String table : tables) {
			query = new StringBuilder();
			query.append("INSERT INTO " + table + " ");
			query.append("SELECT 0,'" + implementationId + "', t.* FROM "
					+ database + "." + table + " AS t ");
			query.append("WHERE t.uuid NOT IN (SELECT uuid FROM " + table + ")");
			log.info("Inserting data from " + database + "." + table
					+ " into data warehouse");
			dwDb.runCommand(CommandType.INSERT, query.toString());
		}
		query = new StringBuilder();
		query.append("INSERT INTO concept_numeric ");
		query.append("SELECT 0,'" + implementationId + "', t.* FROM "
				+ database + ".concept_numeric AS t ");
		query.append("WHERE t.concept_id NOT IN (SELECT concept_id FROM user_role WHERE implementation_id="
				+ implementationId + ")");
		log.info("Inserting data from " + database
				+ ".concept_numeric into data warehouse");
		dwDb.runCommand(CommandType.INSERT, query.toString());
	}

	/**
	 * Load data from patient-related tables into data warehouse
	 * 
	 * @param implementationId
	 * @param database
	 * @throws ClassNotFoundException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	public void loadPatientData(Integer implementationId, String database)
			throws InstantiationException, IllegalAccessException,
			ClassNotFoundException {
		StringBuilder query = new StringBuilder();
		query.append("INSERT INTO patient ");
		query.append("SELECT 0,'" + implementationId + "', t.* FROM "
				+ database + ".patient AS t ");
		query.append("WHERE t.patient_id NOT IN (SELECT patient_id FROM patient WHERE implementation_id="
				+ implementationId + ") ");
		query.append("AND t.patient_id IN (SELECT person_id FROM person WHERE implementation_id="
				+ implementationId + ")");
		log.info("Inserting data from " + database
				+ ".patient into data warehouse");
		dwDb.runCommand(CommandType.INSERT, query.toString());

		String[] tables = { "patient_identifier_type", "patient_identifier" };
		for (String table : tables) {
			query = new StringBuilder();
			query.append("INSERT INTO " + table + " ");
			query.append("SELECT 0,'" + implementationId + "', t.* FROM "
					+ database + "." + table + " AS t ");
			query.append("WHERE t.uuid NOT IN (SELECT uuid FROM " + table + ")");
			log.info("Inserting data from " + database + "." + table
					+ " into data warehouse");
			dwDb.runCommand(CommandType.INSERT, query.toString());
		}
	}

	/**
	 * Load data from encounter-related tables into data warehouse
	 * 
	 * @param implementationId
	 * @param database
	 * @throws ClassNotFoundException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	public void loadEncounterData(Integer implementationId, String database)
			throws InstantiationException, IllegalAccessException,
			ClassNotFoundException {
		String[] tables = { "encounter_type", "form", "encounter_role",
				"encounter", "encounter_provider", "obs" };
		StringBuilder query = new StringBuilder();
		for (String table : tables) {
			query = new StringBuilder();
			query.append("INSERT INTO " + table + " ");
			query.append("SELECT 0,'" + implementationId + "', t.* FROM "
					+ database + "." + table + " AS t ");
			query.append("WHERE t.uuid NOT IN (SELECT uuid FROM " + table + ")");
			log.info("Inserting data from " + database + "." + table
					+ " into data warehouse");
			dwDb.runCommand(CommandType.INSERT, query.toString());
		}
	}

	/**
	 * Update data from person-related tables into data warehouse
	 * 
	 * @param database
	 * @throws ClassNotFoundException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	public void updatePeopleData(Integer implementationId, String database)
			throws InstantiationException, IllegalAccessException,
			ClassNotFoundException {
		StringBuilder query = new StringBuilder();
		query.append("UPDATE " + database + ".person SET creator = 1 WHERE creator NOT IN (SELECT user_id FROM users)");
		log.info("Updating people data in data warehouse");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE person_attribute_type AS a, " + database + ".person_attribute_type AS t ");
		query.append("SET a.name = t.name, a.description = t.description, a.format = t.format, a.foreign_key = t.foreign_key, a.searchable = t.searchable, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.retired = t.retired, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retire_reason = t.retire_reason, a.edit_privilege = t.edit_privilege, a.sort_weight = t.sort_weight ");
		query.append("WHERE a.person_attribute_type_id = t.person_attribute_type_id AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE person AS a, " + database + ".person AS t ");
		query.append("SET a.gender = t.gender, a.birthdate = t.birthdate, a.birthdate_estimated = t.birthdate_estimated, a.dead = t.dead, a.death_date = t.death_date, a.cause_of_death = t.cause_of_death, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.voided = t.voided, a.voided_by = t.voided_by, a.date_voided = t.date_voided, a.void_reason = t.void_reason ");
		query.append("WHERE a.person_id = t.person_id AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE person_address AS a, " + database + ".person_address AS t ");
		query.append("SET a.person_id = t.person_id, a.preferred = t.preferred, a.address1 = t.address1, a.address2 = t.address2, a.city_village = t.city_village, a.state_province = t.state_province, a.postal_code = t.postal_code, a.country = t.country, a.latitude = t.latitude, a.longitude = t.longitude, a.start_date = t.start_date, a.end_date = t.end_date, a.creator = t.creator, a.date_created = t.date_created, a.voided = t.voided, a.voided_by = t.voided_by, a.date_voided = t.date_voided, a.void_reason = t.void_reason, a.county_district = t.county_district, a.address3 = t.address3, a.address4 = t.address4, a.address5 = t.address5, a.address6 = t.address6, a.date_changed = t.date_changed, a.changed_by = t.changed_by ");
		query.append("WHERE a.person_address_id = t.person_address_id AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE person_attribute AS a, " + database + ".person_attribute AS t ");
		query.append("SET a.person_id = t.person_id, a.value = t.value, a.person_attribute_type_id = t.person_attribute_type_id, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.voided = t.voided, a.voided_by = t.voided_by, a.date_voided = t.date_voided, a.void_reason = t.void_reason ");
		query.append("WHERE a.person_attribute_id = t.person_attribute_id AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE person_name AS a, " + database + ".person_name AS t ");
		query.append("SET a.preferred = t.preferred, a.person_id = t.person_id, a.prefix = t.prefix, a.given_name = t.given_name, a.middle_name = t.middle_name, a.family_name_prefix = t.family_name_prefix, a.family_name = t.family_name, a.family_name2 = t.family_name2, a.family_name_suffix = t.family_name_suffix, a.degree = t.degree, a.creator = t.creator, a.date_created = t.date_created, a.voided = t.voided, a.voided_by = t.voided_by, a.date_voided = t.date_voided, a.void_reason = t.void_reason, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.uuid = t.uuid ");
		query.append("WHERE a.person_name_id = t.person_name_id AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
	}
	
	/**
	 * Update data from user-related tables into data warehouse
	 * 
	 * @param implementationId
	 * @param database
	 * @throws ClassNotFoundException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	public void updateUserData(Integer implementationId, String database)
			throws InstantiationException, IllegalAccessException,
			ClassNotFoundException {
		StringBuilder query = new StringBuilder();
		query.append("UPDATE role AS a, " + database + ".role AS t ");
		query.append("SET a.description = t.description ");
		query.append("WHERE a.role = t.role AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE users AS a, " + database + ".users AS t ");
		query.append("SET a.system_id = t.system_id, a.username = t.username, a.password = t.password, a.salt = t.salt, a.secret_question = t.secret_question, a.secret_answer = t.secret_answer, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.person_id = t.person_id, a.retired = t.retired, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retire_reason = t.retire_reason ");
		query.append("WHERE a.user_id = t.user_id AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE user_property AS a, " + database + ".user_property AS t ");
		query.append("SET a.property_value = t.property_value ");
		query.append("WHERE a.user_id = t.user_id AND a.property = t.property AND a.implementation_id = " + implementationId);
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE provider AS a, " + database + ".provider AS t ");
		query.append("SET a.person_id = t.person_id, a.name = t.name, a.identifier = t.identifier, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.retired = t.retired, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retire_reason = t.retire_reason ");
		query.append("WHERE a.provider_id = t.provider_id AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
	}
	
	/**
	 * Update data from location-related tables into data warehouse
	 * 
	 * @param database
	 * @throws ClassNotFoundException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	public void updateLocationData(Integer implementationId, String database)
			throws InstantiationException, IllegalAccessException,
			ClassNotFoundException {
		StringBuilder query = new StringBuilder();
		query.append("UPDATE location_attribute_type AS a, " + database + ".location_attribute_type AS t ");
		query.append("SET a.name = t.name, a.description = t.description, a.datatype = t.datatype, a.datatype_config = t.datatype_config, a.preferred_handler = t.preferred_handler, a.handler_config = t.handler_config, a.min_occurs = t.min_occurs, a.max_occurs = t.max_occurs, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.retired = t.retired, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retire_reason = t.retire_reason ");
		query.append("WHERE a.location_attribute_type_id = t.location_attribute_type_id AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE location AS a, " + database + ".location AS t ");
		query.append("SET a.name = t.name, a.description = t.description, a.address1 = t.address1, a.address2 = t.address2, a.city_village = t.city_village, a.state_province = t.state_province, a.postal_code = t.postal_code, a.country = t.country, a.latitude = t.latitude, a.longitude = t.longitude, a.creator = t.creator, a.date_created = t.date_created, a.county_district = t.county_district, a.address3 = t.address3, a.address4 = t.address4, a.address5 = t.address5, a.address6 = t.address6, a.retired = t.retired, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retire_reason = t.retire_reason, a.parent_location = t.parent_location ");
		query.append("WHERE a.location_id = t.location_id AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE location_attribute AS a, " + database + ".location_attribute AS t ");
		query.append("SET a.attribute_type_id = t.attribute_type_id, a.value_reference = t.value_reference, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.voided = t.voided, a.voided_by = t.voided_by, a.date_voided = t.date_voided, a.void_reason = t.void_reason ");
		query.append("WHERE a.location_attribute_id = t.location_attribute_id AND a.location_id = t.location_id AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
	}
	
	/**
	 * Update data from concept-related tables into data warehouse
	 * 
	 * @param implementationId
	 * @param database
	 * @throws ClassNotFoundException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	public void updateConceptData(Integer implementationId, String database)
			throws InstantiationException, IllegalAccessException,
			ClassNotFoundException {
		StringBuilder query = new StringBuilder();
		query.append("UPDATE concept_class AS a, " + database + ".concept_class AS t ");
		query.append("SET a.name = t.name, a.description = t.description, a.creator = t.creator, a.date_created = t.date_created, a.retired = t.retired, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retire_reason = t.retire_reason ");
		query.append("WHERE a.concept_class_id = t.concept_class_id AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE concept_datatype AS a, " + database + ".concept_datatype AS t ");
		query.append("SET a.name = t.name, a.hl7_abbreviation = t.hl7_abbreviation, a.description = t.description, a.creator = t.creator, a.date_created = t.date_created, a.retired = t.retired, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retire_reason = t.retire_reason ");
		query.append("WHERE a.concept_datatype_id = t.concept_datatype_id AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE concept_map_type AS a, " + database + ".concept_map_type AS t ");
		query.append("SET a.name = t.name, a.description = t.description, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.is_hidden = t.is_hidden, a.retired = t.retired, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retire_reason = t.retire_reason ");
		query.append("WHERE a.concept_map_type_id = t.concept_map_type_id AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE concept AS a, " + database + ".concept AS t ");
		query.append("SET a.retired = t.retired, a.short_name = t.short_name, a.description = t.description, a.form_text = t.form_text, a.datatype_id = t.datatype_id, a.class_id = t.class_id, a.is_set = t.is_set, a.creator = t.creator, a.date_created = t.date_created, a.version = t.version, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retire_reason = t.retire_reason ");
		query.append("WHERE a.concept_id = t.concept_id AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE concept_name AS a, " + database + ".concept_name AS t ");
		query.append("SET a.concept_id = t.concept_id, a.name = t.name, a.locale = t.locale, a.locale_preferred = t.locale_preferred, a.creator = t.creator, a.date_created = t.date_created, a.concept_name_type = t.concept_name_type, a.voided = t.voided, a.voided_by = t.voided_by, a.date_voided = t.date_voided, a.void_reason = t.void_reason ");
		query.append("WHERE a.concept_name_id = t.concept_name_id AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE concept_numeric AS a, " + database + ".concept_numeric AS t ");
		query.append("SET a.hi_absolute = t.hi_absolute, a.hi_critical = t.hi_critical, a.hi_normal = t.hi_normal, a.low_absolute = t.low_absolute, a.low_critical = t.low_critical, a.low_normal = t.low_normal, a.units = t.units, a.precise = t.precise ");
		query.append("WHERE a.concept_id = t.concept_id AND a.implementation_id=" + implementationId);
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE concept_description AS a, " + database + ".concept_description AS t ");
		query.append("SET a.concept_id = t.concept_id, a.description = t.description, a.locale = t.locale, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed ");
		query.append("WHERE a.concept_description_id = t.concept_description_id AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE concept_answer AS a, " + database + ".concept_answer AS t ");
		query.append("SET a.concept_id = t.concept_id, a.answer_concept = t.answer_concept, a.answer_drug = t.answer_drug, a.creator = t.creator, a.date_created = t.date_created, a.sort_weight = t.sort_weight ");
		query.append("WHERE a.concept_answer_id = t.concept_answer_id AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE concept_reference_map AS a, " + database + ".concept_reference_map AS t ");
		query.append("SET a.concept_reference_term_id = t.concept_reference_term_id, a.concept_map_type_id = t.concept_map_type_id, a.creator = t.creator, a.date_created = t.date_created, a.concept_id = t.concept_id, a.changed_by = t.changed_by, a.date_changed = t.date_changed ");
		query.append("WHERE a.concept_map_id = t.concept_map_id AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE concept_reference_term AS a, " + database + ".concept_reference_term AS t ");
		query.append("SET a.concept_source_id = t.concept_source_id, a.name = t.name, a.code = t.code, a.version = t.version, a.description = t.description, a.creator = t.creator, a.date_created = t.date_created, a.date_changed = t.date_changed, a.changed_by = t.changed_by, a.retired = t.retired, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retire_reason = t.retire_reason ");
		query.append("WHERE a.concept_reference_term_id = t.concept_reference_term_id AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
	}

	/**
	 * Update data from patient-related tables into data warehouse
	 * 
	 * @param implementationId
	 * @param database
	 * @throws ClassNotFoundException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	public void updatePatientData(Integer implementationId, String database)
			throws InstantiationException, IllegalAccessException,
			ClassNotFoundException {
		StringBuilder query = new StringBuilder();
		query.append("UPDATE patient AS a, " + database + ".patient AS t ");
		query.append("SET a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.voided = t.voided, a.voided_by = t.voided_by, a.date_voided = t.date_voided, a.void_reason = t.void_reason ");
		query.append("WHERE a.patient_id = t.patient_id and a.implementation_id = " + implementationId);
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE patient_identifier AS a, " + database + ".patient_identifier AS t ");
		query.append("SET a.patient_id = t.patient_id, a.identifier = t.identifier, a.identifier_type = t.identifier_type, a.preferred = t.preferred, a.location_id = t.location_id, a.creator = t.creator, a.date_created = t.date_created, a.date_changed = t.date_changed, a.changed_by = t.changed_by, a.voided = t.voided, a.voided_by = t.voided_by, a.date_voided = t.date_voided, a.void_reason = t.void_reason ");
		query.append("WHERE a.patient_identifier_id = t.patient_identifier_id AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
	}
	
	/**
	 * Update data from encounters-related tables into data warehouse
	 * 
	 * @param implementationId
	 * @param database
	 * @throws ClassNotFoundException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	public void updateEncounterData(Integer implementationId, String database)
			throws InstantiationException, IllegalAccessException,
			ClassNotFoundException {
		StringBuilder query = new StringBuilder();
		query.append("UPDATE encounter_type AS a, " + database + ".encounter_type AS t ");
		query.append("SET a.name = t.name, a.description = t.description, a.creator = t.creator, a.date_created = t.date_created, a.retired = t.retired, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retire_reason = t.retire_reason ");
		query.append("WHERE a.encounter_type_id = t.encounter_type_id AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE form AS a, " + database + ".form AS t ");
		query.append("SET a.name = t.name, a.version = t.version, a.build = t.build, a.published = t.published, a.xslt = t.xslt, a.template = t.template, a.description = t.description, a.encounter_type = t.encounter_type, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.retired = t.retired, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retired_reason = t.retired_reason ");
		query.append("WHERE a.form_id = t.form_id AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE encounter_role AS a, " + database + ".encounter_role AS t ");
		query.append("SET a.name = t.name, a.description = t.description, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.retired = t.retired, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retire_reason = t.retire_reason ");
		query.append("WHERE a.encounter_role_id = t.encounter_role_id AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE encounter AS a, " + database + ".encounter AS t ");
		query.append("SET a.encounter_type = t.encounter_type, a.patient_id = t.patient_id, a.location_id = t.location_id, a.form_id = t.form_id, a.encounter_datetime = t.encounter_datetime, a.creator = t.creator, a.date_created = t.date_created, a.voided = t.voided, a.voided_by = t.voided_by, a.date_voided = t.date_voided, a.void_reason = t.void_reason, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.visit_id = t.visit_id ");
		query.append("WHERE a.encounter_id = t.encounter_id AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE encounter_provider AS a, " + database + ".encounter_provider AS t ");
		query.append("SET a.encounter_id = t.encounter_id, a.provider_id = t.provider_id, a.encounter_role_id = t.encounter_role_id, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.voided = t.voided, a.date_voided = t.date_voided, a.voided_by = t.voided_by, a.void_reason = t.void_reason ");
		query.append("WHERE a.encounter_provider_id = t.encounter_provider_id AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE obs AS a, " + database + ".obs AS t ");
		query.append("SET a.person_id = t.person_id, a.concept_id = t.concept_id, a.encounter_id = t.encounter_id, a.order_id = t.order_id, a.obs_datetime = t.obs_datetime, a.location_id = t.location_id, a.obs_group_id = t.obs_group_id, a.accession_number = t.accession_number, a.value_group_id = t.value_group_id, a.value_boolean = t.value_boolean, a.value_coded = t.value_coded, a.value_coded_name_id = t.value_coded_name_id, a.value_drug = t.value_drug, a.value_datetime = t.value_datetime, a.value_numeric = t.value_numeric, a.value_modifier = t.value_modifier, a.value_text = t.value_text, a.value_complex = t.value_complex, a.comments = t.comments, a.creator = t.creator, a.date_created = t.date_created, a.voided = t.voided, a.voided_by = t.voided_by, a.date_voided = t.date_voided, a.void_reason = t.void_reason, a.previous_version = t.previous_version ");
		query.append("WHERE a.obs_id = t.obs_id AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
	}
}
