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
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.logging.Logger;

import com.ihsinformatics.util.CommandType;
import com.ihsinformatics.util.DatabaseUtil;
import com.ihsinformatics.util.DateTimeUtil;
import com.ihsinformatics.util.FileUtil;

/**
 * @author owais.hussain@ihsinformatics.com
 *
 */
public class GfatmSyncMain {

	public static final Logger log = Logger.getLogger(Class.class.getName());
	public static final String propertiesFile = "res/gfatm-sync.properties";
	public static final String createWarehouseFile = "res/create_datawarehouse.sql";
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
		//gfatm.loadData();
		gfatm.dimensionModeling(true);
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
			InputStream propFile = new FileInputStream(propertiesFile);
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
			log.severe("Properties file not found in class path.");
		}
	}

	public void dimensionModeling(boolean fromScratch) {
		if (fromScratch) {
			FileUtil fu = new FileUtil();
			String[] queries = fu.getLines(createWarehouseFile);
			for (String query : queries) {
				try {
					if (query.startsWith("DROP TABLE")) {
						dwDb.runCommand(CommandType.DROP, query);
					} else if (query.startsWith("CREATE TABLE")) {
						dwDb.runCommand(CommandType.CREATE, query);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		try {
			StringBuilder query = new StringBuilder();
			// Fill in datetime dimension table
			// Detect the last date in dimension and begin from there
			Object lastSqlDate = dwDb.runCommand(CommandType.SELECT, "select max(full_date) as latest from dim_datetime");
			Calendar start = Calendar.getInstance();
			start.set(Calendar.YEAR, 2000);
			start.set(Calendar.MONTH, Calendar.JANUARY);
			start.set(Calendar.DATE, 1);
			if (lastSqlDate != null) {
				Date latestDate = DateTimeUtil.getDateFromString(lastSqlDate.toString(), DateTimeUtil.SQL_DATETIME);
				start.setTime(latestDate);
			}
			start.add(Calendar.DATE, 1);
			Calendar end = Calendar.getInstance();
			end.set(Calendar.HOUR, 0);
			query = new StringBuilder("insert into dim_datetime values ");
			while (start.getTime().before(end.getTime())) {
				String sqlDate = "'" + DateTimeUtil.getSqlDate(start.getTime()) + "'";
				query.append("(0, " + sqlDate + ", ");
				query.append("year(" + sqlDate + "), ");
				query.append("month(" + sqlDate + "), ");
				query.append("day(" + sqlDate + "), ");
				query.append("dayname(" + sqlDate + "), ");
				query.append("monthname(" + sqlDate + ")),");
				start.add(Calendar.DATE, 1);
			}
			query.setCharAt(query.length() - 1, ';');
			dwDb.runCommand(CommandType.INSERT, query.toString());

			// Fill the concept dimension data
			query = new StringBuilder();
			query.append("insert ignore into dim_concept (surrogate_id, implementation_id, concept_id, full_name, concept, description, retired, data_type, class, creator, date_created, version, changed_by, date_changed, uuid) ");
			query.append("select c.surrogate_id, c.implementation_id, c.concept_id, n1.name as full_name, n2.name as concept, d.description, c.retired, dt.name as data_type, cl.name as class, c.creator, c.date_created, c.version, c.changed_by, c.date_changed, c.uuid from concept as c ");
			query.append("left outer join concept_datatype as dt on dt.implementation_id = c.implementation_id and dt.concept_datatype_id = c.datatype_id ");
			query.append("left outer join concept_class as cl on cl.implementation_id = c.implementation_id and cl.concept_class_id = c.class_id ");
			query.append("left outer join concept_name as n1 on n1.implementation_id = c.implementation_id and n1.concept_id = c.concept_id and n1.locale = 'en' and n1.voided = 0 and n1.concept_name_type = 'FULLY_SPECIFIED' ");
			query.append("left outer join concept_name as n2 on n2.implementation_id = c.implementation_id and n2.concept_id = c.concept_id and n2.locale = 'en' and n2.voided = 0 and n2.concept_name_type <> 'FULLY_SPECIFIED' ");
			query.append("left outer join concept_description as d on d.implementation_id = c.implementation_id and d.concept_id = c.concept_id and d.locale = 'en'");
			log.info("Inserting new concepts to dimension.");
			dwDb.runCommand(CommandType.INSERT, query.toString());
			query = new StringBuilder(
					"update dim_concept set full_name = 'Yes', concept = 'Yes' where concept_id = 1");
			log.info("Setting names of Yes/No concepts.");
			dwDb.runCommand(CommandType.UPDATE, query.toString());
			query = new StringBuilder(
					"update dim_concept set full_name = 'No', concept = 'No' where concept_id = 2");
			dwDb.runCommand(CommandType.UPDATE, query.toString());

			// Fill the location dimension data
			query = new StringBuilder("insert ignore into dim_location (surrogate_id, system_id, location_id, location_name, description, address1, address2, city_village, state_province, postal_code, country, latitude, longitude, creator, date_created, etired, parent_location, uuid) ");
			query.append("select l.surrogate_id, l.implementation_id, l.location_id, l.name as location_name, l.description, l.address1, l.address2, l.city_village, l.state_province, l.postal_code, l.country, l.latitude, l.longitude, l.creator, l.date_created, l.retired, l.parent_location, l.uuid from location as l ");
			log.info("Inserting new locations to dimension.");
			dwDb.runCommand(CommandType.INSERT, query.toString());

			// Fill the user dimension data
			query = new StringBuilder("insert ignore into dim_user (surrogate_id, implementation_id, user_id, username, person_id, identifier, secret_question, secret_answer, creator, date_created, changed_by, date_changed, retired, retire_reason, uuid) ");
			query.append("select u.surrogate_id, u.implementation_id, u.user_id, u.username, u.person_id, p.identifier, u.secret_question, u.secret_answer, u.creator, u.date_created, u.changed_by, u.date_changed, u.retired, u.retire_reason, u.uuid from users as u ");
			query.append("left outer join provider as p on p.implementation_id = u.implementation_id and p.person_id = u.person_id");
			log.info("Inserting new users to dimension.");
			dwDb.runCommand(CommandType.INSERT, query.toString());

			// Collect latest patient identifiers
			log.info("Setting preferred identifiers to OpenMRS ID (with check digit).");
			dwDb.runCommand(CommandType.UPDATE, "update patient_identifier set preferred = 1 where identifier_type = 3");
			log.info("Setting identifiers other than OpenMRS ID to non-preferred.");
			dwDb.runCommand(CommandType.UPDATE, "update patient_identifier set preferred = 0 where identifier_type <> 3");
			log.info("Selecting patient identifiers.");
			dwDb.runCommand(CommandType.DROP, "drop table if exists patient_latest_identifier");
			query = new StringBuilder("create table patient_latest_identifier ");
			query.append("select * from patient_identifier as a having a.patient_id = (select max(patient_id) from patient_identifier where implementation_id = a.implementation_id and patient_id = a.patient_id and preferred = 1 and voided = 0) union ");
			query.append("select * from patient_identifier as a having a.patient_id = (select max(patient_id) from patient_identifier where implementation_id = a.implementation_id and patient_id = a.patient_id and preferred = 0 and voided = 0)");
			dwDb.runCommand(CommandType.CREATE, query.toString());
			dwDb.runCommand(CommandType.ALTER, "alter table patient_latest_identifier add primary key surrogate_id (surrogate_id)");

			// Collect latest person names
			log.info("Selecting people names (preferred/latest).");
			dwDb.runCommand(CommandType.DROP, "drop table if exists person_latest_name");
			query = new StringBuilder("create table person_latest_name ");
			query.append("select * from person_name as a where a.person_name_id = (select max(person_name_id) from person_name where implementation_id = a.implementation_id and person_id = a.person_id and preferred = 1)");
			dwDb.runCommand(CommandType.CREATE, query.toString());
			query = new StringBuilder("insert into person_latest_name ");
			query.append("select * from person_name as a where a.person_name_id = (select max(person_name_id) from person_name where implementation_id = a.implementation_id and person_id = a.person_id and preferred = 0)");
			dwDb.runCommand(CommandType.INSERT, query.toString());
			dwDb.runCommand(CommandType.ALTER, "alter table person_latest_name add primary key surrogate_id (surrogate_id)");

			// Collect latest person addresses
			log.info("Selecting people addresses (preferred/latest).");
			dwDb.runCommand(CommandType.DROP, "drop table if exists person_latest_address");
			query = new StringBuilder("create table person_latest_address ");
			query.append("select * from person_address as a where a.person_address_id = (select max(person_address_id) from person_address where implementation_id = a.implementation_id and person_id = a.person_id and preferred = 1)");
			dwDb.runCommand(CommandType.CREATE, query.toString());
			query = new StringBuilder("insert into person_latest_address ");
			query.append("select * from person_address as a where a.person_address_id = (select max(person_address_id) from person_address where implementation_id = a.implementation_id and person_id = a.person_id and preferred = 0)");
			dwDb.runCommand(CommandType.INSERT, query.toString());
			dwDb.runCommand(CommandType.ALTER, "alter table person_latest_address add primary key surrogate_id (surrogate_id)");

			// Recreate person attributes
			log.info("Transforming people attribute.");
			dwDb.runCommand(CommandType.DROP, "drop table if exists person_attribute_merged");
			Object[][] attributeTypes = dwDb.getTableData("person_attribute_type", "person_attribute_type_id,name", null, true);
			StringBuilder groupConcat = new StringBuilder();
			for (Object[] type : attributeTypes) {
				String typeId = type[0].toString();
				String typeName = type[1].toString().replace(" ", "_").replace("'", "").replace("(\\W|^_)*", "_").toLowerCase();
				groupConcat.append("group_concat(if(a.person_attribute_type_id = " + typeId + ", a.value, null)) as " + typeName + ", ");
			}
			groupConcat.append("'' as BLANK ");
			query = new StringBuilder("create table person_attribute_merged ");
			query.append("select a.implementation_id, a.person_id, a.creator, a.date_created, a.uuid, ");
			query.append(groupConcat.toString());
			query.append("from person_attribute as a where a.voided = 0 ");
			query.append("group by a.implementation_id, a.person_id");
			dwDb.runCommand(CommandType.CREATE, query.toString());
			dwDb.runCommand(CommandType.ALTER, "alter table person_attribute_merged add primary key (implementation_id, person_id)");

			log.info("Finished dimension modeling");

		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (ParseException e) {
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
				"concept_name", "concept_description", "concept_answer",
				"concept_reference_map", "concept_reference_term",
				"concept_reference_term_map", "concept_reference_source" };
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
		query.append("UPDATE "
				+ database
				+ ".person SET creator = 1 WHERE creator NOT IN (SELECT user_id FROM users)");
		log.info("Updating people data in data warehouse");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE person_attribute_type AS a, " + database
				+ ".person_attribute_type AS t ");
		query.append("SET a.name = t.name, a.description = t.description, a.format = t.format, a.foreign_key = t.foreign_key, a.searchable = t.searchable, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.retired = t.retired, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retire_reason = t.retire_reason, a.edit_privilege = t.edit_privilege, a.sort_weight = t.sort_weight ");
		query.append("WHERE a.person_attribute_type_id = t.person_attribute_type_id AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE person AS a, " + database + ".person AS t ");
		query.append("SET a.gender = t.gender, a.birthdate = t.birthdate, a.birthdate_estimated = t.birthdate_estimated, a.dead = t.dead, a.death_date = t.death_date, a.cause_of_death = t.cause_of_death, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.voided = t.voided, a.voided_by = t.voided_by, a.date_voided = t.date_voided, a.void_reason = t.void_reason ");
		query.append("WHERE a.person_id = t.person_id AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE person_address AS a, " + database
				+ ".person_address AS t ");
		query.append("SET a.person_id = t.person_id, a.preferred = t.preferred, a.address1 = t.address1, a.address2 = t.address2, a.city_village = t.city_village, a.state_province = t.state_province, a.postal_code = t.postal_code, a.country = t.country, a.latitude = t.latitude, a.longitude = t.longitude, a.start_date = t.start_date, a.end_date = t.end_date, a.creator = t.creator, a.date_created = t.date_created, a.voided = t.voided, a.voided_by = t.voided_by, a.date_voided = t.date_voided, a.void_reason = t.void_reason, a.county_district = t.county_district, a.address3 = t.address3, a.address4 = t.address4, a.address5 = t.address5, a.address6 = t.address6, a.date_changed = t.date_changed, a.changed_by = t.changed_by ");
		query.append("WHERE a.person_address_id = t.person_address_id AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE person_attribute AS a, " + database
				+ ".person_attribute AS t ");
		query.append("SET a.person_id = t.person_id, a.value = t.value, a.person_attribute_type_id = t.person_attribute_type_id, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.voided = t.voided, a.voided_by = t.voided_by, a.date_voided = t.date_voided, a.void_reason = t.void_reason ");
		query.append("WHERE a.person_attribute_id = t.person_attribute_id AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE person_name AS a, " + database
				+ ".person_name AS t ");
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
		query.append("UPDATE user_property AS a, " + database
				+ ".user_property AS t ");
		query.append("SET a.property_value = t.property_value ");
		query.append("WHERE a.user_id = t.user_id AND a.property = t.property AND a.implementation_id = "
				+ implementationId);
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
		query.append("UPDATE location_attribute_type AS a, " + database
				+ ".location_attribute_type AS t ");
		query.append("SET a.name = t.name, a.description = t.description, a.datatype = t.datatype, a.datatype_config = t.datatype_config, a.preferred_handler = t.preferred_handler, a.handler_config = t.handler_config, a.min_occurs = t.min_occurs, a.max_occurs = t.max_occurs, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.retired = t.retired, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retire_reason = t.retire_reason ");
		query.append("WHERE a.location_attribute_type_id = t.location_attribute_type_id AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE location AS a, " + database + ".location AS t ");
		query.append("SET a.name = t.name, a.description = t.description, a.address1 = t.address1, a.address2 = t.address2, a.city_village = t.city_village, a.state_province = t.state_province, a.postal_code = t.postal_code, a.country = t.country, a.latitude = t.latitude, a.longitude = t.longitude, a.creator = t.creator, a.date_created = t.date_created, a.county_district = t.county_district, a.address3 = t.address3, a.address4 = t.address4, a.address5 = t.address5, a.address6 = t.address6, a.retired = t.retired, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retire_reason = t.retire_reason, a.parent_location = t.parent_location ");
		query.append("WHERE a.location_id = t.location_id AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE location_attribute AS a, " + database
				+ ".location_attribute AS t ");
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
		query.append("UPDATE concept_class AS a, " + database
				+ ".concept_class AS t ");
		query.append("SET a.name = t.name, a.description = t.description, a.creator = t.creator, a.date_created = t.date_created, a.retired = t.retired, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retire_reason = t.retire_reason ");
		query.append("WHERE a.concept_class_id = t.concept_class_id AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE concept_datatype AS a, " + database
				+ ".concept_datatype AS t ");
		query.append("SET a.name = t.name, a.hl7_abbreviation = t.hl7_abbreviation, a.description = t.description, a.creator = t.creator, a.date_created = t.date_created, a.retired = t.retired, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retire_reason = t.retire_reason ");
		query.append("WHERE a.concept_datatype_id = t.concept_datatype_id AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE concept_map_type AS a, " + database
				+ ".concept_map_type AS t ");
		query.append("SET a.name = t.name, a.description = t.description, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.is_hidden = t.is_hidden, a.retired = t.retired, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retire_reason = t.retire_reason ");
		query.append("WHERE a.concept_map_type_id = t.concept_map_type_id AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE concept AS a, " + database + ".concept AS t ");
		query.append("SET a.retired = t.retired, a.short_name = t.short_name, a.description = t.description, a.form_text = t.form_text, a.datatype_id = t.datatype_id, a.class_id = t.class_id, a.is_set = t.is_set, a.creator = t.creator, a.date_created = t.date_created, a.version = t.version, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retire_reason = t.retire_reason ");
		query.append("WHERE a.concept_id = t.concept_id AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE concept_name AS a, " + database
				+ ".concept_name AS t ");
		query.append("SET a.concept_id = t.concept_id, a.name = t.name, a.locale = t.locale, a.locale_preferred = t.locale_preferred, a.creator = t.creator, a.date_created = t.date_created, a.concept_name_type = t.concept_name_type, a.voided = t.voided, a.voided_by = t.voided_by, a.date_voided = t.date_voided, a.void_reason = t.void_reason ");
		query.append("WHERE a.concept_name_id = t.concept_name_id AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE concept_numeric AS a, " + database
				+ ".concept_numeric AS t ");
		query.append("SET a.hi_absolute = t.hi_absolute, a.hi_critical = t.hi_critical, a.hi_normal = t.hi_normal, a.low_absolute = t.low_absolute, a.low_critical = t.low_critical, a.low_normal = t.low_normal, a.units = t.units, a.precise = t.precise ");
		query.append("WHERE a.concept_id = t.concept_id AND a.implementation_id="
				+ implementationId);
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE concept_description AS a, " + database
				+ ".concept_description AS t ");
		query.append("SET a.concept_id = t.concept_id, a.description = t.description, a.locale = t.locale, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed ");
		query.append("WHERE a.concept_description_id = t.concept_description_id AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE concept_answer AS a, " + database
				+ ".concept_answer AS t ");
		query.append("SET a.concept_id = t.concept_id, a.answer_concept = t.answer_concept, a.answer_drug = t.answer_drug, a.creator = t.creator, a.date_created = t.date_created, a.sort_weight = t.sort_weight ");
		query.append("WHERE a.concept_answer_id = t.concept_answer_id AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE concept_reference_map AS a, " + database
				+ ".concept_reference_map AS t ");
		query.append("SET a.concept_reference_term_id = t.concept_reference_term_id, a.concept_map_type_id = t.concept_map_type_id, a.creator = t.creator, a.date_created = t.date_created, a.concept_id = t.concept_id, a.changed_by = t.changed_by, a.date_changed = t.date_changed ");
		query.append("WHERE a.concept_map_id = t.concept_map_id AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE concept_reference_term AS a, " + database
				+ ".concept_reference_term AS t ");
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
		query.append("WHERE a.patient_id = t.patient_id and a.implementation_id = "
				+ implementationId);
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE patient_identifier AS a, " + database
				+ ".patient_identifier AS t ");
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
		query.append("UPDATE encounter_type AS a, " + database
				+ ".encounter_type AS t ");
		query.append("SET a.name = t.name, a.description = t.description, a.creator = t.creator, a.date_created = t.date_created, a.retired = t.retired, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retire_reason = t.retire_reason ");
		query.append("WHERE a.encounter_type_id = t.encounter_type_id AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE form AS a, " + database + ".form AS t ");
		query.append("SET a.name = t.name, a.version = t.version, a.build = t.build, a.published = t.published, a.xslt = t.xslt, a.template = t.template, a.description = t.description, a.encounter_type = t.encounter_type, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.retired = t.retired, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retired_reason = t.retired_reason ");
		query.append("WHERE a.form_id = t.form_id AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE encounter_role AS a, " + database
				+ ".encounter_role AS t ");
		query.append("SET a.name = t.name, a.description = t.description, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.retired = t.retired, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retire_reason = t.retire_reason ");
		query.append("WHERE a.encounter_role_id = t.encounter_role_id AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE encounter AS a, " + database + ".encounter AS t ");
		query.append("SET a.encounter_type = t.encounter_type, a.patient_id = t.patient_id, a.location_id = t.location_id, a.form_id = t.form_id, a.encounter_datetime = t.encounter_datetime, a.creator = t.creator, a.date_created = t.date_created, a.voided = t.voided, a.voided_by = t.voided_by, a.date_voided = t.date_voided, a.void_reason = t.void_reason, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.visit_id = t.visit_id ");
		query.append("WHERE a.encounter_id = t.encounter_id AND a.uuid = t.uuid");
		dwDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE encounter_provider AS a, " + database
				+ ".encounter_provider AS t ");
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
