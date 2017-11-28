/* Copyright(C) 2016 Interactive Health Solutions, Pvt. Ltd.

This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License as
published by the Free Software Foundation; either version 3 of the License (GPLv3), or any later version.
This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

See the GNU General Public License for more details. You should have received a copy of the GNU General Public License along with this program; if not, write to the Interactive Health Solutions, info@ihsinformatics.com
You can also access the license on the internet at the address: http://www.gnu.org/licenses/gpl-3.0.html

Interactive Health Solutions, hereby disclaims all copyright interest in this program written by the contributors.
 */
package com.ihsinformatics.gfatmimport;

import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.logging.Logger;

import com.ihsinformatics.util.CommandType;
import com.ihsinformatics.util.DatabaseUtil;

/**
 * @author owais.hussain@ihsinformatics.com
 *
 */
public class OpenMrsImportController extends AbstractImportController {

	private static final Logger log = Logger.getLogger(Class.class.getName());

	public OpenMrsImportController(DatabaseUtil sourceDb, DatabaseUtil targetDb) {
		this.sourceDb = sourceDb;
		this.targetDb = targetDb;
		this.fromDate = new Date();
		this.toDate = new Date();
	}

	public OpenMrsImportController(DatabaseUtil sourceDb,
			DatabaseUtil targetDb, Date fromDate, Date toDate) {
		this.sourceDb = sourceDb;
		this.targetDb = targetDb;
		this.fromDate = fromDate;
		this.toDate = toDate;
	}

	/**
	 * Insert data from all sources into data warehouse.
	 * 
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 * @throws ParseException
	 */
	public void importData(int implementationId) throws InstantiationException,
			IllegalAccessException, ClassNotFoundException, SQLException,
			ParseException {
		sourceDb.getConnection();
		// Import data from this connection into data warehouse
		try {
			// Update status of implementation record
			log.info("Cleaning temporary tables...");
			clearTempTables(implementationId);
			log.info("Importing people data...");
			importPeopleData(sourceDb, implementationId);
			log.info("Importing user data...");
			importUserData(sourceDb, implementationId);
			log.info("Importing location data...");
			importLocationData(sourceDb, implementationId);
			log.info("Importing concept data...");
			importConceptData(sourceDb, implementationId);
			log.info("Importing patient data...");
			importPatientData(sourceDb, implementationId);
			log.info("Importing encounter data...");
			importEncounterData(sourceDb, implementationId);
			log.info("Importing visit data...");
			importVisitData(sourceDb, implementationId);
			log.info("Importing forms data...");
			importFormData(sourceDb, implementationId);
			log.info("Import complete");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Remove all data from temporary tables related to given implementation ID
	 * 
	 * @param implementationId
	 */
	private void clearTempTables(int implementationId) {
		String[] tables = { "tmp_person", "tmp_person_attribute",
				"tmp_person_attribute_type", "tmp_person_address",
				"tmp_person_name", "tmp_role", "tmp_role_role",
				"tmp_privilege", "tmp_role_privilege", "tmp_users",
				"tmp_user_property", "tmp_user_role",
				"tmp_provider_attribute_type", "tmp_provider",
				"tmp_provider_attribute", "tmp_location_attribute_type",
				"tmp_location", "tmp_location_attribute", "tmp_location_tag",
				"tmp_location_tag_map", "tmp_concept_class", "tmp_concept_set",
				"tmp_concept_datatype", "tmp_concept_map_type", "tmp_concept",
				"tmp_concept_name", "tmp_concept_description",
				"tmp_concept_answer", "tmp_concept_numeric",
				"tmp_patient_identifier_type", "tmp_patient",
				"tmp_patient_identifier", "tmp_patient_program",
				"tmp_encounter_type", "tmp_form", "tmp_encounter_role",
				"tmp_encounter", "tmp_encounter_provider", "tmp_obs",
				"tmp_visit_type", "tmp_visit_attribute_type",
				"tmp_visit_attribute", "tmp_field", "tmp_field_answer",
				"tmp_field_type", "tmp_form_field" };
		for (String table : tables) {
			try {
				targetDb.runCommandWithException(CommandType.TRUNCATE,
						"TRUNCATE TABLE " + table);
			} catch (SQLException e) {
				log.warning("Table: " + table + " not found in data warehouse!");
			} catch (Exception e) {
				log.warning("Table: " + table + " not found in data warehouse!");
			}
		}
	}

	/**
	 * Load data from person-related tables into data warehouse
	 * 
	 * @param remoteDb
	 * @param implementationId
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 */
	public void importPeopleData(DatabaseUtil remoteDb, int implementationId)
			throws InstantiationException, IllegalAccessException,
			ClassNotFoundException {
		String database = remoteDb.getDbName();
		String insertQuery;
		String updateQuery;
		String selectQuery;
		String tableName;
		try {
			tableName = "person";
			// insert into temp_person table...
			insertQuery = "INSERT  INTO tmp_"
					+ tableName
					+ " (surrogate_id, implementation_id, person_id, gender, birthdate, birthdate_estimated, dead, death_date, cause_of_death, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'"
					+ implementationId
					+ "', person_id, gender, birthdate, birthdate_estimated, dead, death_date, cause_of_death, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason, uuid FROM "
					+ database + "." + tableName + " AS t "
					+ filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database + "." + tableName
					+ " into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery,
					remoteDb.getConnection(), targetDb.getConnection());
			// Insert new records
			insertQuery = "INSERT IGNORE INTO "
					+ tableName
					+ " SELECT * FROM tmp_"
					+ tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM "
					+ tableName
					+ " WHERE implementation_id = t.implementation_id AND person_id = t.person_id)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			// Update the existing records
			updateQuery = "UPDATE "
					+ tableName
					+ " AS a, tmp_"
					+ tableName
					+ " AS t SET a.gender = t.gender, a.birthdate = t.birthdate, a.birthdate_estimated = t.birthdate_estimated, a.dead = t.dead, a.death_date = t.death_date, a.cause_of_death = t.cause_of_death, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.voided = t.voided, a.voided_by = t.voided_by, a.date_voided = t.date_voided, a.void_reason = t.void_reason WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.uuid = t.uuid";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);

			tableName = "person_attribute_type";
			// Insert into temp_person_Attribute_type
			insertQuery = "INSERT INTO tmp_"
					+ tableName
					+ " (surrogate_id, implementation_id, person_attribute_type_id, name, description, format, foreign_key, searchable, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, edit_privilege, sort_weight, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'"
					+ implementationId
					+ "', person_attribute_type_id, name, description, format, foreign_key, searchable, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, edit_privilege, sort_weight, uuid FROM "
					+ database + "." + tableName + " AS t "
					+ filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database + "." + tableName
					+ " into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery,
					remoteDb.getConnection(), targetDb.getConnection());
			// Insert into person_attribute_type
			insertQuery = "INSERT IGNORE INTO "
					+ tableName
					+ " SELECT * FROM tmp_"
					+ tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM "
					+ tableName
					+ " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			updateQuery = "UPDATE "
					+ tableName
					+ " AS a, tmp_"
					+ tableName
					+ " AS t SET a.name = t.name, a.description = t.description, a.format = t.format, a.foreign_key = t.foreign_key, a.searchable = t.searchable, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.retired = t.retired, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retire_reason = t.retire_reason, a.edit_privilege = t.edit_privilege, a.sort_weight = t.sort_weight WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.uuid = t.uuid";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);

			tableName = "person_attribute";
			insertQuery = "INSERT INTO tmp_"
					+ tableName
					+ " (surrogate_id, implementation_id, person_attribute_id, person_id, value, person_attribute_type_id, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'"
					+ implementationId
					+ "', person_attribute_id, person_id, value, person_attribute_type_id, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason, uuid FROM "
					+ database + "." + tableName + " AS t "
					+ filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database + "." + tableName
					+ " into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery,
					remoteDb.getConnection(), targetDb.getConnection());
			insertQuery = "INSERT IGNORE INTO "
					+ tableName
					+ " SELECT * FROM tmp_"
					+ tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM "
					+ tableName
					+ " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			updateQuery = "UPDATE "
					+ tableName
					+ " AS a, tmp_"
					+ tableName
					+ " AS t SET a.person_id = t.person_id, a.value = t.value, a.person_attribute_type_id = t.person_attribute_type_id, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.voided = t.voided, a.voided_by = t.voided_by, a.date_voided = t.date_voided, a.void_reason = t.void_reason WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.uuid = t.uuid";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);

			tableName = "person_address";
			insertQuery = "INSERT INTO tmp_"
					+ tableName
					+ " (surrogate_id, implementation_id, person_address_id, person_id, preferred, address1, address2, city_village, state_province, postal_code, country, latitude, longitude, start_date, end_date, creator, date_created, voided, voided_by, date_voided, void_reason, county_district, address3, address4, address5, address6, date_changed, changed_by, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'"
					+ implementationId
					+ "', person_address_id, person_id, preferred, address1, address2, city_village, state_province, postal_code, country, latitude, longitude, start_date, end_date, creator, date_created, voided, voided_by, date_voided, void_reason, county_district, address3, address4, address5, address6, date_changed, changed_by, uuid FROM "
					+ database + "." + tableName + " AS t "
					+ filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database + "." + tableName
					+ " into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery,
					remoteDb.getConnection(), targetDb.getConnection());
			insertQuery = "INSERT IGNORE INTO "
					+ tableName
					+ " SELECT * FROM tmp_"
					+ tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM "
					+ tableName
					+ " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			updateQuery = "UPDATE "
					+ tableName
					+ " AS a, tmp_"
					+ tableName
					+ " AS t SET a.person_id = t.person_id, a.preferred = t.preferred, a.address1 = t.address1, a.address2 = t.address2, a.city_village = t.city_village, a.state_province = t.state_province, a.postal_code = t.postal_code, a.country = t.country, a.latitude = t.latitude, a.longitude = t.longitude, a.start_date = t.start_date, a.end_date = t.end_date, a.creator = t.creator, a.date_created = t.date_created, a.voided = t.voided, a.voided_by = t.voided_by, a.date_voided = t.date_voided, a.void_reason = t.void_reason, a.county_district = t.county_district, a.address3 = t.address3, a.address4 = t.address4, a.address5 = t.address5, a.address6 = t.address6, a.date_changed = t.date_changed, a.changed_by = t.changed_by WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.uuid = t.uuid";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);

			tableName = "person_name";
			insertQuery = "INSERT INTO tmp_"
					+ tableName
					+ " (surrogate_id, implementation_id, person_name_id, preferred, person_id, prefix, given_name, middle_name, family_name_prefix, family_name, family_name2, family_name_suffix, degree, creator, date_created, voided, voided_by, date_voided, void_reason, changed_by, date_changed, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'"
					+ implementationId
					+ "', person_name_id, preferred, person_id, prefix, given_name, middle_name, family_name_prefix, family_name, family_name2, family_name_suffix, degree, creator, date_created, voided, voided_by, date_voided, void_reason, changed_by, date_changed, uuid FROM "
					+ database + "." + tableName + " AS t "
					+ filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database + "." + tableName
					+ " into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery,
					remoteDb.getConnection(), targetDb.getConnection());
			insertQuery = "INSERT IGNORE INTO "
					+ tableName
					+ " SELECT * FROM tmp_"
					+ tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM "
					+ tableName
					+ " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			updateQuery = "UPDATE "
					+ tableName
					+ " AS a, tmp_"
					+ tableName
					+ " AS t SET a.preferred = t.preferred, a.person_id = t.person_id, a.prefix = t.prefix, a.given_name = t.given_name, a.middle_name = t.middle_name, a.family_name_prefix = t.family_name_prefix, a.family_name = t.family_name, a.family_name2 = t.family_name2, a.family_name_suffix = t.family_name_suffix, a.degree = t.degree, a.creator = t.creator, a.date_created = t.date_created, a.voided = t.voided, a.voided_by = t.voided_by, a.date_voided = t.date_voided, a.void_reason = t.void_reason, a.changed_by = t.changed_by, a.date_changed = t.date_changed WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.uuid = t.uuid";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Load data from user-related tables into data warehouse
	 * 
	 * @param remoteDb
	 * @param implementationId
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 */
	public void importUserData(DatabaseUtil remoteDb, int implementationId)
			throws InstantiationException, IllegalAccessException,
			ClassNotFoundException {
		String database = remoteDb.getDbName();
		String insertQuery;
		String updateQuery;
		String selectQuery;
		String deleteQuery;
		String tableName;
		try {
			tableName = "role";
			insertQuery = "INSERT INTO tmp_"
					+ tableName
					+ " (surrogate_id, implementation_id, role, description, uuid) VALUES (?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId
					+ "', role, description, uuid FROM " + database + "."
					+ tableName + " AS t ";
			log.info("Inserting data from " + database + "." + tableName
					+ " into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery,
					remoteDb.getConnection(), targetDb.getConnection());
			insertQuery = "INSERT IGNORE INTO "
					+ tableName
					+ " SELECT * FROM tmp_"
					+ tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM "
					+ tableName
					+ " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			updateQuery = "UPDATE "
					+ tableName
					+ " AS a, tmp_"
					+ tableName
					+ " AS t SET a.role = t.role, a.description = t.description WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.uuid = t.uuid";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);

			tableName = "role_role";
			insertQuery = "INSERT INTO tmp_"
					+ tableName
					+ " (surrogate_id, implementation_id, parent_role, child_role) VALUES (?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId
					+ "', parent_role, child_role FROM " + database + "."
					+ tableName + " AS t ";
			log.info("Inserting data from " + database + "." + tableName
					+ " into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery,
					remoteDb.getConnection(), targetDb.getConnection());
			deleteQuery = "DELETE FROM " + tableName
					+ " WHERE implementation_id = '" + implementationId + "'";
			targetDb.runCommand(CommandType.DELETE, deleteQuery);
			insertQuery = "INSERT IGNORE INTO " + tableName
					+ " SELECT * FROM tmp_" + tableName
					+ " AS t WHERE t.implementation_id = '" + implementationId
					+ "'";
			targetDb.runCommand(CommandType.INSERT, insertQuery);

			tableName = "privilege";
			insertQuery = "INSERT INTO tmp_"
					+ tableName
					+ " (surrogate_id, implementation_id, privilege, description, uuid) VALUES (?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId
					+ "', privilege, description, uuid FROM " + database + "."
					+ tableName + "  AS t ";
			log.info("Inserting data from " + database + "." + tableName
					+ "  into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery,
					remoteDb.getConnection(), targetDb.getConnection());
			insertQuery = "INSERT IGNORE INTO "
					+ tableName
					+ " SELECT * FROM tmp_"
					+ tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM "
					+ tableName
					+ " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			updateQuery = "UPDATE "
					+ tableName
					+ " AS a, tmp_"
					+ tableName
					+ " AS t SET a.privilege = t.privilege, a.description = t.description WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.uuid = t.uuid";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);

			tableName = "role_privilege";
			insertQuery = "INSERT INTO tmp_"
					+ tableName
					+ " (surrogate_id, implementation_id, role, privilege) VALUES (?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId
					+ "', role, privilege FROM " + database + "." + tableName
					+ " AS t ";
			log.info("Inserting data from " + database + "." + tableName
					+ " into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery,
					remoteDb.getConnection(), targetDb.getConnection());
			deleteQuery = "DELETE FROM " + tableName
					+ " WHERE implementation_id = '" + implementationId + "'";
			targetDb.runCommand(CommandType.DELETE, deleteQuery);
			insertQuery = "INSERT IGNORE INTO " + tableName
					+ " SELECT * FROM tmp_" + tableName
					+ " AS t WHERE t.implementation_id = '" + implementationId
					+ "'";
			targetDb.runCommand(CommandType.INSERT, insertQuery);

			tableName = "users";
			insertQuery = "INSERT INTO tmp_"
					+ tableName
					+ " (surrogate_id, implementation_id, user_id, system_id, username, password, salt, secret_question, secret_answer, creator, date_created, changed_by, date_changed, person_id, retired, retired_by, date_retired, retire_reason, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'"
					+ implementationId
					+ "', user_id, system_id, username, password, salt, secret_question, secret_answer, creator, date_created, changed_by, date_changed, person_id, retired, retired_by, date_retired, retire_reason, uuid FROM "
					+ database + "." + tableName + " AS t "
					+ filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database + "." + tableName
					+ " into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery,
					remoteDb.getConnection(), targetDb.getConnection());
			insertQuery = "INSERT IGNORE INTO "
					+ tableName
					+ " SELECT * FROM tmp_"
					+ tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM "
					+ tableName
					+ " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			updateQuery = "UPDATE "
					+ tableName
					+ " AS a, tmp_"
					+ tableName
					+ " AS t SET a.username = t.username, a.password = t.password, a.salt = t.salt, a.secret_question = t.secret_question, a.secret_answer = t.secret_answer, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.person_id = t.person_id, a.retired = t.retired, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retire_reason = t.retire_reason WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.uuid = t.uuid";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);

			tableName = "user_property";
			insertQuery = "INSERT INTO tmp_"
					+ tableName
					+ " (surrogate_id, implementation_id, user_id, property, property_value) VALUES (?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId
					+ "', user_id, property, property_value FROM " + database
					+ "." + tableName + " AS t ";
			log.info("Inserting data from " + database + "." + tableName
					+ " into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery,
					remoteDb.getConnection(), targetDb.getConnection());
			deleteQuery = "DELETE FROM " + tableName
					+ " WHERE implementation_id = '" + implementationId + "'";
			targetDb.runCommand(CommandType.DELETE, deleteQuery);
			insertQuery = "INSERT IGNORE INTO " + tableName
					+ " SELECT * FROM tmp_" + tableName
					+ " AS t WHERE t.implementation_id = '" + implementationId
					+ "'";
			targetDb.runCommand(CommandType.INSERT, insertQuery);

			tableName = "user_role";
			insertQuery = "INSERT INTO tmp_"
					+ tableName
					+ " (surrogate_id, implementation_id, user_id, role) VALUES (?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId
					+ "', user_id, role FROM " + database + "." + tableName
					+ " AS t ";
			log.info("Inserting data from " + database + "." + tableName
					+ " into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery,
					remoteDb.getConnection(), targetDb.getConnection());
			deleteQuery = "DELETE FROM " + tableName
					+ " WHERE implementation_id = '" + implementationId + "'";
			targetDb.runCommand(CommandType.DELETE, deleteQuery);
			insertQuery = "INSERT IGNORE INTO " + tableName
					+ " SELECT * FROM tmp_" + tableName
					+ " AS t WHERE t.implementation_id = '" + implementationId
					+ "'";
			targetDb.runCommand(CommandType.INSERT, insertQuery);

			tableName = "provider_attribute_type";
			insertQuery = "INSERT INTO tmp_"
					+ tableName
					+ " (surrogate_id, implementation_id, provider_attribute_type_id, name, description, datatype, datatype_config, preferred_handler, handler_config, min_occurs, max_occurs, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'"
					+ implementationId
					+ "', provider_attribute_type_id, name, description, datatype, datatype_config, preferred_handler, handler_config, min_occurs, max_occurs, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, uuid FROM "
					+ database + "." + tableName + " AS t "
					+ filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database + "." + tableName
					+ " into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery,
					remoteDb.getConnection(), targetDb.getConnection());
			insertQuery = "INSERT IGNORE INTO "
					+ tableName
					+ " SELECT * FROM tmp_"
					+ tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM "
					+ tableName
					+ " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			updateQuery = "UPDATE "
					+ tableName
					+ " AS a, tmp_"
					+ tableName
					+ " AS t SET a.name = t.name, a.description = t.description, a.datatype = t.datatype, a.datatype_config = t.datatype_config, a.preferred_handler = t.preferred_handler, a.handler_config = t.handler_config, a.min_occurs = t.min_occurs, a.max_occurs = t.max_occurs, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.retired = t.retired, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retire_reason = t.retire_reason WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.uuid = t.uuid";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);

			tableName = "provider";
			insertQuery = "INSERT INTO tmp_"
					+ tableName
					+ " (surrogate_id, implementation_id, provider_id, person_id, name, identifier, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'"
					+ implementationId
					+ "', provider_id, person_id, name, identifier, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, uuid FROM "
					+ database + "." + tableName + " AS t "
					+ filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database + "." + tableName
					+ " into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery,
					remoteDb.getConnection(), targetDb.getConnection());
			insertQuery = "INSERT IGNORE INTO "
					+ tableName
					+ " SELECT * FROM tmp_"
					+ tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM "
					+ tableName
					+ " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			updateQuery = "UPDATE "
					+ tableName
					+ " AS a, tmp_"
					+ tableName
					+ " AS t SET a.person_id = t.person_id, a.name = t.name, a.identifier = t.identifier, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.retired = t.retired, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retire_reason = t.retire_reason WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.uuid = t.uuid";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);

			tableName = "provider_attribute";
			insertQuery = "INSERT INTO tmp_"
					+ tableName
					+ " (surrogate_id, implementation_id, provider_attribute_id, provider_id, attribute_type_id, value_reference, uuid, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'"
					+ implementationId
					+ "', provider_attribute_id, provider_id, attribute_type_id, value_reference, uuid, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason FROM "
					+ database + "." + tableName + " AS t "
					+ filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database + "." + tableName
					+ " into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery,
					remoteDb.getConnection(), targetDb.getConnection());
			insertQuery = "INSERT IGNORE INTO "
					+ tableName
					+ " SELECT * FROM tmp_"
					+ tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM "
					+ tableName
					+ " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			updateQuery = "UPDATE "
					+ tableName
					+ " AS a, tmp_"
					+ tableName
					+ " AS t SET a.provider_id = t.provider_id, a.attribute_type_id = t.attribute_type_id, a.value_reference = t.value_reference, a.uuid = t.uuid, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.voided = t.voided, a.voided_by = t.voided_by, a.date_voided = t.date_voided, a.void_reason = t.void_reason WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.uuid = t.uuid";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);
		} catch (SQLException e) {
			e.printStackTrace();
		}
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
	public void importLocationData(DatabaseUtil remoteDb, int implementationId)
			throws InstantiationException, IllegalAccessException,
			ClassNotFoundException {

		String database = remoteDb.getDbName();
		String insertQuery;
		String updateQuery;
		String selectQuery;
		String tableName;

		try {
			tableName = "location_attribute_type";
			insertQuery = "INSERT INTO tmp_"
					+ tableName
					+ "(surrogate_id, implementation_id, location_attribute_type_id, name, description, datatype, datatype_config, preferred_handler, handler_config, min_occurs, max_occurs, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'"
					+ implementationId
					+ "', location_attribute_type_id, name, description, datatype, datatype_config, preferred_handler, handler_config, min_occurs, max_occurs, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, uuid FROM "
					+ database + "." + tableName + "  AS t "
					+ filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database
					+ ".location_attribute_type into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery,
					remoteDb.getConnection(), targetDb.getConnection());
			insertQuery = "INSERT IGNORE INTO "
					+ tableName
					+ " SELECT * FROM tmp_"
					+ tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM "
					+ tableName
					+ " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			updateQuery = "UPDATE "
					+ tableName
					+ " AS a, tmp_"
					+ tableName
					+ " AS t SET a. location_attribute_type_id=t. location_attribute_type_id,a. name=t. name,a. description=t. description,a. datatype=t. datatype,a. datatype_config=t. datatype_config,a. preferred_handler=t. preferred_handler,a. handler_config=t. handler_config,a. min_occurs=t. min_occurs,a. max_occurs=t. max_occurs,a. creator=t. creator,a. date_created=t. date_created,a. changed_by=t. changed_by,a. date_changed=t. date_changed,a. retired=t. retired,a. retired_by=t. retired_by,a. date_retired=t. date_retired,a. retire_reason=t. retire_reason WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.uuid = t.uuid";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);

			// Location
			tableName = "location";
			insertQuery = "INSERT INTO tmp_"
					+ tableName
					+ "(surrogate_id, implementation_id, location_id, name, description, address1, address2, city_village, state_province, postal_code, country, latitude, longitude, creator, date_created, county_district, address3, address4, address5, address6, retired, retired_by, date_retired, retire_reason, parent_location, uuid, changed_by, date_changed) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'"
					+ implementationId
					+ "', location_id, name, description, address1, address2, city_village, state_province, postal_code, country, latitude, longitude, creator, date_created, county_district, address3, address4, address5, address6, retired, retired_by, date_retired, retire_reason, parent_location, uuid, changed_by, date_changed FROM "
					+ database + "." + tableName + " AS t "
					+ filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database
					+ ".location into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery,
					remoteDb.getConnection(), targetDb.getConnection());
			// Insert into warehouse from tmp_table
			insertQuery = "INSERT IGNORE INTO "
					+ tableName
					+ " SELECT * FROM tmp_"
					+ tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM "
					+ tableName
					+ " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			// Update the actual database from temp table
			updateQuery = "UPDATE "
					+ tableName
					+ " AS a, tmp_"
					+ tableName
					+ " AS t SET a. location_id=t. location_id,a. name=t. name,a. description=t. description,a. address1=t. address1,a. address2=t. address2,a. city_village=t. city_village,a. state_province=t. state_province,a. postal_code=t. postal_code,a. country=t. country,a. latitude=t. latitude,a. longitude=t. longitude,a. creator=t. creator,a. date_created=t. date_created,a. county_district=t. county_district,a. address3=t. address3,a. address4=t. address4,a. address5=t. address5,a. address6=t. address6,a. retired=t. retired,a. retired_by=t. retired_by,a. date_retired=t. date_retired,a. retire_reason=t. retire_reason,a. parent_location=t. parent_location,a. uuid=t. uuid,a. changed_by=t. changed_by,a. date_changed=t. date_changed WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.uuid = t.uuid";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);

			// Location Attribute
			tableName = "location_attribute";
			insertQuery = "INSERT INTO tmp_"
					+ tableName
					+ " (surrogate_id, implementation_id, location_attribute_id, location_id, attribute_type_id, value_reference, uuid, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'"
					+ implementationId
					+ "', location_attribute_id, location_id, attribute_type_id, value_reference, uuid, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason FROM "
					+ database + "." + tableName + " AS t "
					+ filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database
					+ ".location_attribute into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery,
					remoteDb.getConnection(), targetDb.getConnection());
			// Insert into warehouse from tmp_table
			insertQuery = "INSERT IGNORE INTO "
					+ tableName
					+ " SELECT * FROM tmp_"
					+ tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM "
					+ tableName
					+ " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			updateQuery = "UPDATE "
					+ tableName
					+ " AS a, tmp_"
					+ tableName
					+ " AS t SET a.location_attribute_id=t.location_attribute_id,a. location_id=t. location_id,a. attribute_type_id=t. attribute_type_id,a. value_reference=t. value_reference,a. uuid=t. uuid,a. creator=t. creator,a. date_created=t. date_created,a. changed_by=t. changed_by,a. date_changed=t. date_changed,a. voided=t. voided,a. voided_by=t. voided_by,a. date_voided=t. date_voided,a. void_reason=t. void_reason WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.uuid = t.uuid";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);

			// Location Tag
			tableName = "location_tag";
			insertQuery = "INSERT INTO tmp_"
					+ tableName
					+ " (surrogate_id, implementation_id, location_tag_id, name, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid, changed_by, date_changed) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'"
					+ implementationId
					+ "', location_tag_id, name, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid, changed_by, date_changed FROM "
					+ database + "." + tableName + " AS t "
					+ filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database
					+ ".location_tag into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery,
					remoteDb.getConnection(), targetDb.getConnection());
			// Insert into warehouse from tmp_table
			insertQuery = "INSERT IGNORE INTO "
					+ tableName
					+ " SELECT * FROM tmp_"
					+ tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM "
					+ tableName
					+ " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			updateQuery = "UPDATE "
					+ tableName
					+ " AS a, tmp_"
					+ tableName
					+ " AS t SET a.location_tag_id=t.location_tag_id,a.name=t.name,a.description=t.description,a.creator=t.creator,a.date_created=t.date_created,a.retired=t.retired,a.retired_by=t.retired_by,a.date_retired=t.date_retired,a.retire_reason=t.retire_reason,a.uuid=t.uuid,a.changed_by=t.changed_by,a.date_changed= t.date_changed WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.uuid = t.uuid";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);

			// Location Tag Map
			tableName = "location_tag_map";
			insertQuery = "INSERT IGNORE INTO tmp_"
					+ tableName
					+ " (surrogate_id, implementation_id, location_id, location_tag_id) VALUES (?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId
					+ "', location_id, location_tag_id FROM " + database + "."
					+ tableName + " AS t ";
			log.info("Inserting data from " + database
					+ ".location_tag_map into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery,
					remoteDb.getConnection(), targetDb.getConnection());
			// Insert into warehouse from tmp_table
			insertQuery = "INSERT IGNORE INTO " + tableName
					+ " SELECT * FROM tmp_" + tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM " + tableName
					+ " WHERE implementation_id = t.implementation_id)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			updateQuery = "UPDATE "
					+ tableName
					+ " AS a, tmp_"
					+ tableName
					+ " AS t SET a.location_id = t.location_id, a.location_tag_id = t.location_tag_id WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "'";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);

		} catch (SQLException e) {
			e.printStackTrace();
		}
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
	public void importConceptData(DatabaseUtil remoteDb, int implementationId)
			throws InstantiationException, IllegalAccessException,
			ClassNotFoundException {
		String database = remoteDb.getDbName();
		String insertQuery;
		String updateQuery;
		String selectQuery;
		String tableName;
		try {
			// Concept Class
			tableName = "concept_class";
			insertQuery = "INSERT INTO  tmp_"
					+ tableName
					+ " (surrogate_id, implementation_id, concept_class_id, name, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'"
					+ implementationId
					+ "', concept_class_id, name, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid FROM "
					+ database + "." + tableName + " AS t "
					+ filter("t.date_created", null);
			log.info("Inserting data from " + database + "." + tableName
					+ " into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery,
					remoteDb.getConnection(), targetDb.getConnection());
			insertQuery = "INSERT IGNORE INTO "
					+ tableName
					+ " SELECT * FROM tmp_"
					+ tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM "
					+ tableName
					+ " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			// Update the warehouse database from tmp table...
			updateQuery = "UPDATE "
					+ tableName
					+ " AS a, tmp_"
					+ tableName
					+ " AS t SET a.concept_class_id=t.concept_class_id,a.name=t.name,a.description=t.description,a.creator=t.creator,a.date_created=t.date_created,a.retired=t.retired,a.retired_by=t.retired_by,a.date_retired=t.date_retired,a.retire_reason=t.retire_reason WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.uuid = t.uuid";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);

			// Concept Set
			tableName = "concept_set";
			insertQuery = "INSERT INTO tmp_"
					+ tableName
					+ " (surrogate_id, implementation_id, concept_set_id, concept_id, concept_set, sort_weight, creator, date_created, uuid) VALUES (?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'"
					+ implementationId
					+ "', concept_set_id, concept_id, concept_set, sort_weight, creator, date_created, uuid FROM "
					+ database + "." + tableName + " AS t "
					+ filter("t.date_created", null);
			log.info("Inserting data from " + database + "." + tableName
					+ " into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery,
					remoteDb.getConnection(), targetDb.getConnection());
			// Insert into warehouse from tmp_table...
			insertQuery = "INSERT IGNORE INTO "
					+ tableName
					+ " SELECT * FROM tmp_"
					+ tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM "
					+ tableName
					+ " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			// Update the warehouse database from tmp table...
			updateQuery = "UPDATE "
					+ tableName
					+ " AS a, tmp_"
					+ tableName
					+ " AS t SET a.concept_set_id=t.concept_set_id,a.concept_id=t.concept_id,a.concept_set=t.concept_set,a.sort_weight=t.sort_weight,a.creator=t.creator,a.date_created=t.date_created WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.uuid = t.uuid";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);

			// Concept Data Type
			tableName = "concept_datatype";
			insertQuery = "INSERT INTO tmp_"
					+ tableName
					+ " (surrogate_id, implementation_id, concept_datatype_id, name, hl7_abbreviation, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'"
					+ implementationId
					+ "', concept_datatype_id, name, hl7_abbreviation, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid FROM "
					+ database + "." + tableName + " AS t "
					+ filter("t.date_created", null);
			log.info("Inserting data from " + database + "." + tableName
					+ " into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery,
					remoteDb.getConnection(), targetDb.getConnection());
			// Insert into warehouse from tmp_table...
			insertQuery = "INSERT IGNORE INTO "
					+ tableName
					+ " SELECT * FROM tmp_"
					+ tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM "
					+ tableName
					+ " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			// Update the warehouse database from tmp table...
			updateQuery = "UPDATE "
					+ tableName
					+ " AS a, tmp_"
					+ tableName
					+ " AS t SET a.concept_datatype_id=t.concept_datatype_id,a.name=t.name,a.hl7_abbreviation=t.hl7_abbreviation,a.description=t.description,a.creator=t.creator,a.date_created=t.date_created,a.retired=t.retired,a.retired_by=t.retired_by,a.date_retired=t.date_retired,a.retire_reason=t.retire_reason WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.uuid = t.uuid";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);

			// Concept Map Type
			tableName = "concept_map_type";
			insertQuery = "INSERT INTO tmp_"
					+ tableName
					+ " (surrogate_id, implementation_id, concept_map_type_id, name, description, creator, date_created, changed_by, date_changed, is_hidden, retired, retired_by, date_retired, retire_reason, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'"
					+ implementationId
					+ "', concept_map_type_id, name, description, creator, date_created, changed_by, date_changed, is_hidden, retired, retired_by, date_retired, retire_reason, uuid FROM "
					+ database + "." + tableName + " AS t "
					+ filter("t.date_created", null);
			log.info("Inserting data from " + database
					+ ".concept_map_type into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery,
					remoteDb.getConnection(), targetDb.getConnection());
			// Insert into warehouse from tmp_table...
			insertQuery = "INSERT IGNORE INTO "
					+ tableName
					+ " SELECT * FROM tmp_"
					+ tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM "
					+ tableName
					+ " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			// Update the warehouse database from tmp table...
			updateQuery = "UPDATE "
					+ tableName
					+ " AS a, tmp_"
					+ tableName
					+ " AS t SET a.concept_map_type_id=t.concept_map_type_id,a.name=t.name,a.description=t.description,a.creator=t.creator,a.date_created=t.date_created,a.changed_by=t.changed_by,a.date_changed=t.date_changed,a.is_hidden=t.is_hidden,a.retired=t.retired,a.retired_by=t.retired_by,a.date_retired=t.date_retired,a.retire_reason=t.retire_reason WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.uuid = t.uuid";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);

			// Concept
			tableName = "concept";
			insertQuery = "INSERT INTO tmp_"
					+ tableName
					+ " (surrogate_id, implementation_id, concept_id, retired, short_name, description, form_text, datatype_id, class_id, is_set, creator, date_created, version, changed_by, date_changed, retired_by, date_retired, retire_reason, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'"
					+ implementationId
					+ "', concept_id, retired, short_name, description, form_text, datatype_id, class_id, is_set, creator, date_created, version, changed_by, date_changed, retired_by, date_retired, retire_reason, uuid FROM "
					+ database + "." + tableName + " AS t "
					+ filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database
					+ ".concept into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery,
					remoteDb.getConnection(), targetDb.getConnection());
			// Insert into warehouse from tmp_table...
			insertQuery = "INSERT IGNORE INTO "
					+ tableName
					+ " SELECT * FROM tmp_"
					+ tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM "
					+ tableName
					+ " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			// Update the warehouse database from tmp table...
			updateQuery = "UPDATE "
					+ tableName
					+ " AS a, tmp_"
					+ tableName
					+ " AS t SET a.concept_id=t.concept_id,a.retired=t.retired,a.short_name=t.short_name,a.description=t.description,a.form_text=t.form_text,a.datatype_id=t.datatype_id,a.class_id=t.class_id,a.is_set=t.is_set,a.creator=t.creator,a.date_created=t.date_created,a.version=t.version,a.changed_by=t.changed_by,a.date_changed=t.date_changed,a.retired_by=t.retired_by,a.date_retired=t.date_retired,a.retire_reason=t.retire_reason WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.uuid = t.uuid";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);

			// Concept Name
			tableName = "concept_name";
			insertQuery = "INSERT INTO tmp_"
					+ tableName
					+ " (surrogate_id, implementation_id, concept_id, name, locale, creator, date_created, concept_name_id, voided, voided_by, date_voided, void_reason, uuid, concept_name_type, locale_preferred) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'"
					+ implementationId
					+ "', concept_id, name, locale, creator, date_created, concept_name_id, voided, voided_by, date_voided, void_reason, uuid, concept_name_type, locale_preferred FROM "
					+ database + "." + tableName + " AS t "
					+ filter("t.date_created", null);
			log.info("Inserting data from " + database
					+ ".concept_name into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery,
					remoteDb.getConnection(), targetDb.getConnection());
			// Insert into warehouse from tmp_table...
			insertQuery = "INSERT IGNORE INTO "
					+ tableName
					+ " SELECT * FROM tmp_"
					+ tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM "
					+ tableName
					+ " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			// Update the warehouse database from tmp table...
			updateQuery = "UPDATE "
					+ tableName
					+ " AS a, tmp_"
					+ tableName
					+ " AS t SET a.concept_id=t.concept_id,a.name=t.name,a.locale=t.locale,a.creator=t.creator,a.date_created=t.date_created,a.concept_name_id=t.concept_name_id,a.voided=t.voided,a.voided_by=t.voided_by,a.date_voided=t.date_voided,a.void_reason=t.void_reason,a.concept_name_type=t.concept_name_type,a.locale_preferred=t.locale_preferred WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.uuid = t.uuid";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);

			// Concept Description
			tableName = "concept_description";
			insertQuery = "INSERT INTO tmp_"
					+ tableName
					+ " (surrogate_id, implementation_id, concept_description_id, concept_id, description, locale, creator, date_created, changed_by, date_changed, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'"
					+ implementationId
					+ "', concept_description_id, concept_id, description, locale, creator, date_created, changed_by, date_changed, uuid FROM "
					+ database + "." + tableName + " AS t "
					+ filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database
					+ ".concept_description into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery,
					remoteDb.getConnection(), targetDb.getConnection());
			// Insert into warehouse from tmp_table...
			insertQuery = "INSERT IGNORE INTO "
					+ tableName
					+ " SELECT * FROM tmp_"
					+ tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM "
					+ tableName
					+ " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			// Update the warehouse database from tmp table...
			updateQuery = "UPDATE "
					+ tableName
					+ " AS a, tmp_"
					+ tableName
					+ " AS t SET a.concept_description_id=t.concept_description_id,a.concept_id=t.concept_id,a.description=t.description,a.locale=t.locale,a.creator=t.creator,a.date_created=t.date_created,a.changed_by=t.changed_by,a.date_changed=t.date_changed WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.uuid = t.uuid";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);

			// Concept Answer
			tableName = "concept_answer";
			insertQuery = "INSERT INTO tmp_"
					+ tableName
					+ " (surrogate_id, implementation_id, concept_answer_id, concept_id, answer_concept, answer_drug, creator, date_created, uuid, sort_weight) VALUES (?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'"
					+ implementationId
					+ "', concept_answer_id, concept_id, answer_concept, answer_drug, creator, date_created, uuid, sort_weight FROM "
					+ database + "." + tableName + " AS t "
					+ filter("t.date_created", null);
			log.info("Inserting data from " + database
					+ ".concept_answer into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery,
					remoteDb.getConnection(), targetDb.getConnection());
			// Insert into warehouse from tmp_table...
			insertQuery = "INSERT IGNORE INTO "
					+ tableName
					+ " SELECT * FROM tmp_"
					+ tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM "
					+ tableName
					+ " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			// Update the warehouse database from tmp table...
			updateQuery = "UPDATE "
					+ tableName
					+ " AS a, tmp_"
					+ tableName
					+ " AS t SET a.concept_answer_id=t.concept_answer_id,a.concept_id=t.concept_id,a.answer_concept=t.answer_concept,a.answer_drug=t.answer_drug,a.creator=t.creator,a.date_created=t.date_created,a.sort_weight=t.sort_weight WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.uuid = t.uuid";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);

			// Concept Numeric
			tableName = "concept_numeric";
			insertQuery = "INSERT INTO tmp_"
					+ tableName
					+ " (surrogate_id, implementation_id, concept_id, hi_absolute, hi_critical, hi_normal, low_absolute, low_critical, low_normal, units, precise, display_precision) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'"
					+ implementationId
					+ "', concept_id, hi_absolute, hi_critical, hi_normal, low_absolute, low_critical, low_normal, units, precise, display_precision FROM "
					+ database
					+ "."
					+ tableName
					+ " AS t WHERE t.hi_absolute IS NOT NULL OR t.hi_critical IS NOT NULL OR t.hi_normal IS NOT NULL OR t.low_absolute IS NOT NULL OR t.low_critical IS NOT NULL OR t.low_normal IS NOT NULL OR t.units IS NOT NULL";
			log.info("Inserting data from " + database
					+ ".concept_numeric into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery,
					remoteDb.getConnection(), targetDb.getConnection());
			// Insert into warehouse from tmp_table...
			insertQuery = "INSERT IGNORE INTO " + tableName
					+ " SELECT * FROM tmp_" + tableName
					+ " AS t WHERE concept_id NOT IN (SELECT concept_id FROM "
					+ tableName
					+ " WHERE implementation_id = t.implementation_id)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			// Update the warehouse database from tmp table...
			updateQuery = "UPDATE "
					+ tableName
					+ " AS a, tmp_"
					+ tableName
					+ " AS t SET a.concept_id=t.concept_id,a.hi_absolute=t.hi_absolute,a.hi_critical=t.hi_critical,a.hi_normal=t.hi_normal,a.low_absolute=t.low_absolute,a.low_critical=t.low_critical,a.low_normal=t.low_normal,a.units=t.units,a.precise=t.precise,a.display_precision=t.display_precision WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.concept_id = t.concept_id";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);
		} catch (SQLException e) {
			e.printStackTrace();
		}
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
	public void importPatientData(DatabaseUtil remoteDb, int implementationId)
			throws InstantiationException, IllegalAccessException,
			ClassNotFoundException {

		String database = remoteDb.getDbName();
		String insertQuery;
		String updateQuery;
		String selectQuery;
		String tableName;

		try {
			// Patient Identifier Type
			tableName = "patient_identifier_type";
			insertQuery = "INSERT INTO tmp_"
					+ tableName
					+ " (surrogate_id, implementation_id, patient_identifier_type_id, name, description, format, check_digit, creator, date_created, required, format_description, validator, location_behavior, retired, retired_by, date_retired, retire_reason, uuid, uniqueness_behavior) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'"
					+ implementationId
					+ "', patient_identifier_type_id, name, description, format, check_digit, creator, date_created, required, format_description, validator, location_behavior, retired, retired_by, date_retired, retire_reason, uuid, uniqueness_behavior FROM "
					+ database + "." + tableName + " AS t "
					+ filter("t.date_created", null);
			log.info("Inserting data from " + database
					+ ".patient_identifier_type into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery,
					remoteDb.getConnection(), targetDb.getConnection());
			// Insert new records
			insertQuery = "INSERT IGNORE INTO "
					+ tableName
					+ " SELECT * FROM tmp_"
					+ tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM "
					+ tableName
					+ " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			// Update the existing records
			updateQuery = "UPDATE "
					+ tableName
					+ " AS a, tmp_"
					+ tableName
					+ " AS t SET a.patient_identifier_type_id=t.patient_identifier_type_id,a.name=t.name,a.description=t.description,a.format=t.format,a.check_digit=t.check_digit,a.creator=t.creator,a.date_created=t.date_created,a.required=t.required,a.format_description=t.format_description,a.validator=t.validator,a.location_behavior=t.location_behavior,a.retired=t.retired,a.retired_by=t.retired_by,a.date_retired=t.date_retired,a.retire_reason=t.retire_reason,a.uniqueness_behavior=t.uniqueness_behavior WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.uuid = t.uuid";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);

			// Patient
			tableName = "patient";
			insertQuery = "INSERT INTO tmp_"
					+ tableName
					+ " (surrogate_id, implementation_id, patient_id, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason) VALUES (?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'"
					+ implementationId
					+ "', patient_id, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason FROM "
					+ database + "." + tableName + " AS t "
					+ filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database
					+ ".patient into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery,
					remoteDb.getConnection(), targetDb.getConnection());
			// Insert new records
			insertQuery = "INSERT IGNORE INTO " + tableName
					+ " SELECT * FROM tmp_" + tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM " + tableName
					+ " WHERE implementation_id = t.implementation_id and patient_id = t.patient_id)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			// Update the existing records
			updateQuery = "UPDATE "
					+ tableName
					+ " AS a, tmp_"
					+ tableName
					+ " AS t SET a.creator=t.creator,a.date_created=t.date_created,a.changed_by=t.changed_by,a.date_changed=t.date_changed,a.voided=t.voided,a.voided_by=t.voided_by,a.date_voided=t.date_voided,a.void_reason=t.void_reason WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.patient_id=t.patient_id";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);

			// Patient Identifier
			tableName = "patient_identifier";
			insertQuery = "INSERT INTO tmp_"
					+ tableName
					+ " (surrogate_id, implementation_id, patient_identifier_id, patient_id, identifier, identifier_type, preferred, location_id, creator, date_created, date_changed, changed_by, voided, voided_by, date_voided, void_reason, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'"
					+ implementationId
					+ "', patient_identifier_id, patient_id, identifier, identifier_type, preferred, location_id, creator, date_created, date_changed, changed_by, voided, voided_by, date_voided, void_reason, uuid FROM "
					+ database + "." + tableName + " AS t "
					+ filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database
					+ ".patient_identifier into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery,
					remoteDb.getConnection(), targetDb.getConnection());
			// Insert new records
			insertQuery = "INSERT IGNORE INTO "
					+ tableName
					+ " SELECT * FROM tmp_"
					+ tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM "
					+ tableName
					+ " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			// Update the existing records
			updateQuery = "UPDATE "
					+ tableName
					+ " AS a, tmp_"
					+ tableName
					+ " AS t SET a.patient_identifier_id=t.patient_identifier_id,a.patient_id=t.patient_id,a.identifier=t.identifier,a.identifier_type=t.identifier_type,a.preferred=t.preferred,a.location_id=t.location_id,a.creator=t.creator,a.date_created=t.date_created,a.date_changed=t.date_changed,a.changed_by=t.changed_by,a.voided=t.voided,a.voided_by=t.voided_by,a.date_voided=t.date_voided,a.void_reason=t.void_reason WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.uuid = t.uuid";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);

			// Patient Program
			tableName = "patient_program";
			insertQuery = "INSERT INTO tmp_"
					+ tableName
					+ " (surrogate_id, implementation_id, patient_program_id, patient_id, program_id, date_enrolled, date_completed, location_id, outcome_concept_id, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'"
					+ implementationId
					+ "', patient_program_id, patient_id, program_id, date_enrolled, date_completed, location_id, outcome_concept_id, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason, uuid FROM "
					+ database + "." + tableName + " AS t "
					+ filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database
					+ ".patient_program into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery,
					remoteDb.getConnection(), targetDb.getConnection());
			// Insert new records
			insertQuery = "INSERT IGNORE INTO "
					+ tableName
					+ " SELECT * FROM tmp_"
					+ tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM "
					+ tableName
					+ " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			// Update the existing records
			updateQuery = "UPDATE "
					+ tableName
					+ " AS a, tmp_"
					+ tableName
					+ " AS t SET a.patient_program_id=t.patient_program_id,a.patient_id=t.patient_id,a.program_id=t.program_id,a.date_enrolled=t.date_enrolled,a.date_completed=t.date_completed,a.location_id=t.location_id,a.outcome_concept_id=t.outcome_concept_id,a.creator=t.creator,a.date_created=t.date_created,a.changed_by=t.changed_by,a.date_changed=t.date_changed,a.voided=t.voided,a.voided_by=t.voided_by,a.date_voided=t.date_voided,a.void_reason=t.void_reason WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.uuid = t.uuid";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);

		} catch (SQLException e) {
			e.printStackTrace();
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
	public void importEncounterData(DatabaseUtil remoteDb, int implementationId)
			throws InstantiationException, IllegalAccessException,
			ClassNotFoundException {
		String database = remoteDb.getDbName();
		String insertQuery;
		String updateQuery;
		String selectQuery;
		String tableName;
		try {
			// Encounter Type
			tableName = "encounter_type";
			insertQuery = "INSERT INTO tmp_"
					+ tableName
					+ " (surrogate_id, implementation_id, encounter_type_id, name, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid, edit_privilege, view_privilege) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'"
					+ implementationId
					+ "', encounter_type_id, name, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid, edit_privilege, view_privilege FROM "
					+ database + "." + tableName + " AS t "
					+ filter("t.date_created", null);
			log.info("Inserting data from " + database
					+ ".encounter_type into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery,
					remoteDb.getConnection(), targetDb.getConnection());
			// Insert new records
			insertQuery = "INSERT IGNORE INTO "
					+ tableName
					+ " SELECT * FROM tmp_"
					+ tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM "
					+ tableName
					+ " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			// Update the existing records
			updateQuery = "UPDATE "
					+ tableName
					+ " AS a, tmp_"
					+ tableName
					+ " AS t SET a.encounter_type_id=t.encounter_type_id,a.name=t.name,a.description=t.description,a.creator=t.creator,a.date_created=t.date_created,a.retired=t.retired,a.retired_by=t.retired_by,a.date_retired=t.date_retired,a.retire_reason=t.retire_reason,a.edit_privilege=t.edit_privilege,a.view_privilege=t.view_privilege WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.uuid = t.uuid";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);

			// Encounter Role
			tableName = "encounter_role";
			insertQuery = "INSERT INTO tmp_"
					+ tableName
					+ " (surrogate_id, implementation_id, encounter_role_id, name, description, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'"
					+ implementationId
					+ "', encounter_role_id, name, description, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, uuid FROM "
					+ database + "." + tableName + " AS t "
					+ filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database
					+ ".encounter_role into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery,
					remoteDb.getConnection(), targetDb.getConnection());
			// Insert new records
			insertQuery = "INSERT IGNORE INTO "
					+ tableName
					+ " SELECT * FROM tmp_"
					+ tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM "
					+ tableName
					+ " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			// Update the existing records
			updateQuery = "UPDATE "
					+ tableName
					+ " AS a, tmp_"
					+ tableName
					+ " AS t SET a.encounter_role_id=t.encounter_role_id,a.name=t.name,a.description=t.description,a.creator=t.creator,a.date_created=t.date_created,a.changed_by=t.changed_by,a.date_changed=t.date_changed,a.retired=t.retired,a.retired_by=t.retired_by,a.date_retired=t.date_retired,a.retire_reason=t.retire_reason WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.uuid = t.uuid";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);

			// Data is too much to handle in single query; import in batches
			// Encounter
			tableName = "encounter";
			Object[][] dateData = remoteDb.getTableData(tableName,
					"DATE(date_created)", filter("date_created", null), true);
			ArrayList<String> dates = new ArrayList<String>();
			for (Object[] date : dateData) {
				dates.add(date[0].toString());
			}
			for (String date : dates) {
				log.info("Inserting data from " + database + "." + tableName
						+ " into data warehouse for date " + date);
				insertQuery = "INSERT INTO tmp_"
						+ tableName
						+ " (surrogate_id, implementation_id, encounter_id, encounter_type, patient_id, location_id, form_id, encounter_datetime, creator, date_created, voided, voided_by, date_voided, void_reason, changed_by, date_changed, visit_id, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
				selectQuery = "SELECT 0,'"
						+ implementationId
						+ "', encounter_id, encounter_type, patient_id, location_id, form_id, encounter_datetime, creator, date_created, voided, voided_by, date_voided, void_reason, changed_by, date_changed, visit_id, uuid FROM "
						+ database + "." + tableName
						+ " AS t WHERE DATE(t.date_created) = '" + date + "'";
				remoteSelectInsert(selectQuery, insertQuery,
						remoteDb.getConnection(), targetDb.getConnection());

			}
			remoteSelectInsert(selectQuery, insertQuery,
					remoteDb.getConnection(), targetDb.getConnection());
			// Insert new records
			insertQuery = "INSERT IGNORE INTO "
					+ tableName
					+ " SELECT * FROM tmp_"
					+ tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM "
					+ tableName
					+ " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			// Update the existing records
			updateQuery = "UPDATE "
					+ tableName
					+ " AS a, tmp_"
					+ tableName
					+ " AS t SET a.encounter_id=t.encounter_id,a.encounter_type=t.encounter_type,a.patient_id=t.patient_id,a.location_id=t.location_id,a.form_id=t.form_id,a.encounter_datetime=t.encounter_datetime,a.creator=t.creator,a.date_created=t.date_created,a.voided=t.voided,a.voided_by=t.voided_by,a.date_voided=t.date_voided,a.void_reason=t.void_reason,a.changed_by=t.changed_by,a.date_changed=t.date_changed,a.visit_id=t.visit_id WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.uuid = t.uuid";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);

			// Encounter Provider
			tableName = "encounter_provider";
			dateData = remoteDb.getTableData(tableName, "DATE(date_created)",
					filter("date_created", null), true);
			dates = new ArrayList<String>();
			for (Object[] date : dateData) {
				dates.add(date[0].toString());
			}
			for (String date : dates) {
				log.info("Inserting data from " + database + "." + tableName
						+ " into data warehouse for date " + date);
				insertQuery = "INSERT INTO tmp_"
						+ tableName
						+ " (surrogate_id, implementation_id, encounter_provider_id, encounter_id, provider_id, encounter_role_id, creator, date_created, changed_by, date_changed, voided, date_voided, voided_by, void_reason, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
				selectQuery = "SELECT 0,'"
						+ implementationId
						+ "', encounter_provider_id, encounter_id, provider_id, encounter_role_id, creator, date_created, changed_by, date_changed, voided, date_voided, voided_by, void_reason, uuid FROM "
						+ database + "." + tableName
						+ " AS t WHERE DATE(t.date_created) = '" + date + "'";
				remoteSelectInsert(selectQuery, insertQuery,
						remoteDb.getConnection(), targetDb.getConnection());
			}
			log.info("Inserting data from " + database
					+ ".encounter_provider into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery,
					remoteDb.getConnection(), targetDb.getConnection());
			// Insert new records
			insertQuery = "INSERT IGNORE INTO "
					+ tableName
					+ " SELECT * FROM tmp_"
					+ tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM "
					+ tableName
					+ " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			// Update the existing records
			updateQuery = "UPDATE "
					+ tableName
					+ " AS a, tmp_"
					+ tableName
					+ " AS t SET a.encounter_provider_id=t.encounter_provider_id,a.encounter_id=t.encounter_id,a.provider_id=t.provider_id,a.encounter_role_id=t.encounter_role_id,a.creator=t.creator,a.date_created=t.date_created,a.changed_by=t.changed_by,a.date_changed=t.date_changed,a.voided=t.voided,a.date_voided=t.date_voided,a.voided_by=t.voided_by,a.void_reason=t.void_reason WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.uuid = t.uuid";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);

			// Observation
			tableName = "obs";
			// Get all unique dates from within the date range
			dateData = remoteDb.getTableData(tableName, "DATE(date_created)",
					filter("date_created", null), true);
			dates = new ArrayList<String>();
			for (Object[] date : dateData) {
				dates.add(date[0].toString());
			}
			for (String date : dates) {
				log.info("Inserting data from " + database + "." + tableName
						+ " into data warehouse for date " + date);
				insertQuery = "INSERT INTO tmp_"
						+ tableName
						+ " (surrogate_id, implementation_id, obs_id, person_id, concept_id, encounter_id, order_id, obs_datetime, location_id, obs_group_id, accession_number, value_group_id, value_boolean, value_coded, value_coded_name_id, value_drug, value_datetime, value_numeric, value_modifier, value_text, value_complex, comments, creator, date_created, voided, voided_by, date_voided, void_reason, uuid, previous_version) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
				selectQuery = "SELECT 0,'"
						+ implementationId
						+ "', obs_id, person_id, concept_id, encounter_id, order_id, obs_datetime, location_id, obs_group_id, accession_number, value_group_id, value_boolean, value_coded, value_coded_name_id, value_drug, value_datetime, value_numeric, value_modifier, value_text, value_complex, comments, creator, date_created, voided, voided_by, date_voided, void_reason, uuid, previous_version FROM "
						+ database + "." + tableName
						+ " AS t WHERE DATE(t.date_created) = '" + date + "'";
				remoteSelectInsert(selectQuery, insertQuery,
						remoteDb.getConnection(), targetDb.getConnection());
			}
			// Insert new records
			insertQuery = "INSERT IGNORE INTO "
					+ tableName
					+ " SELECT * FROM tmp_"
					+ tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM "
					+ tableName
					+ " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			// Update the existing records
			updateQuery = "UPDATE "
					+ tableName
					+ " AS a, tmp_"
					+ tableName
					+ " AS t SET a.obs_id=t.obs_id,a.person_id=t.person_id,a.concept_id=t.concept_id,a.encounter_id=t.encounter_id,a.order_id=t.order_id,a.obs_datetime=t.obs_datetime,a.location_id=t.location_id,a.obs_group_id=t.obs_group_id,a.accession_number=t.accession_number,a.value_group_id=t.value_group_id,a.value_boolean=t.value_boolean,a.value_coded=t.value_coded,a.value_coded_name_id=t.value_coded_name_id,a.value_drug=t.value_drug,a.value_datetime=t.value_datetime,a.value_numeric=t.value_numeric,a.value_modifier=t.value_modifier,a.value_text =t.value_text ,a.value_complex=t.value_complex,a.comments=t.comments,a.creator=t.creator,a.date_created=t.date_created,a.voided=t.voided,a.voided_by=t.voided_by,a.date_voided=t.date_voided,a.void_reason=t.void_reason,a.previous_version=t.previous_version WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.uuid = t.uuid";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Load data from visit-related tables into data warehouse
	 * 
	 * @param implementationId
	 * @param database
	 * @throws ClassNotFoundException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	public void importVisitData(DatabaseUtil remoteDb, int implementationId)
			throws InstantiationException, IllegalAccessException,
			ClassNotFoundException {
		String database = remoteDb.getDbName();
		String insertQuery;
		String updateQuery;
		String selectQuery;
		String tableName;
		try {
			// Visit Type
			tableName = "visit_type";
			insertQuery = "INSERT INTO tmp_"
					+ tableName
					+ " (surrogate_id, implementation_id, visit_type_id,name ,description ,creator,date_created,changed_by ,date_changed ,retired ,retired_by ,date_retired ,retire_reason,uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'"
					+ implementationId
					+ "', visit_type_id,name ,description ,creator,date_created,changed_by ,date_changed ,retired ,retired_by ,date_retired ,retire_reason,uuid FROM "
					+ database + "." + tableName + " AS t "
					+ filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database
					+ ".encounter_type into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery,
					remoteDb.getConnection(), targetDb.getConnection());
			// Insert new records
			insertQuery = "INSERT IGNORE INTO "
					+ tableName
					+ " SELECT * FROM tmp_"
					+ tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM "
					+ tableName
					+ " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			// Update the existing records
			updateQuery = "UPDATE "
					+ tableName
					+ " AS a, tmp_"
					+ tableName
					+ " AS t SET a.visit_type_id=t.visit_type_id,a.name =t.name ,a.description =t.description ,a.creator=t.creator,a.date_created=t.date_created,a.changed_by =t.changed_by ,a.date_changed =t.date_changed ,a.retired =t.retired ,a.retired_by =t.retired_by ,a.date_retired =t.date_retired ,a.retire_reason=t.retire_reason,a.uuid =t.uuid  WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.uuid = t.uuid";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);

			// Visit Attribute Type
			tableName = "visit_attribute_type";
			insertQuery = "INSERT INTO tmp_"
					+ tableName
					+ " (surrogate_id, implementation_id,visit_attribute_type_id,name,description ,datatype,datatype_config ,preferred_handler ,handler_config ,min_occurs ,max_occurs,creator ,date_created ,changed_by ,date_changed ,retired ,retired_by,date_retired ,retire_reason,uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'"
					+ implementationId
					+ "', visit_attribute_type_id,name,description ,datatype,datatype_config ,preferred_handler ,handler_config ,min_occurs ,max_occurs,creator ,date_created ,changed_by ,date_changed ,retired ,retired_by,date_retired ,retire_reason,uuid  FROM "
					+ database + "." + tableName + " AS t "
					+ filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database
					+ ".encounter_type into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery,
					remoteDb.getConnection(), targetDb.getConnection());
			// Insert new records
			insertQuery = "INSERT IGNORE INTO "
					+ tableName
					+ " SELECT * FROM tmp_"
					+ tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM "
					+ tableName
					+ " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			// Update the existing records
			updateQuery = "UPDATE "
					+ tableName
					+ " AS a, tmp_"
					+ tableName
					+ " AS t SET a.visit_attribute_type_id=t.visit_attribute_type_id,a.name=t.name,a.description =t.description ,a.datatype=t.datatype,a.datatype_config =t.datatype_config ,a.preferred_handler =t.preferred_handler ,a.handler_config =t.handler_config ,a.min_occurs =t.min_occurs ,a.max_occurs=t.max_occurs,a.creator =t.creator ,a.date_created =t.date_created ,a.changed_by =t.changed_by ,a.date_changed =t.date_changed ,a.retired =t.retired ,a.retired_by=t.retired_by,a.date_retired =t.date_retired ,a.retire_reason=t.retire_reason WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.uuid = t.uuid";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);

			// Visit Attribute
			tableName = "visit_attribute";
			insertQuery = "INSERT INTO tmp_"
					+ tableName
					+ " (surrogate_id, implementation_id, visit_attribute_id ,visit_id,attribute_type_id,value_reference,uuid ,creator ,date_created,changed_by,date_changed ,voided,voided_by ,date_voided ,void_reason ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'"
					+ implementationId
					+ "',visit_attribute_id ,visit_id,attribute_type_id,value_reference,uuid ,creator ,date_created,changed_by,date_changed ,voided,voided_by ,date_voided ,void_reason FROM "
					+ database + "." + tableName + " AS t "
					+ filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database
					+ ".encounter_type into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery,
					remoteDb.getConnection(), targetDb.getConnection());
			// Insert new records
			insertQuery = "INSERT IGNORE INTO "
					+ tableName
					+ " SELECT * FROM tmp_"
					+ tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM "
					+ tableName
					+ " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			// Update the existing records
			updateQuery = "UPDATE "
					+ tableName
					+ " AS a, tmp_"
					+ tableName
					+ " AS t SET a.visit_attribute_id =t.visit_attribute_id ,a.visit_id=t.visit_id,a.attribute_type_id=t.attribute_type_id,a.value_reference=t.value_reference,a.uuid =t.uuid ,a.creator =t.creator ,a.date_created=t.date_created,a.changed_by=t.changed_by,a.date_changed =t.date_changed ,a.voided=t.voided,a.voided_by =t.voided_by ,a.date_voided =t.date_voided ,a.void_reason=t.void_reason WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.uuid = t.uuid";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Load data from form-related tables into data warehouse
	 * 
	 * @param implementationId
	 * @param database
	 * @throws ClassNotFoundException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	public void importFormData(DatabaseUtil remoteDb, int implementationId)
			throws InstantiationException, IllegalAccessException,
			ClassNotFoundException {

		String database = remoteDb.getDbName();
		String insertQuery;
		String updateQuery;
		String selectQuery;
		String tableName;

		try {
			// Field
			tableName = "field";
			insertQuery = "INSERT INTO tmp_"
					+ tableName
					+ " (surrogate_id, implementation_id, field_id, name, description, field_type, concept_id, table_name, attribute_name, default_value, select_multiple, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'"
					+ implementationId
					+ "', field_id, name, description, field_type, concept_id, table_name, attribute_name, default_value, select_multiple, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, uuid FROM "
					+ database + "." + tableName + " AS t ";// +
			// filter("t.date_created",
			// "t.date_changed");
			log.info("Inserting data from " + database
					+ ".encounter_type into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery,
					remoteDb.getConnection(), targetDb.getConnection());
			// Insert new records
			insertQuery = "INSERT IGNORE INTO "
					+ tableName
					+ " SELECT * FROM tmp_"
					+ tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM "
					+ tableName
					+ " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			// Update the existing records
			updateQuery = "UPDATE "
					+ tableName
					+ " AS a, tmp_"
					+ tableName
					+ " AS t SET a.field_id=t.field_id,a.name=t.name,a.description=t.description,a.field_type=t.field_type,a.concept_id=t.concept_id,a.table_name=t.table_name,a.attribute_name=t.attribute_name,a.default_value=t.default_value,a.select_multiple=t.select_multiple,a.creator=t.creator,a.date_created=t.date_created,a.changed_by=t.changed_by,a.date_changed=t.date_changed,a.retired=t.retired,a.retired_by=t.retired_by,a.date_retired=t.date_retired,a.retire_reason=t.retire_reason WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.uuid = t.uuid";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);

			// Field Answer
			tableName = "field_answer";
			insertQuery = "INSERT INTO tmp_"
					+ tableName
					+ " (surrogate_id, implementation_id,field_id ,answer_id ,creator, date_created,uuid) VALUES (?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId
					+ "',field_id ,answer_id ,creator, date_created,uuid FROM "
					+ database + "." + tableName + " AS t "
					+ filter("t.date_created", null);
			log.info("Inserting data from " + database
					+ ".encounter_type into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery,
					remoteDb.getConnection(), targetDb.getConnection());
			// Insert new records
			insertQuery = "INSERT IGNORE INTO "
					+ tableName
					+ " SELECT * FROM tmp_"
					+ tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM "
					+ tableName
					+ " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			// Update the existing records
			updateQuery = "UPDATE "
					+ tableName
					+ " AS a, tmp_"
					+ tableName
					+ " AS t SET a.field_id =t.field_id ,a.answer_id =t.answer_id ,a.creator=t.creator,a.date_created=t.date_created WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.uuid = t.uuid";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);

			// Form Field
			tableName = "form_field";
			insertQuery = "INSERT INTO tmp_"
					+ tableName
					+ " (surrogate_id, implementation_id,form_field_id ,form_id ,field_id ,field_number ,field_part ,page_number,parent_form_field ,min_occurs ,max_occurs ,required,changed_by ,date_changed,creator ,date_created ,sort_weight ,uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'"
					+ implementationId
					+ "', form_field_id ,form_id ,field_id ,field_number ,field_part ,page_number,parent_form_field ,min_occurs ,max_occurs ,required,changed_by ,date_changed,creator ,date_created ,sort_weight ,uuid  FROM "
					+ database + "." + tableName + " AS t "
					+ filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database
					+ ".encounter_type into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery,
					remoteDb.getConnection(), targetDb.getConnection());
			// Insert new records
			insertQuery = "INSERT IGNORE INTO "
					+ tableName
					+ " SELECT * FROM tmp_"
					+ tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM "
					+ tableName
					+ " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			// Update the existing records
			updateQuery = "UPDATE "
					+ tableName
					+ " AS a, tmp_"
					+ tableName
					+ " AS t SET a.form_field_id =t.form_field_id ,a.form_id =t.form_id ,a.field_id =t.field_id ,a.field_number =t.field_number ,a.field_part =t.field_part ,a.page_number=t.page_number,a.parent_form_field =t.parent_form_field ,a.min_occurs =t.min_occurs ,a.max_occurs =t.max_occurs ,a.required=t.required,a.changed_by =t.changed_by ,a.date_changed=t.date_changed,a.creator =t.creator ,a.date_created =t.date_created ,a.sort_weight =t.sort_weight WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.uuid = t.uuid";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);

			// Field Types
			tableName = "field_type";
			insertQuery = "INSERT INTO tmp_"
					+ tableName
					+ " (surrogate_id, implementation_id,field_type_id ,name ,description ,is_set ,creator ,date_created ,uuid) VALUES (?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'"
					+ implementationId
					+ "',field_type_id ,name ,description ,is_set ,creator ,date_created ,uuid FROM "
					+ database + "." + tableName + " AS t "
					+ filter("t.date_created", null);
			log.info("Inserting data from " + database
					+ ".encounter_type into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery,
					remoteDb.getConnection(), targetDb.getConnection());
			// Insert new records
			insertQuery = "INSERT IGNORE INTO "
					+ tableName
					+ " SELECT * FROM tmp_"
					+ tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM "
					+ tableName
					+ " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			// Update the existing records
			updateQuery = "UPDATE "
					+ tableName
					+ " AS a, tmp_"
					+ tableName
					+ " AS t SET a.field_type_id =t.field_type_id ,a.name =t.name ,a.description =t.description ,a.is_set =t.is_set ,a.creator =t.creator ,a.date_created =t.date_created WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.uuid = t.uuid";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
