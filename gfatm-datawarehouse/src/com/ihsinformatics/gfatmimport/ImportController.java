/* Copyright(C) 2016 Interactive Health Solutions, Pvt. Ltd.

This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License as
published by the Free Software Foundation; either version 3 of the License (GPLv3), or any later version.
This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

See the GNU General Public License for more details. You should have received a copy of the GNU General Public License along with this program; if not, write to the Interactive Health Solutions, info@ihsinformatics.com
You can also access the license on the internet at the address: http://www.gnu.org/licenses/gpl-3.0.html

Interactive Health Solutions, hereby disclaims all copyright interest in this program written by the contributors.
 */
package com.ihsinformatics.gfatmimport;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.Date;
import java.util.logging.Logger;

import com.ihsinformatics.util.CommandType;
import com.ihsinformatics.util.DatabaseUtil;
import com.ihsinformatics.util.DateTimeUtil;

/**
 * @author owais.hussain@ihsinformatics.com
 *
 */
public class ImportController {

	private static final Logger log = Logger.getLogger(Class.class.getName());
	private DatabaseUtil localDb;
	private DatabaseUtil remoteDb;
	private Date fromDate;
	private Date toDate;

	public ImportController(DatabaseUtil db) {
		this.localDb = db;
		this.fromDate = new Date();
		this.toDate = new Date();
	}

	/**
	 * Returns a filter for select queries
	 * 
	 * @param createDateName
	 * @param updateDateName
	 * @param fromDate
	 * @param toDate
	 * @return
	 */
	public String filter(String createDateName, String updateDateName) {
		StringBuilder filter = new StringBuilder(" WHERE 1=1 ");
		filter.append("AND (" + createDateName);
		filter.append(" BETWEEN TIMESTAMP('"
				+ DateTimeUtil.getSqlDateTime(fromDate) + "') ");
		filter.append("AND TIMESTAMP('" + DateTimeUtil.getSqlDateTime(toDate)
				+ "'))");
		if (updateDateName != null) {
			filter.append(" OR (" + updateDateName);
			filter.append(" BETWEEN TIMESTAMP('"
					+ DateTimeUtil.getSqlDateTime(fromDate) + "') ");
			filter.append("AND TIMESTAMP('" + DateTimeUtil.getSqlDateTime(toDate)
					+ "'))");
		}
		return filter.toString();
	}

	/**
	 * Fetch data from remote database and insert into local database
	 * 
	 * @param selectQuery
	 * @param insertQuery
	 * @return
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	public void remoteSelectInsert(String selectQuery, String insertQuery)
			throws SQLException, InstantiationException,
			IllegalAccessException, ClassNotFoundException {
		Connection remoteConnection = remoteDb.getConnection();
		Connection localConnection = localDb.getConnection();
		remoteSelectInsert(selectQuery, insertQuery, remoteConnection,
				localConnection);
	}

	/**
	 * Fetch data from source database and insert into target database
	 * 
	 * @param selectQuery
	 * @param insertQuery
	 * @param sourceConnection
	 * @param targetConnection
	 * @throws SQLException
	 */
	public void remoteSelectInsert(String selectQuery, String insertQuery,
			Connection sourceConnection, Connection targetConnection)
			throws SQLException {
		PreparedStatement source = sourceConnection
				.prepareStatement(selectQuery);
		PreparedStatement target = targetConnection
				.prepareStatement(insertQuery);
		ResultSet data = source.executeQuery();
		ResultSetMetaData metaData = data.getMetaData();
		while (data.next()) {
			for (int i = 1; i <= metaData.getColumnCount(); i++) {
				String value = data.getString(i);
				target.setString(i, value);
			}
			target.executeUpdate();
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
		String selectQuery;
		try {
			// Person Attribute Type
			insertQuery = "INSERT INTO person_attribute_type (surrogate_key, implementation_id, person_attribute_type_id, name, description, format, foreign_key, searchable, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, edit_privilege, sort_weight, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', person_attribute_type_id, name, description, format, foreign_key, searchable, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, edit_privilege, sort_weight, uuid FROM " + database + ".person_attribute_type AS t " + filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database + ".person_attribute_type into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			// Person
			insertQuery = "INSERT INTO person (surrogate_key, implementation_id, person_id, gender, birthdate, birthdate_estimated, dead, death_date, cause_of_death, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', person_id, gender, birthdate, birthdate_estimated, dead, death_date, cause_of_death, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason, uuid FROM " + database + ".person AS t " + filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database + ".person into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			// Person Address
			insertQuery = "INSERT INTO person_address (surrogate_key, implementation_id, person_address_id, person_id, preferred, address1, address2, city_village, state_province, postal_code, country, latitude, longitude, start_date, end_date, creator, date_created, voided, voided_by, date_voided, void_reason, county_district, address3, address4, address5, address6, date_changed, changed_by, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', person_address_id, person_id, preferred, address1, address2, city_village, state_province, postal_code, country, latitude, longitude, start_date, end_date, creator, date_created, voided, voided_by, date_voided, void_reason, county_district, address3, address4, address5, address6, date_changed, changed_by, uuid FROM " + database + ".person_address AS t " + filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database + ".person_address into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			// Person Attribute
			insertQuery = "INSERT INTO person_attribute (surrogate_key, implementation_id, person_attribute_id, person_id, value, person_attribute_type_id, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', person_attribute_id, person_id, value, person_attribute_type_id, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason, uuid FROM " + database + ".person_attribute AS t " + filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database + ".person_attribute into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			// Person Name
			insertQuery = "INSERT INTO person_name (surrogate_key, implementation_id, person_name_id, preferred, person_id, prefix, given_name, middle_name, family_name_prefix, family_name, family_name2, family_name_suffix, degree, creator, date_created, voided, voided_by, date_voided, void_reason, changed_by, date_changed, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', person_name_id, preferred, person_id, prefix, given_name, middle_name, family_name_prefix, family_name, family_name2, family_name_suffix, degree, creator, date_created, voided, voided_by, date_voided, void_reason, changed_by, date_changed, uuid FROM " + database + ".person_name AS t " + filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database + ".person_name into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
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
		String selectQuery;
		try {
			// Role
			insertQuery = "INSERT IGNORE INTO role (surrogate_key, implementation_id, role, description, uuid) VALUES (?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', role, description, uuid FROM " + database + ".role AS t ";
			log.info("Inserting data from " + database + ".role into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			// Role Role
			insertQuery = "INSERT IGNORE INTO role_role (surrogate_key, implementation_id, parent_role, child_role) VALUES (?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', parent_role, child_role FROM " + database + ".role_role AS t ";
			log.info("Inserting data from " + database + ".role_role into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			// Privilege
			insertQuery = "INSERT IGNORE INTO privilege (surrogate_key, implementation_id, privilege, description, uuid) VALUES (?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', privilege, description, uuid FROM " + database + ".privilege AS t ";
			log.info("Inserting data from " + database + ".privilege into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			// Role Privilege
			insertQuery = "INSERT IGNORE INTO role_privilege (surrogate_key, implementation_id, role, privilege) VALUES (?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', role, privilege FROM " + database + ".role_privilege AS t ";
			log.info("Inserting data from " + database + ".role_privilege into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			// Users
			insertQuery = "INSERT INTO users (surrogate_key, implementation_id, user_id, system_id, username, password, salt, secret_question, secret_answer, creator, date_created, changed_by, date_changed, person_id, retired, retired_by, date_retired, retire_reason, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', user_id, system_id, username, password, salt, secret_question, secret_answer, creator, date_created, changed_by, date_changed, person_id, retired, retired_by, date_retired, retire_reason, uuid FROM " + database + ".users AS t " + filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database + ".users into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			// User Property
			insertQuery = "INSERT IGNORE INTO user_property (surrogate_key, implementation_id, user_id, property, property_value) VALUES (?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', user_id, property, property_value FROM " + database + ".user_property AS t ";
			log.info("Inserting data from " + database + ".user_property into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			// User Role
			insertQuery = "INSERT IGNORE INTO user_role (surrogate_key, implementation_id, user_id, role) VALUES (?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', user_id, role FROM " + database + ".user_role AS t ";
			log.info("Inserting data from " + database + ".user_role into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			// Provider Attribute Type
			insertQuery = "INSERT INTO provider_attribute_type (surrogate_key, implementation_id, provider_attribute_type_id, name, description, datatype, datatype_config, preferred_handler, handler_config, min_occurs, max_occurs, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', provider_attribute_type_id, name, description, datatype, datatype_config, preferred_handler, handler_config, min_occurs, max_occurs, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, uuid FROM " + database + ".provider_attribute_type AS t " + filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database + ".provider_attribute_type into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			// Provider
			insertQuery = "INSERT INTO provider (surrogate_key, implementation_id, provider_id, person_id, name, identifier, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', provider_id, person_id, name, identifier, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, uuid FROM " + database + ".provider AS t " + filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database + ".provider into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			// Provider Attribute
			insertQuery = "INSERT INTO provider_attribute (surrogate_key, implementation_id, provider_attribute_id, provider_id, attribute_type_id, value_reference, uuid, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', provider_attribute_id, provider_id, attribute_type_id, value_reference, uuid, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason FROM " + database + ".provider_attribute AS t " + filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database + ".provider_attribute into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
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
		String selectQuery;
		try {
			// Location Attribute Type
			insertQuery = "INSERT INTO location_attribute_type (surrogate_key, implementation_id, location_attribute_type_id, name, description, datatype, datatype_config, preferred_handler, handler_config, min_occurs, max_occurs, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', location_attribute_type_id, name, description, datatype, datatype_config, preferred_handler, handler_config, min_occurs, max_occurs, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, uuid FROM " + database + ".location_attribute_type AS t " + filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database + ".location_attribute_type into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			// Location
			insertQuery = "INSERT INTO location (surrogate_key, implementation_id, location_id, name, description, address1, address2, city_village, state_province, postal_code, country, latitude, longitude, creator, date_created, county_district, address3, address4, address5, address6, retired, retired_by, date_retired, retire_reason, parent_location, uuid, changed_by, date_changed) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', location_id, name, description, address1, address2, city_village, state_province, postal_code, country, latitude, longitude, creator, date_created, county_district, address3, address4, address5, address6, retired, retired_by, date_retired, retire_reason, parent_location, uuid, changed_by, date_changed FROM " + database + ".location AS t " + filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database + ".location into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			// Location Attribute
			insertQuery = "INSERT INTO location_attribute (surrogate_key, implementation_id, location_attribute_id, location_id, attribute_type_id, value_reference, uuid, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', location_attribute_id, location_id, attribute_type_id, value_reference, uuid, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason FROM " + database + ".location_attribute AS t " + filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database + ".location_attribute into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			// Location Tag
			insertQuery = "INSERT INTO location_tag (surrogate_key, implementation_id, location_tag_id, name, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid, changed_by, date_changed) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', location_tag_id, name, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid, changed_by, date_changed FROM " + database + ".location_tag AS t " + filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database + ".location_tag into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			// Location Tag Map
			insertQuery = "INSERT IGNORE INTO location_tag_map (surrogate_key, implementation_id, location_id, location_tag_id) VALUES (?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', location_id, location_tag_id FROM " + database + ".location_tag_map AS t ";
			log.info("Inserting data from " + database + ".location_tag_map into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
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
		String selectQuery;
		try {
			// Concept Class
			insertQuery = "INSERT INTO concept_class (surrogate_key, implementation_id, concept_class_id, name, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', concept_class_id, name, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid FROM " + database + ".concept_class AS t " + filter("t.date_created", null);
			log.info("Inserting data from " + database + ".concept_class into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			// Concept Set
			insertQuery = "INSERT INTO concept_set (surrogate_key, implementation_id, concept_set_id, concept_id, concept_set, sort_weight, creator, date_created, uuid) VALUES (?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', concept_set_id, concept_id, concept_set, sort_weight, creator, date_created, uuid FROM " + database + ".concept_set AS t " + filter("t.date_created", null);
			log.info("Inserting data from " + database + ".concept_set into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			// Concept Data Type
			insertQuery = "INSERT INTO concept_datatype (surrogate_key, implementation_id, concept_datatype_id, name, hl7_abbreviation, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', concept_datatype_id, name, hl7_abbreviation, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid FROM " + database + ".concept_datatype AS t " + filter("t.date_created", null);
			log.info("Inserting data from " + database + ".concept_datatype into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			// Concept Map Type
			insertQuery = "INSERT INTO concept_map_type (surrogate_key, implementation_id, concept_map_type_id, name, description, creator, date_created, changed_by, date_changed, is_hidden, retired, retired_by, date_retired, retire_reason, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', concept_map_type_id, name, description, creator, date_created, changed_by, date_changed, is_hidden, retired, retired_by, date_retired, retire_reason, uuid FROM " + database + ".concept_map_type AS t " + filter("t.date_created", null);
			log.info("Inserting data from " + database + ".concept_map_type into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			// Concept Stop Word
			insertQuery = "INSERT IGNORE INTO concept_stop_word (surrogate_key, implementation_id, concept_stop_word_id, word, locale, uuid) VALUES (?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', concept_stop_word_id, word, locale, uuid FROM " + database + ".concept_stop_word AS t ";
			log.info("Inserting data from " + database + ".concept_stop_word into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			// Concept
			insertQuery = "INSERT INTO concept (surrogate_key, implementation_id, concept_id, retired, short_name, description, form_text, datatype_id, class_id, is_set, creator, date_created, version, changed_by, date_changed, retired_by, date_retired, retire_reason, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', concept_id, retired, short_name, description, form_text, datatype_id, class_id, is_set, creator, date_created, version, changed_by, date_changed, retired_by, date_retired, retire_reason, uuid FROM " + database + ".concept AS t " + filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database + ".concept into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			// Concept Name
			insertQuery = "INSERT INTO concept_name (surrogate_key, implementation_id, concept_id, name, locale, creator, date_created, concept_name_id, voided, voided_by, date_voided, void_reason, uuid, concept_name_type, locale_preferred) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', concept_id, name, locale, creator, date_created, concept_name_id, voided, voided_by, date_voided, void_reason, uuid, concept_name_type, locale_preferred FROM " + database + ".concept_name AS t " + filter("t.date_created", null);
			log.info("Inserting data from " + database + ".concept_name into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			// Concept Description
			insertQuery = "INSERT INTO concept_description (surrogate_key, implementation_id, concept_description_id, concept_id, description, locale, creator, date_created, changed_by, date_changed, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', concept_description_id, concept_id, description, locale, creator, date_created, changed_by, date_changed, uuid FROM " + database + ".concept_description AS t " + filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database + ".concept_description into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			// Concept Answer
			insertQuery = "INSERT INTO concept_answer (surrogate_key, implementation_id, concept_answer_id, concept_id, answer_concept, answer_drug, creator, date_created, uuid, sort_weight) VALUES (?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', concept_answer_id, concept_id, answer_concept, answer_drug, creator, date_created, uuid, sort_weight FROM " + database + ".concept_answer AS t " + filter("t.date_created", null);
			log.info("Inserting data from " + database + ".concept_answer into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			// Concept Numeric
			insertQuery = "INSERT IGNORE INTO concept_numeric (surrogate_key, implementation_id, concept_id, hi_absolute, hi_critical, hi_normal, low_absolute, low_critical, low_normal, units, precise, display_precision) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', concept_id, hi_absolute, hi_critical, hi_normal, low_absolute, low_critical, low_normal, units, precise, display_precision FROM " + database + ".concept_numeric AS t WHERE t.hi_absolute IS NOT NULL OR t.hi_critical IS NOT NULL OR t.hi_normal IS NOT NULL OR t.low_absolute IS NOT NULL OR t.low_critical IS NOT NULL OR t.low_normal IS NOT NULL OR t.units IS NOT NULL";
			log.info("Inserting data from " + database + ".concept_numeric into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
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
	public void insertPatientData(DatabaseUtil remoteDb, int implementationId)
			throws InstantiationException, IllegalAccessException,
			ClassNotFoundException {
		String database = remoteDb.getDbName();
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
		localDb.runCommand(CommandType.INSERT, query.toString());

		String[] tables = { "patient_identifier_type", "patient_identifier" };
		for (String table : tables) {
			query = new StringBuilder();
			query.append("INSERT INTO " + table + " ");
			query.append("SELECT 0,'" + implementationId + "', t.* FROM "
					+ database + "." + table + " AS t ");
			query.append("WHERE t.uuid NOT IN (SELECT uuid FROM " + table + ")");
			log.info("Inserting data from " + database + "." + table
					+ " into data warehouse");
			localDb.runCommand(CommandType.INSERT, query.toString());
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
	public void insertEncounterData(DatabaseUtil remoteDb, int implementationId)
			throws InstantiationException, IllegalAccessException,
			ClassNotFoundException {
		String database = remoteDb.getDbName();
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
			localDb.runCommand(CommandType.INSERT, query.toString());
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
	public void updatePeopleData(DatabaseUtil remoteDb, Integer implementationId)
			throws InstantiationException, IllegalAccessException,
			ClassNotFoundException {
		String database = remoteDb.getDbName();
		StringBuilder query = new StringBuilder();
		query.append("UPDATE "
				+ database
				+ ".person SET creator = 1 WHERE creator NOT IN (SELECT user_id FROM users)");
		log.info("Updating people data in data warehouse");
		localDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE person_attribute_type AS a, " + database
				+ ".person_attribute_type AS t ");
		query.append("SET a.name = t.name, a.description = t.description, a.format = t.format, a.foreign_key = t.foreign_key, a.searchable = t.searchable, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.retired = t.retired, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retire_reason = t.retire_reason, a.edit_privilege = t.edit_privilege, a.sort_weight = t.sort_weight ");
		query.append("WHERE a.person_attribute_type_id = t.person_attribute_type_id AND a.uuid = t.uuid");
		localDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE person AS a, " + database + ".person AS t ");
		query.append("SET a.gender = t.gender, a.birthdate = t.birthdate, a.birthdate_estimated = t.birthdate_estimated, a.dead = t.dead, a.death_date = t.death_date, a.cause_of_death = t.cause_of_death, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.voided = t.voided, a.voided_by = t.voided_by, a.date_voided = t.date_voided, a.void_reason = t.void_reason ");
		query.append("WHERE a.person_id = t.person_id AND a.uuid = t.uuid");
		localDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE person_address AS a, " + database
				+ ".person_address AS t ");
		query.append("SET a.person_id = t.person_id, a.preferred = t.preferred, a.address1 = t.address1, a.address2 = t.address2, a.city_village = t.city_village, a.state_province = t.state_province, a.postal_code = t.postal_code, a.country = t.country, a.latitude = t.latitude, a.longitude = t.longitude, a.start_date = t.start_date, a.end_date = t.end_date, a.creator = t.creator, a.date_created = t.date_created, a.voided = t.voided, a.voided_by = t.voided_by, a.date_voided = t.date_voided, a.void_reason = t.void_reason, a.county_district = t.county_district, a.address3 = t.address3, a.address4 = t.address4, a.address5 = t.address5, a.address6 = t.address6, a.date_changed = t.date_changed, a.changed_by = t.changed_by ");
		query.append("WHERE a.person_address_id = t.person_address_id AND a.uuid = t.uuid");
		localDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE person_attribute AS a, " + database
				+ ".person_attribute AS t ");
		query.append("SET a.person_id = t.person_id, a.value = t.value, a.person_attribute_type_id = t.person_attribute_type_id, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.voided = t.voided, a.voided_by = t.voided_by, a.date_voided = t.date_voided, a.void_reason = t.void_reason ");
		query.append("WHERE a.person_attribute_id = t.person_attribute_id AND a.uuid = t.uuid");
		localDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE person_name AS a, " + database
				+ ".person_name AS t ");
		query.append("SET a.preferred = t.preferred, a.person_id = t.person_id, a.prefix = t.prefix, a.given_name = t.given_name, a.middle_name = t.middle_name, a.family_name_prefix = t.family_name_prefix, a.family_name = t.family_name, a.family_name2 = t.family_name2, a.family_name_suffix = t.family_name_suffix, a.degree = t.degree, a.creator = t.creator, a.date_created = t.date_created, a.voided = t.voided, a.voided_by = t.voided_by, a.date_voided = t.date_voided, a.void_reason = t.void_reason, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.uuid = t.uuid ");
		query.append("WHERE a.person_name_id = t.person_name_id AND a.uuid = t.uuid");
		localDb.runCommand(CommandType.UPDATE, query.toString());
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
	public void updateUserData(DatabaseUtil remoteDb, Integer implementationId)
			throws InstantiationException, IllegalAccessException,
			ClassNotFoundException {
		String database = remoteDb.getDbName();
		StringBuilder query = new StringBuilder();
		query.append("UPDATE role AS a, " + database + ".role AS t ");
		query.append("SET a.description = t.description ");
		query.append("WHERE a.role = t.role AND a.uuid = t.uuid");
		localDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE users AS a, " + database + ".users AS t ");
		query.append("SET a.implementation_id = t.implementation_id, a.username = t.username, a.password = t.password, a.salt = t.salt, a.secret_question = t.secret_question, a.secret_answer = t.secret_answer, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.person_id = t.person_id, a.retired = t.retired, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retire_reason = t.retire_reason ");
		query.append("WHERE a.user_id = t.user_id AND a.uuid = t.uuid");
		localDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE user_property AS a, " + database
				+ ".user_property AS t ");
		query.append("SET a.property_value = t.property_value ");
		query.append("WHERE a.user_id = t.user_id AND a.property = t.property AND a.implementation_id = "
				+ implementationId);
		localDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE provider AS a, " + database + ".provider AS t ");
		query.append("SET a.person_id = t.person_id, a.name = t.name, a.identifier = t.identifier, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.retired = t.retired, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retire_reason = t.retire_reason ");
		query.append("WHERE a.provider_id = t.provider_id AND a.uuid = t.uuid");
		localDb.runCommand(CommandType.UPDATE, query.toString());
	}

	/**
	 * Update data from location-related tables into data warehouse
	 * 
	 * @param database
	 * @throws ClassNotFoundException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	public void updateLocationData(DatabaseUtil remoteDb,
			Integer implementationId) throws InstantiationException,
			IllegalAccessException, ClassNotFoundException {
		String database = remoteDb.getDbName();
		StringBuilder query = new StringBuilder();
		query.append("UPDATE location_attribute_type AS a, " + database
				+ ".location_attribute_type AS t ");
		query.append("SET a.name = t.name, a.description = t.description, a.datatype = t.datatype, a.datatype_config = t.datatype_config, a.preferred_handler = t.preferred_handler, a.handler_config = t.handler_config, a.min_occurs = t.min_occurs, a.max_occurs = t.max_occurs, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.retired = t.retired, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retire_reason = t.retire_reason ");
		query.append("WHERE a.location_attribute_type_id = t.location_attribute_type_id AND a.uuid = t.uuid");
		localDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE location AS a, " + database + ".location AS t ");
		query.append("SET a.name = t.name, a.description = t.description, a.address1 = t.address1, a.address2 = t.address2, a.city_village = t.city_village, a.state_province = t.state_province, a.postal_code = t.postal_code, a.country = t.country, a.latitude = t.latitude, a.longitude = t.longitude, a.creator = t.creator, a.date_created = t.date_created, a.county_district = t.county_district, a.address3 = t.address3, a.address4 = t.address4, a.address5 = t.address5, a.address6 = t.address6, a.retired = t.retired, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retire_reason = t.retire_reason, a.parent_location = t.parent_location ");
		query.append("WHERE a.location_id = t.location_id AND a.uuid = t.uuid");
		localDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE location_attribute AS a, " + database
				+ ".location_attribute AS t ");
		query.append("SET a.attribute_type_id = t.attribute_type_id, a.value_reference = t.value_reference, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.voided = t.voided, a.voided_by = t.voided_by, a.date_voided = t.date_voided, a.void_reason = t.void_reason ");
		query.append("WHERE a.location_attribute_id = t.location_attribute_id AND a.location_id = t.location_id AND a.uuid = t.uuid");
		localDb.runCommand(CommandType.UPDATE, query.toString());
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
	public void updateConceptData(DatabaseUtil remoteDb,
			Integer implementationId) throws InstantiationException,
			IllegalAccessException, ClassNotFoundException {
		String database = remoteDb.getDbName();
		StringBuilder query = new StringBuilder();
		query.append("UPDATE concept_class AS a, " + database
				+ ".concept_class AS t ");
		query.append("SET a.name = t.name, a.description = t.description, a.creator = t.creator, a.date_created = t.date_created, a.retired = t.retired, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retire_reason = t.retire_reason ");
		query.append("WHERE a.concept_class_id = t.concept_class_id AND a.uuid = t.uuid");
		localDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE concept_datatype AS a, " + database
				+ ".concept_datatype AS t ");
		query.append("SET a.name = t.name, a.hl7_abbreviation = t.hl7_abbreviation, a.description = t.description, a.creator = t.creator, a.date_created = t.date_created, a.retired = t.retired, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retire_reason = t.retire_reason ");
		query.append("WHERE a.concept_datatype_id = t.concept_datatype_id AND a.uuid = t.uuid");
		localDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE concept_map_type AS a, " + database
				+ ".concept_map_type AS t ");
		query.append("SET a.name = t.name, a.description = t.description, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.is_hidden = t.is_hidden, a.retired = t.retired, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retire_reason = t.retire_reason ");
		query.append("WHERE a.concept_map_type_id = t.concept_map_type_id AND a.uuid = t.uuid");
		localDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE concept AS a, " + database + ".concept AS t ");
		query.append("SET a.retired = t.retired, a.short_name = t.short_name, a.description = t.description, a.form_text = t.form_text, a.datatype_id = t.datatype_id, a.class_id = t.class_id, a.is_set = t.is_set, a.creator = t.creator, a.date_created = t.date_created, a.version = t.version, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retire_reason = t.retire_reason ");
		query.append("WHERE a.concept_id = t.concept_id AND a.uuid = t.uuid");
		localDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE concept_name AS a, " + database
				+ ".concept_name AS t ");
		query.append("SET a.concept_id = t.concept_id, a.name = t.name, a.locale = t.locale, a.locale_preferred = t.locale_preferred, a.creator = t.creator, a.date_created = t.date_created, a.concept_name_type = t.concept_name_type, a.voided = t.voided, a.voided_by = t.voided_by, a.date_voided = t.date_voided, a.void_reason = t.void_reason ");
		query.append("WHERE a.concept_name_id = t.concept_name_id AND a.uuid = t.uuid");
		localDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE concept_numeric AS a, " + database
				+ ".concept_numeric AS t ");
		query.append("SET a.hi_absolute = t.hi_absolute, a.hi_critical = t.hi_critical, a.hi_normal = t.hi_normal, a.low_absolute = t.low_absolute, a.low_critical = t.low_critical, a.low_normal = t.low_normal, a.units = t.units, a.precise = t.precise ");
		query.append("WHERE a.concept_id = t.concept_id AND a.implementation_id="
				+ implementationId);
		localDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE concept_description AS a, " + database
				+ ".concept_description AS t ");
		query.append("SET a.concept_id = t.concept_id, a.description = t.description, a.locale = t.locale, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed ");
		query.append("WHERE a.concept_description_id = t.concept_description_id AND a.uuid = t.uuid");
		localDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE concept_answer AS a, " + database
				+ ".concept_answer AS t ");
		query.append("SET a.concept_id = t.concept_id, a.answer_concept = t.answer_concept, a.answer_drug = t.answer_drug, a.creator = t.creator, a.date_created = t.date_created, a.sort_weight = t.sort_weight ");
		query.append("WHERE a.concept_answer_id = t.concept_answer_id AND a.uuid = t.uuid");
		localDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE concept_reference_map AS a, " + database
				+ ".concept_reference_map AS t ");
		query.append("SET a.concept_reference_term_id = t.concept_reference_term_id, a.concept_map_type_id = t.concept_map_type_id, a.creator = t.creator, a.date_created = t.date_created, a.concept_id = t.concept_id, a.changed_by = t.changed_by, a.date_changed = t.date_changed ");
		query.append("WHERE a.concept_map_id = t.concept_map_id AND a.uuid = t.uuid");
		localDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE concept_reference_term AS a, " + database
				+ ".concept_reference_term AS t ");
		query.append("SET a.concept_source_id = t.concept_source_id, a.name = t.name, a.code = t.code, a.version = t.version, a.description = t.description, a.creator = t.creator, a.date_created = t.date_created, a.date_changed = t.date_changed, a.changed_by = t.changed_by, a.retired = t.retired, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retire_reason = t.retire_reason ");
		query.append("WHERE a.concept_reference_term_id = t.concept_reference_term_id AND a.uuid = t.uuid");
		localDb.runCommand(CommandType.UPDATE, query.toString());
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
	public void updatePatientData(DatabaseUtil remoteDb,
			Integer implementationId) throws InstantiationException,
			IllegalAccessException, ClassNotFoundException {
		String database = remoteDb.getDbName();
		StringBuilder query = new StringBuilder();
		query.append("UPDATE patient AS a, " + database + ".patient AS t ");
		query.append("SET a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.voided = t.voided, a.voided_by = t.voided_by, a.date_voided = t.date_voided, a.void_reason = t.void_reason ");
		query.append("WHERE a.patient_id = t.patient_id and a.implementation_id = "
				+ implementationId);
		localDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE patient_identifier AS a, " + database
				+ ".patient_identifier AS t ");
		query.append("SET a.patient_id = t.patient_id, a.identifier = t.identifier, a.identifier_type = t.identifier_type, a.preferred = t.preferred, a.location_id = t.location_id, a.creator = t.creator, a.date_created = t.date_created, a.date_changed = t.date_changed, a.changed_by = t.changed_by, a.voided = t.voided, a.voided_by = t.voided_by, a.date_voided = t.date_voided, a.void_reason = t.void_reason ");
		query.append("WHERE a.patient_identifier_id = t.patient_identifier_id AND a.uuid = t.uuid");
		localDb.runCommand(CommandType.UPDATE, query.toString());
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
	public void updateEncounterData(DatabaseUtil remoteDb,
			Integer implementationId) throws InstantiationException,
			IllegalAccessException, ClassNotFoundException {
		String database = remoteDb.getDbName();
		StringBuilder query = new StringBuilder();
		query.append("UPDATE encounter_type AS a, " + database
				+ ".encounter_type AS t ");
		query.append("SET a.name = t.name, a.description = t.description, a.creator = t.creator, a.date_created = t.date_created, a.retired = t.retired, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retire_reason = t.retire_reason ");
		query.append("WHERE a.encounter_type_id = t.encounter_type_id AND a.uuid = t.uuid");
		localDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE form AS a, " + database + ".form AS t ");
		query.append("SET a.name = t.name, a.version = t.version, a.build = t.build, a.published = t.published, a.xslt = t.xslt, a.template = t.template, a.description = t.description, a.encounter_type = t.encounter_type, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.retired = t.retired, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retired_reason = t.retired_reason ");
		query.append("WHERE a.form_id = t.form_id AND a.uuid = t.uuid");
		localDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE encounter_role AS a, " + database
				+ ".encounter_role AS t ");
		query.append("SET a.name = t.name, a.description = t.description, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.retired = t.retired, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retire_reason = t.retire_reason ");
		query.append("WHERE a.encounter_role_id = t.encounter_role_id AND a.uuid = t.uuid");
		localDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE encounter AS a, " + database + ".encounter AS t ");
		query.append("SET a.encounter_type = t.encounter_type, a.patient_id = t.patient_id, a.location_id = t.location_id, a.form_id = t.form_id, a.encounter_datetime = t.encounter_datetime, a.creator = t.creator, a.date_created = t.date_created, a.voided = t.voided, a.voided_by = t.voided_by, a.date_voided = t.date_voided, a.void_reason = t.void_reason, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.visit_id = t.visit_id ");
		query.append("WHERE a.encounter_id = t.encounter_id AND a.uuid = t.uuid");
		localDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE encounter_provider AS a, " + database
				+ ".encounter_provider AS t ");
		query.append("SET a.encounter_id = t.encounter_id, a.provider_id = t.provider_id, a.encounter_role_id = t.encounter_role_id, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.voided = t.voided, a.date_voided = t.date_voided, a.voided_by = t.voided_by, a.void_reason = t.void_reason ");
		query.append("WHERE a.encounter_provider_id = t.encounter_provider_id AND a.uuid = t.uuid");
		localDb.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder();
		query.append("UPDATE obs AS a, " + database + ".obs AS t ");
		query.append("SET a.person_id = t.person_id, a.concept_id = t.concept_id, a.encounter_id = t.encounter_id, a.order_id = t.order_id, a.obs_datetime = t.obs_datetime, a.location_id = t.location_id, a.obs_group_id = t.obs_group_id, a.accession_number = t.accession_number, a.value_group_id = t.value_group_id, a.value_boolean = t.value_boolean, a.value_coded = t.value_coded, a.value_coded_name_id = t.value_coded_name_id, a.value_drug = t.value_drug, a.value_datetime = t.value_datetime, a.value_numeric = t.value_numeric, a.value_modifier = t.value_modifier, a.value_text = t.value_text, a.value_complex = t.value_complex, a.comments = t.comments, a.creator = t.creator, a.date_created = t.date_created, a.voided = t.voided, a.voided_by = t.voided_by, a.date_voided = t.date_voided, a.void_reason = t.void_reason, a.previous_version = t.previous_version ");
		query.append("WHERE a.obs_id = t.obs_id AND a.uuid = t.uuid");
		localDb.runCommand(CommandType.UPDATE, query.toString());
	}

	/**
	 * Insert data from all sources into data warehouse
	 * 
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 * @throws ParseException 
	 */
	public void importData(boolean insertOnly) throws InstantiationException,
			IllegalAccessException, ClassNotFoundException, SQLException, ParseException {
		// Fetch source databases from _implementation table
		Object[][] sources = localDb
				.getTableData(
						"_implementation",
						"implementation_id,connection_url,driver,db_name,username,password,last_updated",
						"active=1 AND status<>'RUNNING'");
		// For each source, generate CSV files of all data
		for (Object[] source : sources) {
			int implementationId = Integer.parseInt(source[0].toString());
			String url = source[1].toString();
			String driverName = source[2].toString();
			String dbName = source[3].toString();
			String userName = source[4].toString();
			String password = source[5].toString();
			if (source[6] == null) {
				source[6] = new String("2000-01-01 00:00:00");
			}
			String lastUpdated = source[6].toString();
			fromDate = DateTimeUtil.getDateFromString(lastUpdated, DateTimeUtil.SQL_DATETIME);
			remoteDb = new DatabaseUtil();
			remoteDb.setConnection(url, dbName, driverName, userName, password);
			remoteDb.getConnection();
			// Import data from this connection into data warehouse
			try {
				// Update status of implementation record
// TODO: Enable on production				localDb.updateRecord("_implementation", new String[] {"status"}, new String[] {"RUNNING"}, "implementation_id='" + implementationId + "'");
//				importPeopleData(remoteDb, implementationId);
//				importUserData(remoteDb, implementationId);
//				importLocationData(remoteDb, implementationId);
				importConceptData(remoteDb, implementationId);
/*				insertPatientData(remoteDb, implementationId);
				insertEncounterData(remoteDb, implementationId);
				if (!insertOnly) {
					updatePeopleData(remoteDb, implementationId);
					updateUserData(remoteDb, implementationId);
					updateLocationData(remoteDb, implementationId);
					updateConceptData(remoteDb, implementationId);
					updatePatientData(remoteDb, implementationId);
					updateEncounterData(remoteDb, implementationId);
				}				
*/				// Update the status in _implementation table
				localDb.updateRecord("_implementation", new String[] {"last_updated"}, new String[] {DateTimeUtil.getSqlDateTime(new Date())}, "implementation_id='" + implementationId + "'");
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				localDb.updateRecord("_implementation", new String[] {"status"}, new String[] {"STOPPED"}, "implementation_id='" + implementationId + "'");
			}
		}
	}

	@Deprecated
	public void importMetadata() {
		log.info("Importing metadata from remote source");
		try {
			String selectQuery = "";
			String insertQuery = "";
			// active_list_type
			insertQuery = "INSERT INTO openmrs.active_list_type(active_list_type_id,name,description,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT active_list_type.active_list_type_id,active_list_type.name,active_list_type.description,active_list_type.creator,active_list_type.date_created,active_list_type.retired,active_list_type.retired_by,active_list_type.date_retired,active_list_type.retire_reason,active_list_type.uuid FROM openmrs.active_list_type";
			remoteSelectInsert(selectQuery, insertQuery);
			// concept
			insertQuery = "INSERT INTO openmrs.concept(concept_id,retired,short_name,description,form_text,datatype_id,class_id,is_set,creator,date_created,version,changed_by,date_changed,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT concept.concept_id,concept.retired,concept.short_name,concept.description,concept.form_text,concept.datatype_id,concept.class_id,concept.is_set,concept.creator,concept.date_created,concept.version,concept.changed_by,concept.date_changed,concept.retired_by,concept.date_retired,concept.retire_reason,concept.uuid FROM openmrs.concept";
			remoteSelectInsert(selectQuery, insertQuery);
			// concept_answer
			insertQuery = "INSERT INTO openmrs.concept_answer(concept_answer_id,concept_id,answer_concept,answer_drug,creator,date_created,uuid,sort_weight)VALUES(?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT concept_answer.concept_answer_id,concept_answer.concept_id,concept_answer.answer_concept,concept_answer.answer_drug,concept_answer.creator,concept_answer.date_created,concept_answer.uuid,concept_answer.sort_weightFROM openmrs.concept_answer";
			remoteSelectInsert(selectQuery, insertQuery);
			// concept_class
			insertQuery = "INSERT INTO openmrs.concept_class(concept_class_id,name,description,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT concept_class.concept_class_id,concept_class.name,concept_class.description,concept_class.creator,concept_class.date_created,concept_class.retired,concept_class.retired_by,concept_class.date_retired,concept_class.retire_reason,concept_class.uuid FROM openmrs.concept_class";
			remoteSelectInsert(selectQuery, insertQuery);
			// concept_datatype
			insertQuery = "INSERT INTO openmrs.concept_datatype(concept_datatype_id,name,hl7_abbreviation,description,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT concept_datatype.concept_datatype_id,concept_datatype.name,concept_datatype.hl7_abbreviation,concept_datatype.description,concept_datatype.creator,concept_datatype.date_created,concept_datatype.retired,concept_datatype.retired_by,concept_datatype.date_retired,concept_datatype.retire_reason,concept_datatype.uuid FROM openmrs.concept_datatype";
			remoteSelectInsert(selectQuery, insertQuery);
			// concept_description
			insertQuery = "INSERT INTO openmrs.concept_description(concept_description_id,concept_id,description,locale,creator,date_created,changed_by,date_changed,uuid)VALUES(?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT concept_description.concept_description_id,concept_description.concept_id,concept_description.description,concept_description.locale,concept_description.creator,concept_description.date_created,concept_description.changed_by,concept_description.date_changed,concept_description.uuid FROM openmrs.concept_description";
			remoteSelectInsert(selectQuery, insertQuery);
			// concept_map_type
			insertQuery = "INSERT INTO openmrs.concept_map_type(concept_map_type_id,name,description,creator,date_created,changed_by,date_changed,is_hidden,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT concept_map_type.concept_map_type_id,concept_map_type.name,concept_map_type.description,concept_map_type.creator,concept_map_type.date_created,concept_map_type.changed_by,concept_map_type.date_changed,concept_map_type.is_hidden,concept_map_type.retired,concept_map_type.retired_by,concept_map_type.date_retired,concept_map_type.retire_reason,concept_map_type.uuid FROM openmrs.concept_map_type";
			remoteSelectInsert(selectQuery, insertQuery);
			// concept_name
			insertQuery = "INSERT INTO openmrs.concept_name(concept_id,name,locale,creator,date_created,concept_name_id,voided,voided_by,date_voided,void_reason,uuid,concept_name_type,locale_preferred)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT concept_name.concept_id,concept_name.name,concept_name.locale,concept_name.creator,concept_name.date_created,concept_name.concept_name_id,concept_name.voided,concept_name.voided_by,concept_name.date_voided,concept_name.void_reason,concept_name.uuid,concept_name.concept_name_type,concept_name.locale_preferredFROM openmrs.concept_name";
			remoteSelectInsert(selectQuery, insertQuery);
			// concept_numeric
			insertQuery = "INSERT INTO openmrs.concept_numeric(concept_id,hi_absolute,hi_critical,hi_normal,low_absolute,low_critical,low_normal,units,precise,display_precision)VALUES(?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT concept_numeric.concept_id,concept_numeric.hi_absolute,concept_numeric.hi_critical,concept_numeric.hi_normal,concept_numeric.low_absolute,concept_numeric.low_critical,concept_numeric.low_normal,concept_numeric.units,concept_numeric.precise,concept_numeric.display_precisionFROM openmrs.concept_numeric";
			remoteSelectInsert(selectQuery, insertQuery);
			// concept_reference_map
			insertQuery = "INSERT INTO openmrs.concept_reference_map(concept_map_id,creator,date_created,concept_id,uuid,concept_reference_term_id,concept_map_type_id,changed_by,date_changed)VALUES(?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT concept_reference_map.concept_map_id,concept_reference_map.creator,concept_reference_map.date_created,concept_reference_map.concept_id,concept_reference_map.uuid,concept_reference_map.concept_reference_term_id,concept_reference_map.concept_map_type_id,concept_reference_map.changed_by,concept_reference_map.date_changedFROM openmrs.concept_reference_map";
			remoteSelectInsert(selectQuery, insertQuery);
			// concept_reference_source
			insertQuery = "INSERT INTO openmrs.concept_reference_source(concept_source_id,name,description,hl7_code,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT concept_reference_source.concept_source_id,concept_reference_source.name,concept_reference_source.description,concept_reference_source.hl7_code,concept_reference_source.creator,concept_reference_source.date_created,concept_reference_source.retired,concept_reference_source.retired_by,concept_reference_source.date_retired,concept_reference_source.retire_reason,concept_reference_source.uuid FROM openmrs.concept_reference_source";
			remoteSelectInsert(selectQuery, insertQuery);
			// concept_reference_map
			insertQuery = "INSERT INTO openmrs.concept_reference_term(concept_reference_term_id,concept_source_id,name,code,version,description,creator,date_created,date_changed,changed_by,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT concept_reference_term.concept_reference_term_id,concept_reference_term.concept_source_id,concept_reference_term.name,concept_reference_term.code,concept_reference_term.version,concept_reference_term.description,concept_reference_term.creator,concept_reference_term.date_created,concept_reference_term.date_changed,concept_reference_term.changed_by,concept_reference_term.retired,concept_reference_term.retired_by,concept_reference_term.date_retired,concept_reference_term.retire_reason,concept_reference_term.uuid FROM openmrs.concept_reference_term";
			remoteSelectInsert(selectQuery, insertQuery);
			// concept_set
			insertQuery = "INSERT INTO openmrs.concept_set(concept_set_id,concept_id,concept_set,sort_weight,creator,date_created,uuid)VALUES(?,?,?,?,?,?,?)";
			selectQuery = "SELECT concept_set.concept_set_id,concept_set.concept_id,concept_set.concept_set,concept_set.sort_weight,concept_set.creator,concept_set.date_created,concept_set.uuid FROM openmrs.concept_set";
			remoteSelectInsert(selectQuery, insertQuery);
			// concept_stop_word
			insertQuery = "INSERT INTO openmrs.concept_stop_word(concept_stop_word_id,word,locale,uuid)VALUES(?,?,?,?)";
			selectQuery = "SELECT concept_stop_word.concept_stop_word_id,concept_stop_word.word,concept_stop_word.locale,concept_stop_word.uuid FROM openmrs.concept_stop_word";
			remoteSelectInsert(selectQuery, insertQuery);
			// encounter_role
			insertQuery = "INSERT INTO openmrs.encounter_role(encounter_role_id,name,description,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT encounter_role.encounter_role_id,encounter_role.name,encounter_role.description,encounter_role.creator,encounter_role.date_created,encounter_role.changed_by,encounter_role.date_changed,encounter_role.retired,encounter_role.retired_by,encounter_role.date_retired,encounter_role.retire_reason,encounter_role.uuid FROM openmrs.encounter_role";
			remoteSelectInsert(selectQuery, insertQuery);
			// encounter_type
			insertQuery = "INSERT INTO openmrs.encounter_type(encounter_type_id,name,description,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid,edit_privilege,view_privilege)VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT encounter_type.encounter_type_id,encounter_type.name,encounter_type.description,encounter_type.creator,encounter_type.date_created,encounter_type.retired,encounter_type.retired_by,encounter_type.date_retired,encounter_type.retire_reason,encounter_type.uuid,encounter_type.edit_privilege,encounter_type.view_privilegeFROM openmrs.encounter_type";
			remoteSelectInsert(selectQuery, insertQuery);
			// field_type
			insertQuery = "INSERT INTO openmrs.field_type(field_type_id,name,description,is_set,creator,date_created,uuid)VALUES(?,?,?,?,?,?,?)";
			selectQuery = "SELECT field_type.field_type_id,field_type.name,field_type.description,field_type.is_set,field_type.creator,field_type.date_created,field_type.uuid FROM openmrs.field_type";
			remoteSelectInsert(selectQuery, insertQuery);
			// hl7_source
			insertQuery = "INSERT INTO openmrs.hl7_source(hl7_source_id,name,description,creator,date_created,uuid)VALUES(?,?,?,?,?,?)";
			selectQuery = "SELECT hl7_source.hl7_source_id,hl7_source.name,hl7_source.description,hl7_source.creator,hl7_source.date_created,hl7_source.uuid FROM openmrs.hl7_source";
			remoteSelectInsert(selectQuery, insertQuery);
			// htmlformentry_html_form
			insertQuery = "INSERT INTO openmrs.htmlformentry_html_form(id,form_id,name,xml_data,creator,date_created,changed_by,date_changed,retired,uuid,description,retired_by,date_retired,retire_reason)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT htmlformentry_html_form.id,htmlformentry_html_form.form_id,htmlformentry_html_form.name,htmlformentry_html_form.xml_data,htmlformentry_html_form.creator,htmlformentry_html_form.date_created,htmlformentry_html_form.changed_by,htmlformentry_html_form.date_changed,htmlformentry_html_form.retired,htmlformentry_html_form.uuid,htmlformentry_html_form.description,htmlformentry_html_form.retired_by,htmlformentry_html_form.date_retired,htmlformentry_html_form.retire_reasonFROM openmrs.htmlformentry_html_form";
			remoteSelectInsert(selectQuery, insertQuery);
			// location
			insertQuery = "INSERT INTO openmrs.location(location_id,name,description,address1,address2,city_village,state_province,postal_code,country,latitude,longitude,creator,date_created,county_district,address3,address4,address5,address6,retired,retired_by,date_retired,retire_reason,parent_location,uuid,changed_by,date_changed)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT location.location_id,location.name,location.description,location.address1,location.address2,location.city_village,location.state_province,location.postal_code,location.country,location.latitude,location.longitude,location.creator,location.date_created,location.county_district,location.address3,location.address4,location.address5,location.address6,location.retired,location.retired_by,location.date_retired,location.retire_reason,location.parent_location,location.uuid,location.changed_by,location.date_changedFROM openmrs.location";
			remoteSelectInsert(selectQuery, insertQuery);
			// location_attribute
			insertQuery = "INSERT INTO openmrs.location_attribute(location_attribute_id,location_id,attribute_type_id,value_reference,uuid,creator,date_created,changed_by,date_changed,voided,voided_by,date_voided,void_reason)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT location_attribute.location_attribute_id,location_attribute.location_id,location_attribute.attribute_type_id,location_attribute.value_reference,location_attribute.uuid,location_attribute.creator,location_attribute.date_created,location_attribute.changed_by,location_attribute.date_changed,location_attribute.voided,location_attribute.voided_by,location_attribute.date_voided,location_attribute.void_reasonFROM openmrs.location_attribute";
			remoteSelectInsert(selectQuery, insertQuery);
			// location_attribute_type
			insertQuery = "INSERT INTO openmrs.location_attribute_type(location_attribute_type_id,name,description,datatype,datatype_config,preferred_handler,handler_config,min_occurs,max_occurs,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT location_attribute_type.location_attribute_type_id,location_attribute_type.name,location_attribute_type.description,location_attribute_type.datatype,location_attribute_type.datatype_config,location_attribute_type.preferred_handler,location_attribute_type.handler_config,location_attribute_type.min_occurs,location_attribute_type.max_occurs,location_attribute_type.creator,location_attribute_type.date_created,location_attribute_type.changed_by,location_attribute_type.date_changed,location_attribute_type.retired,location_attribute_type.retired_by,location_attribute_type.date_retired,location_attribute_type.retire_reason,location_attribute_type.uuid FROM openmrs.location_attribute_type";
			remoteSelectInsert(selectQuery, insertQuery);
			// location_tag
			insertQuery = "INSERT INTO openmrs.location_tag(location_tag_id,name,description,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid,changed_by,date_changed)VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT location_tag.location_tag_id,location_tag.name,location_tag.description,location_tag.creator,location_tag.date_created,location_tag.retired,location_tag.retired_by,location_tag.date_retired,location_tag.retire_reason,location_tag.uuid,location_tag.changed_by,location_tag.date_changedFROM openmrs.location_tag";
			remoteSelectInsert(selectQuery, insertQuery);
			// location_tag_map
			insertQuery = "INSERT INTO openmrs.location_tag_map(location_id,location_tag_id)VALUES(?,?)";
			selectQuery = "SELECT location_tag_map.location_id,location_tag_map.location_tag_idFROM openmrs.location_tag_map";
			remoteSelectInsert(selectQuery, insertQuery);
			// order_type
			insertQuery = "INSERT INTO openmrs.order_type(order_type_id,name,description,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid,java_class_name,parent,changed_by,date_changed)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT order_type.order_type_id,order_type.name,order_type.description,order_type.creator,order_type.date_created,order_type.retired,order_type.retired_by,order_type.date_retired,order_type.retire_reason,order_type.uuid,order_type.java_class_name,order_type.parent,order_type.changed_by,order_type.date_changedFROM openmrs.order_type";
			remoteSelectInsert(selectQuery, insertQuery);
			// privilege
			insertQuery = "INSERT INTO openmrs.privilege(privilege,description,uuid)VALUES(?,?,?)";
			selectQuery = "SELECT privilege.privilege,privilege.description,privilege.uuid FROM openmrs.privilege";
			remoteSelectInsert(selectQuery, insertQuery);
			// provider
			insertQuery = "INSERT INTO openmrs.provider(provider_id,person_id,name,identifier,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid,provider_role_id)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT provider.provider_id,provider.person_id,provider.name,provider.identifier,provider.creator,provider.date_created,provider.changed_by,provider.date_changed,provider.retired,provider.retired_by,provider.date_retired,provider.retire_reason,provider.uuid,provider.provider_role_idFROM openmrs.provider";
			remoteSelectInsert(selectQuery, insertQuery);
			// relationship_type
			insertQuery = "INSERT INTO openmrs.relationship_type(relationship_type_id,a_is_to_b,b_is_to_a,preferred,weight,description,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT relationship_type.relationship_type_id,relationship_type.a_is_to_b,relationship_type.b_is_to_a,relationship_type.preferred,relationship_type.weight,relationship_type.description,relationship_type.creator,relationship_type.date_created,relationship_type.retired,relationship_type.retired_by,relationship_type.date_retired,relationship_type.retire_reason,relationship_type.uuid FROM openmrs.relationship_type";
			remoteSelectInsert(selectQuery, insertQuery);
			// role
			insertQuery = "INSERT INTO openmrs.role(role,description,uuid)VALUES(?,?,?)";
			selectQuery = "SELECT role.role,role.description,role.uuid FROM openmrs.role";
			remoteSelectInsert(selectQuery, insertQuery);
			// role_privilege
			insertQuery = "INSERT INTO openmrs.role_privilege(role,privilege)VALUES(?,?)";
			selectQuery = "SELECT role_privilege.role,role_privilege.privilegeFROM openmrs.role_privilege";
			remoteSelectInsert(selectQuery, insertQuery);
			// role_role
			insertQuery = "INSERT INTO openmrs.role_role(parent_role,child_role)VALUES(?,?)";
			selectQuery = "SELECT role_role.parent_role,role_role.child_roleFROM openmrs.role_role";
			remoteSelectInsert(selectQuery, insertQuery);
			// scheduler_task_config
			insertQuery = "INSERT INTO openmrs.scheduler_task_config(task_config_id,name,description,schedulable_class,start_time,start_time_pattern,repeat_interval,start_on_startup,started,created_by,date_created,changed_by,date_changed,last_execution_time,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT scheduler_task_config.task_config_id,scheduler_task_config.name,scheduler_task_config.description,scheduler_task_config.schedulable_class,scheduler_task_config.start_time,scheduler_task_config.start_time_pattern,scheduler_task_config.repeat_interval,scheduler_task_config.start_on_startup,scheduler_task_config.started,scheduler_task_config.created_by,scheduler_task_config.date_created,scheduler_task_config.changed_by,scheduler_task_config.date_changed,scheduler_task_config.last_execution_time,scheduler_task_config.uuid FROM openmrs.scheduler_task_config";
			remoteSelectInsert(selectQuery, insertQuery);
			// user_property
			insertQuery = "INSERT INTO openmrs.user_property(user_id,property,property_value)VALUES(?,?,?)";
			selectQuery = "SELECT user_property.user_id,user_property.property,user_property.property_valueFROM openmrs.user_property";
			remoteSelectInsert(selectQuery, insertQuery);
			// user_role
			insertQuery = "INSERT INTO openmrs.user_role(user_id,role)VALUES(?,?)";
			selectQuery = "SELECT user_role.user_id,user_role.roleFROM openmrs.user_role";
			remoteSelectInsert(selectQuery, insertQuery);
			// users
			insertQuery = "INSERT INTO openmrs.users(user_id,system_id,username,password,salt,secret_question,secret_answer,creator,date_created,changed_by,date_changed,person_id,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT users.user_id,users.system_id,users.username,users.password,users.salt,users.secret_question,users.secret_answer,users.creator,users.date_created,users.changed_by,users.date_changed,users.person_id,users.retired,users.retired_by,users.date_retired,users.retire_reason,users.uuid FROM openmrs.users";
			remoteSelectInsert(selectQuery, insertQuery);
			// visit_type
			insertQuery = "INSERT INTO openmrs.visit_type(visit_type_id,name,description,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT visit_type.visit_type_id,visit_type.name,visit_type.description,visit_type.creator,visit_type.date_created,visit_type.changed_by,visit_type.date_changed,visit_type.retired,visit_type.retired_by,visit_type.date_retired,visit_type.retire_reason,visit_type.uuid FROM openmrs.visit_type";
			remoteSelectInsert(selectQuery, insertQuery);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
