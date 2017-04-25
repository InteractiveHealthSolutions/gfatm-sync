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
	 * Insert data from all sources into data warehouse.
	 * 
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 * @throws ParseException 
	 */
	public void importData() throws InstantiationException,
			IllegalAccessException, ClassNotFoundException, SQLException, ParseException {
		// Fetch source databases from _implementation table
		Object[][] sources = localDb
				.getTableData(
						"_implementation",
						"implementation_id,connection_url,driver,db_name,username,password,last_updated",
						"active=1 AND status<>'RUNNING'");
		// For each source, import all data
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
				clearTempTables(implementationId);
//				importPeopleData(remoteDb, implementationId);
				importUserData(remoteDb, implementationId);
				importLocationData(remoteDb, implementationId);
				importConceptData(remoteDb, implementationId);
				importPatientData(remoteDb, implementationId);
				importEncounterData(remoteDb, implementationId);
				// Update the status in _implementation table
				localDb.updateRecord("_implementation", new String[] {"last_updated"}, new String[] {DateTimeUtil.getSqlDateTime(new Date())}, "implementation_id='" + implementationId + "'");
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				localDb.updateRecord("_implementation", new String[] {"status"}, new String[] {"STOPPED"}, "implementation_id='" + implementationId + "'");
			}
		}
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
				+ "')) ");
		if (updateDateName != null) {
			filter.append(" OR (" + updateDateName);
			filter.append(" BETWEEN TIMESTAMP('"
					+ DateTimeUtil.getSqlDateTime(fromDate) + "') ");
			filter.append("AND TIMESTAMP('" + DateTimeUtil.getSqlDateTime(toDate)
					+ "')) ");
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
	 * Remove all data from temporary tables related to given implementation ID
	 * @param implementationId
	 */
	private void clearTempTables(int implementationId) {
		String[] tables = {"tmp_person", "tmp_person_attribute", "tmp_person_attribute_type", "tmp_person_address", "tmp_person_name", "tmp_role", "tmp_role_role"};
		for (String table : tables) {
			try {
				localDb.runCommandWithException(CommandType.TRUNCATE, "TRUNCATE TABLE " + table);
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
			insertQuery = "INSERT INTO tmp_" + tableName + " (surrogate_key, implementation_id, person_id, gender, birthdate, birthdate_estimated, dead, death_date, cause_of_death, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', person_id, gender, birthdate, birthdate_estimated, dead, death_date, cause_of_death, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason, uuid FROM " + database + "." + tableName + " AS t " + filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database + "." + tableName + " into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			// Insert new records
			insertQuery = "INSERT INTO " + tableName + " SELECT * FROM tmp_" + tableName + " AS t WHERE NOT EXISTS (SELECT * FROM " + tableName + " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			localDb.runCommand(CommandType.INSERT, insertQuery);
			// Update the existing records
			updateQuery = "UPDATE " + tableName + " AS a, tmp_" + tableName + " AS t SET a.gender = t.gender, a.birthdate = t.birthdate, a.birthdate_estimated = t.birthdate_estimated, a.dead = t.dead, a.death_date = t.death_date, a.cause_of_death = t.cause_of_death, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.voided = t.voided, a.voided_by = t.voided_by, a.date_voided = t.date_voided, a.void_reason = t.void_reason WHERE a.implementation_id = t.implementation_id = '" + implementationId + "' AND a.uuid = t.uuid";
			localDb.runCommand(CommandType.UPDATE, updateQuery);

			tableName = "person_attribute_type";
			insertQuery = "INSERT INTO tmp_" + tableName + " (surrogate_key, implementation_id, person_attribute_type_id, name, description, format, foreign_key, searchable, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, edit_privilege, sort_weight, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', person_attribute_type_id, name, description, format, foreign_key, searchable, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, edit_privilege, sort_weight, uuid FROM " + database + "." + tableName + " AS t " + filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database + "." + tableName + " into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			insertQuery = "INSERT INTO " + tableName + " SELECT * FROM tmp_" + tableName + " AS t WHERE NOT EXISTS (SELECT * FROM " + tableName + " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			localDb.runCommand(CommandType.INSERT, insertQuery);
			updateQuery = "UPDATE " + tableName + " AS a, tmp_" + tableName + " AS t SET a.name = t.name, a.description = t.description, a.format = t.format, a.foreign_key = t.foreign_key, a.searchable = t.searchable, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.retired = t.retired, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retire_reason = t.retire_reason, a.edit_privilege = t.edit_privilege, a.sort_weight = t.sort_weight WHERE a.implementation_id = t.implementation_id = '" + implementationId + "' AND a.uuid = t.uuid";
			localDb.runCommand(CommandType.UPDATE, updateQuery);

			tableName = "person_attribute";
			insertQuery = "INSERT INTO tmp_" + tableName + " (surrogate_key, implementation_id, person_attribute_id, person_id, value, person_attribute_type_id, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', person_attribute_id, person_id, value, person_attribute_type_id, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason, uuid FROM " + database + "." + tableName + " AS t " + filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database + "." + tableName + " into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			insertQuery = "INSERT INTO " + tableName + " SELECT * FROM tmp_" + tableName + " AS t WHERE NOT EXISTS (SELECT * FROM " + tableName + " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			localDb.runCommand(CommandType.INSERT, insertQuery);
			updateQuery = "UPDATE " + tableName + " AS a, tmp_" + tableName + " AS t SET a.person_id = t.person_id, a.value = t.value, a.person_attribute_type_id = t.person_attribute_type_id, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.voided = t.voided, a.voided_by = t.voided_by, a.date_voided = t.date_voided, a.void_reason = t.void_reason WHERE a.implementation_id = t.implementation_id = '" + implementationId + "' AND a.uuid = t.uuid";
			localDb.runCommand(CommandType.UPDATE, updateQuery);

			tableName = "person_address";
			insertQuery = "INSERT INTO tmp_" + tableName + " (surrogate_key, implementation_id, person_address_id, person_id, preferred, address1, address2, city_village, state_province, postal_code, country, latitude, longitude, start_date, end_date, creator, date_created, voided, voided_by, date_voided, void_reason, county_district, address3, address4, address5, address6, date_changed, changed_by, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', person_address_id, person_id, preferred, address1, address2, city_village, state_province, postal_code, country, latitude, longitude, start_date, end_date, creator, date_created, voided, voided_by, date_voided, void_reason, county_district, address3, address4, address5, address6, date_changed, changed_by, uuid FROM " + database + "." + tableName + " AS t " + filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database + "." + tableName + " into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			insertQuery = "INSERT INTO " + tableName + " SELECT * FROM tmp_" + tableName + " AS t WHERE NOT EXISTS (SELECT * FROM " + tableName + " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			localDb.runCommand(CommandType.INSERT, insertQuery);
			updateQuery = "UPDATE " + tableName + " AS a, tmp_" + tableName + " AS t SET a.person_id = t.person_id, a.preferred = t.preferred, a.address1 = t.address1, a.address2 = t.address2, a.city_village = t.city_village, a.state_province = t.state_province, a.postal_code = t.postal_code, a.country = t.country, a.latitude = t.latitude, a.longitude = t.longitude, a.start_date = t.start_date, a.end_date = t.end_date, a.creator = t.creator, a.date_created = t.date_created, a.voided = t.voided, a.voided_by = t.voided_by, a.date_voided = t.date_voided, a.void_reason = t.void_reason, a.county_district = t.county_district, a.address3 = t.address3, a.address4 = t.address4, a.address5 = t.address5, a.address6 = t.address6, a.date_changed = t.date_changed, a.changed_by = t.changed_by WHERE a.implementation_id = t.implementation_id = '" + implementationId + "' AND a.uuid = t.uuid";
			localDb.runCommand(CommandType.UPDATE, updateQuery);

			tableName = "person_name";
			insertQuery = "INSERT INTO tmp_" + tableName + " (surrogate_key, implementation_id, person_name_id, preferred, person_id, prefix, given_name, middle_name, family_name_prefix, family_name, family_name2, family_name_suffix, degree, creator, date_created, voided, voided_by, date_voided, void_reason, changed_by, date_changed, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', person_name_id, preferred, person_id, prefix, given_name, middle_name, family_name_prefix, family_name, family_name2, family_name_suffix, degree, creator, date_created, voided, voided_by, date_voided, void_reason, changed_by, date_changed, uuid FROM " + database + "." + tableName + " AS t " + filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database + "." + tableName + " into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			insertQuery = "INSERT INTO " + tableName + " SELECT * FROM tmp_" + tableName + " AS t WHERE NOT EXISTS (SELECT * FROM " + tableName + " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			localDb.runCommand(CommandType.INSERT, insertQuery);
			updateQuery = "UPDATE " + tableName + " AS a, tmp_" + tableName + " AS t SET a.preferred = t.preferred, a.person_id = t.person_id, a.prefix = t.prefix, a.given_name = t.given_name, a.middle_name = t.middle_name, a.family_name_prefix = t.family_name_prefix, a.family_name = t.family_name, a.family_name2 = t.family_name2, a.family_name_suffix = t.family_name_suffix, a.degree = t.degree, a.creator = t.creator, a.date_created = t.date_created, a.voided = t.voided, a.voided_by = t.voided_by, a.date_voided = t.date_voided, a.void_reason = t.void_reason, a.changed_by = t.changed_by, a.date_changed = t.date_changed WHERE a.implementation_id = t.implementation_id = '" + implementationId + "' AND a.uuid = t.uuid";
			localDb.runCommand(CommandType.UPDATE, updateQuery);
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
			insertQuery = "INSERT INTO tmp_" + tableName + " (surrogate_key, implementation_id, role, description, uuid) VALUES (?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', role, description, uuid FROM " + database + "." + tableName + " AS t ";
			log.info("Inserting data from " + database + "." + tableName + " into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			insertQuery = "INSERT INTO " + tableName + " SELECT * FROM tmp_" + tableName + " AS t WHERE NOT EXISTS (SELECT * FROM " + tableName + " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			localDb.runCommand(CommandType.INSERT, insertQuery);
			updateQuery = "UPDATE " + tableName + " AS a, tmp_" + tableName + " AS t SET a.role = t.role, a.description = t.description WHERE a.implementation_id = t.implementation_id = '" + implementationId + "' AND a.uuid = t.uuid";
			localDb.runCommand(CommandType.UPDATE, updateQuery);

			tableName = "role_role";
			insertQuery = "INSERT INTO tmp_" + tableName + " (surrogate_key, implementation_id, parent_role, child_role) VALUES (?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', parent_role, child_role FROM " + database + "." + tableName + " AS t ";
			log.info("Inserting data from " + database + "." + tableName + " into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			deleteQuery = "DELETE FROM " + tableName + " WHERE implementation_id = '" + implementationId + "'";
			localDb.runCommand(CommandType.DELETE, deleteQuery);
			insertQuery = "INSERT INTO " + tableName + " SELECT * FROM tmp_" + tableName + " AS t WHERE t.implementation_id = '" + implementationId + "'";
			localDb.runCommand(CommandType.INSERT, insertQuery);

			tableName = "privilege";
			insertQuery = "INSERT INTO tmp_" + tableName + " (surrogate_key, implementation_id, privilege, description, uuid) VALUES (?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', privilege, description, uuid FROM " + database + "." + tableName + "  AS t ";
			log.info("Inserting data from " + database + "." + tableName + "  into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			insertQuery = "INSERT INTO " + tableName + " SELECT * FROM tmp_" + tableName + " AS t WHERE NOT EXISTS (SELECT * FROM " + tableName + " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			localDb.runCommand(CommandType.INSERT, insertQuery);
			updateQuery = "UPDATE " + tableName + " AS a, tmp_" + tableName + " AS t SET a.privilege = t.privilege, a.description = t.description WHERE a.implementation_id = t.implementation_id = '" + implementationId + "' AND a.uuid = t.uuid";
			localDb.runCommand(CommandType.UPDATE, updateQuery);

			tableName = "role_privilege";
			insertQuery = "INSERT INTO tmp_" + tableName + " (surrogate_key, implementation_id, role, privilege) VALUES (?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', role, privilege FROM " + database + "." + tableName + " AS t ";
			log.info("Inserting data from " + database + "." + tableName + " into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			deleteQuery = "DELETE FROM " + tableName + " WHERE implementation_id = '" + implementationId + "'";
			localDb.runCommand(CommandType.DELETE, deleteQuery);
			insertQuery = "INSERT INTO " + tableName + " SELECT * FROM tmp_" + tableName + " AS t WHERE t.implementation_id = '" + implementationId + "'";
			localDb.runCommand(CommandType.INSERT, insertQuery);

			tableName = "users";
			insertQuery = "INSERT INTO tmp_" + tableName + " (surrogate_key, implementation_id, user_id, system_id, username, password, salt, secret_question, secret_answer, creator, date_created, changed_by, date_changed, person_id, retired, retired_by, date_retired, retire_reason, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', user_id, system_id, username, password, salt, secret_question, secret_answer, creator, date_created, changed_by, date_changed, person_id, retired, retired_by, date_retired, retire_reason, uuid FROM " + database + "." + tableName + " AS t " + filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database + "." + tableName + " into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			insertQuery = "INSERT INTO " + tableName + " SELECT * FROM tmp_" + tableName + " AS t WHERE NOT EXISTS (SELECT * FROM " + tableName + " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			localDb.runCommand(CommandType.INSERT, insertQuery);
			updateQuery = "UPDATE " + tableName + " AS a, tmp_" + tableName + " AS t SET a.username = t.username, a.password = t.password, a.salt = t.salt, a.secret_question = t.secret_question, a.secret_answer = t.secret_answer, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.person_id = t.person_id, a.retired = t.retired, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retire_reason = t.retire_reason WHERE a.implementation_id = t.implementation_id = '" + implementationId + "' AND a.uuid = t.uuid";
			localDb.runCommand(CommandType.UPDATE, updateQuery);

			tableName = "user_property";
			insertQuery = "INSERT INTO tmp_" + tableName + " (surrogate_key, implementation_id, user_id, property, property_value) VALUES (?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', user_id, property, property_value FROM " + database + "." + tableName + " AS t ";
			log.info("Inserting data from " + database + "." + tableName + " into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			deleteQuery = "DELETE FROM " + tableName + " WHERE implementation_id = '" + implementationId + "'";
			localDb.runCommand(CommandType.DELETE, deleteQuery);
			insertQuery = "INSERT INTO " + tableName + " SELECT * FROM tmp_" + tableName + " AS t WHERE t.implementation_id = '" + implementationId + "'";
			localDb.runCommand(CommandType.INSERT, insertQuery);

			tableName = "user_role";
			insertQuery = "INSERT INTO tmp_" + tableName + " (surrogate_key, implementation_id, user_id, role) VALUES (?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', user_id, role FROM " + database + "." + tableName + " AS t ";
			log.info("Inserting data from " + database + "." + tableName + " into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			deleteQuery = "DELETE FROM " + tableName + " WHERE implementation_id = '" + implementationId + "'";
			localDb.runCommand(CommandType.DELETE, deleteQuery);
			insertQuery = "INSERT INTO " + tableName + " SELECT * FROM tmp_" + tableName + " AS t WHERE t.implementation_id = '" + implementationId + "'";
			localDb.runCommand(CommandType.INSERT, insertQuery);
			
			tableName = "provider_attribute_type";
			insertQuery = "INSERT INTO tmp_" + tableName + " (surrogate_key, implementation_id, provider_attribute_type_id, name, description, datatype, datatype_config, preferred_handler, handler_config, min_occurs, max_occurs, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', provider_attribute_type_id, name, description, datatype, datatype_config, preferred_handler, handler_config, min_occurs, max_occurs, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, uuid FROM " + database + "." + tableName + " AS t " + filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database + "." + tableName + " into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			insertQuery = "INSERT INTO " + tableName + " SELECT * FROM tmp_" + tableName + " AS t WHERE NOT EXISTS (SELECT * FROM " + tableName + " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			localDb.runCommand(CommandType.INSERT, insertQuery);
			updateQuery = "UPDATE " + tableName + " AS a, tmp_" + tableName + " AS t SET a.name = t.name, a.description = t.description, a.datatype = t.datatype, a.datatype_config = t.datatype_config, a.preferred_handler = t.preferred_handler, a.handler_config = t.handler_config, a.min_occurs = t.min_occurs, a.max_occurs = t.max_occurs, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.retired = t.retired, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retire_reason = t.retire_reason WHERE a.implementation_id = t.implementation_id = '" + implementationId + "' AND a.uuid = t.uuid";
			localDb.runCommand(CommandType.UPDATE, updateQuery);

			tableName = "provider";
			insertQuery = "INSERT INTO tmp_" + tableName + " (surrogate_key, implementation_id, provider_id, person_id, name, identifier, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', provider_id, person_id, name, identifier, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, uuid FROM " + database + "." + tableName + " AS t " + filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database + "." + tableName + " into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			insertQuery = "INSERT INTO " + tableName + " SELECT * FROM tmp_" + tableName + " AS t WHERE NOT EXISTS (SELECT * FROM " + tableName + " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			localDb.runCommand(CommandType.INSERT, insertQuery);
			updateQuery = "UPDATE " + tableName + " AS a, tmp_" + tableName + " AS t SET a.person_id = t.person_id, a.name = t.name, a.identifier = t.identifier, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.retired = t.retired, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retire_reason = t.retire_reason WHERE a.implementation_id = t.implementation_id = '" + implementationId + "' AND a.uuid = t.uuid";
			localDb.runCommand(CommandType.UPDATE, updateQuery);
			
			tableName = "provider_attribute";
			insertQuery = "INSERT INTO tmp_" + tableName + " (surrogate_key, implementation_id, provider_attribute_id, provider_id, attribute_type_id, value_reference, uuid, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', provider_attribute_id, provider_id, attribute_type_id, value_reference, uuid, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason FROM " + database + "." + tableName + " AS t " + filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database + "." + tableName + " into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			insertQuery = "INSERT INTO " + tableName + " SELECT * FROM tmp_" + tableName + " AS t WHERE NOT EXISTS (SELECT * FROM " + tableName + " WHERE implementation_id = t.implementation_id AND uuid = t.uuid)";
			localDb.runCommand(CommandType.INSERT, insertQuery);
			updateQuery = "UPDATE " + tableName + " AS a, tmp_" + tableName + " AS t SET a.provider_id = t.provider_id, a.attribute_type_id = t.attribute_type_id, a.value_reference = t.value_reference, a.uuid = t.uuid, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.voided = t.voided, a.voided_by = t.voided_by, a.date_voided = t.date_voided, a.void_reason = t.void_reason WHERE a.implementation_id = t.implementation_id = '" + implementationId + "' AND a.uuid = t.uuid";
			localDb.runCommand(CommandType.UPDATE, updateQuery);
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
		String updateQuery;
		String selectQuery;
		String tableName;
		try {
			// Concept Class
			tableName = "concept_class";
			insertQuery = "INSERT INTO " + tableName + " (surrogate_key, implementation_id, concept_class_id, name, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', concept_class_id, name, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid FROM " + database + "." + tableName + " AS t " + filter("t.date_created", null);
			log.info("Inserting data from " + database + "." + tableName + " into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			// Concept Set
			tableName = "concept_set";
			insertQuery = "INSERT INTO " + tableName + " (surrogate_key, implementation_id, concept_set_id, concept_id, concept_set, sort_weight, creator, date_created, uuid) VALUES (?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', concept_set_id, concept_id, concept_set, sort_weight, creator, date_created, uuid FROM " + database + "." + tableName + " AS t " + filter("t.date_created", null);
			log.info("Inserting data from " + database + "." + tableName + " into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			// Concept Data Type
			tableName = "concept_datatype";
			insertQuery = "INSERT INTO " + tableName + " (surrogate_key, implementation_id, concept_datatype_id, name, hl7_abbreviation, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', concept_datatype_id, name, hl7_abbreviation, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid FROM " + database + "." + tableName + " AS t " + filter("t.date_created", null);
			log.info("Inserting data from " + database + "." + tableName + " into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			// Concept Map Type
			insertQuery = "INSERT INTO concept_map_type (surrogate_key, implementation_id, concept_map_type_id, name, description, creator, date_created, changed_by, date_changed, is_hidden, retired, retired_by, date_retired, retire_reason, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', concept_map_type_id, name, description, creator, date_created, changed_by, date_changed, is_hidden, retired, retired_by, date_retired, retire_reason, uuid FROM " + database + ".concept_map_type AS t " + filter("t.date_created", null);
			log.info("Inserting data from " + database + ".concept_map_type into data warehouse");
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
			insertQuery = "INSERT INTO concept_numeric (surrogate_key, implementation_id, concept_id, hi_absolute, hi_critical, hi_normal, low_absolute, low_critical, low_normal, units, precise, display_precision) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";
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
			insertQuery = "INSERT IGNORE INTO patient_identifier_type (surrogate_key, implementation_id, patient_identifier_type_id, name, description, format, check_digit, creator, date_created, required, format_description, validator, location_behavior, retired, retired_by, date_retired, retire_reason, uuid, uniqueness_behavior) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', patient_identifier_type_id, name, description, format, check_digit, creator, date_created, required, format_description, validator, location_behavior, retired, retired_by, date_retired, retire_reason, uuid, uniqueness_behavior FROM " + database + ".patient_identifier_type AS t " + filter("t.date_created", null);
			log.info("Inserting data from " + database + ".patient_identifier_type into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			// Patient
			insertQuery = "INSERT INTO patient (surrogate_key, implementation_id, patient_id, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason) VALUES (?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', patient_id, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason FROM " + database + ".patient AS t " + filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database + ".patient into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			// Patient Identifier
			insertQuery = "INSERT INTO patient_identifier (surrogate_key, implementation_id, patient_identifier_id, patient_id, identifier, identifier_type, preferred, location_id, creator, date_created, date_changed, changed_by, voided, voided_by, date_voided, void_reason, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', patient_identifier_id, patient_id, identifier, identifier_type, preferred, location_id, creator, date_created, date_changed, changed_by, voided, voided_by, date_voided, void_reason, uuid FROM " + database + ".patient_identifier AS t " + filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database + ".patient_identifier into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			// Patient Program
			insertQuery = "INSERT INTO patient_program (surrogate_key, implementation_id, patient_program_id, patient_id, program_id, date_enrolled, date_completed, location_id, outcome_concept_id, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', patient_program_id, patient_id, program_id, date_enrolled, date_completed, location_id, outcome_concept_id, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason, uuid FROM " + database + ".patient_program AS t " + filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database + ".patient_program into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
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
			insertQuery = "INSERT INTO encounter_type (surrogate_key, implementation_id, encounter_type_id, name, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid, edit_privilege, view_privilege) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', encounter_type_id, name, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid, edit_privilege, view_privilege FROM " + database + ".encounter_type AS t " + filter("t.date_created", null);
			log.info("Inserting data from " + database + ".encounter_type into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			// Form
			insertQuery = "INSERT INTO form (surrogate_key, implementation_id, form_id, name, version, build, published, xslt, template, description, encounter_type, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retired_reason, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', form_id, name, version, build, published, xslt, template, description, encounter_type, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retired_reason, uuid FROM " + database + ".form AS t " + filter("t.date_created", null);
			log.info("Inserting data from " + database + ".form into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			// Encounter Role
			insertQuery = "INSERT INTO encounter_role (surrogate_key, implementation_id, encounter_role_id, name, description, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', encounter_role_id, name, description, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, uuid FROM " + database + ".encounter_role AS t " + filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database + ".encounter_role into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			// Encounter
			insertQuery = "INSERT INTO encounter (surrogate_key, implementation_id, encounter_id, encounter_type, patient_id, location_id, form_id, encounter_datetime, creator, date_created, voided, voided_by, date_voided, void_reason, changed_by, date_changed, visit_id, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', encounter_id, encounter_type, patient_id, location_id, form_id, encounter_datetime, creator, date_created, voided, voided_by, date_voided, void_reason, changed_by, date_changed, visit_id, uuid FROM " + database + ".encounter AS t " + filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database + ".encounter into data warehouse");
			//remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			// Encounter Provider
			insertQuery = "INSERT INTO encounter_provider (surrogate_key, implementation_id, encounter_provider_id, encounter_id, provider_id, encounter_role_id, creator, date_created, changed_by, date_changed, voided, date_voided, voided_by, void_reason, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId + "', encounter_provider_id, encounter_id, provider_id, encounter_role_id, creator, date_created, changed_by, date_changed, voided, date_voided, voided_by, void_reason, uuid FROM " + database + ".encounter_provider AS t " + filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database + ".encounter_provider into data warehouse");
			//remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());

			// Observation data is too much to handle in single query, so import in batches
			tableName = "obs";
			int from = 1, to = 0;
			int batchSize = 500;
			int rows = (int) remoteDb.getTotalRows(tableName, filter("t.date_created", null));
			log.info("Inserting data from " + database + "." + tableName + " into data warehouse");
			while (from <= rows) {
				to = (from + batchSize) > rows ? rows : (from + batchSize);
				String limitClause = "LIMIT " + from + ", " + to;
				from += batchSize;
				insertQuery = "INSERT INTO tmp_" + tableName + " (surrogate_key, implementation_id, obs_id, person_id, concept_id, encounter_id, order_id, obs_datetime, location_id, obs_group_id, accession_number, value_group_id, value_boolean, value_coded, value_coded_name_id, value_drug, value_datetime, value_numeric, value_modifier, value_text, value_complex, comments, creator, date_created, voided, voided_by, date_voided, void_reason, uuid, previous_version) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
				selectQuery = "SELECT 0,'" + implementationId + "', obs_id, person_id, concept_id, encounter_id, order_id, obs_datetime, location_id, obs_group_id, accession_number, value_group_id, value_boolean, value_coded, value_coded_name_id, value_drug, value_datetime, value_numeric, value_modifier, value_text, value_complex, comments, creator, date_created, voided, voided_by, date_voided, void_reason, uuid, previous_version FROM " + database + "." + tableName + " AS t " + filter("t.date_created", null) + limitClause + " ";
				remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), localDb.getConnection());
			}
			// TODO: Handle tables visit_type, visit, visit_attribute_type, visit_attribute, form, field, form_field, field_type
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
