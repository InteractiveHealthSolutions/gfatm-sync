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
import java.util.Date;
import java.util.logging.Logger;

import com.ihsinformatics.util.CommandType;
import com.ihsinformatics.util.DatabaseUtil;

/**
 * @author owais.hussain@ihsinformatics.com
 *
 */
public class OpenMrsExtendedImportController extends AbstractImportController {

	private static final Logger log = Logger.getLogger(Class.class.getName());

	public OpenMrsExtendedImportController(DatabaseUtil sourceDb, DatabaseUtil targetDb) {
		this.sourceDb = sourceDb;
		this.targetDb = targetDb;
		this.fromDate = new Date();
		this.toDate = new Date();
	}

	public OpenMrsExtendedImportController(DatabaseUtil sourceDb, DatabaseUtil targetDb, Date fromDate, Date toDate) {
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
	public void importData(int implementationId) throws InstantiationException, IllegalAccessException,
			ClassNotFoundException, SQLException, ParseException {
		sourceDb.getConnection();
		// Import data from this connection into data warehouse
		try {
			// Update status of implementation record
			log.info("Cleaning temporary tables...");
			clearTempTables(implementationId);
			log.info("Importing Lab Module data...");
			importLabModuleData(sourceDb, implementationId);
			log.info("Importing Facility Module data...");
			importFacilityModuleData(sourceDb, implementationId);
			log.info("Import complete");
		} catch (Exception e) {
			log.warning(e.getMessage());
		}
	}

	/**
	 * Remove all data from temporary tables related to given implementation ID
	 * 
	 * @param implementationId
	 */
	private void clearTempTables(int implementationId) {
		String[] tables = {
				"tmp_commonlabtest_attribute, tmp_commonlabtest_attribute_type, tmp_commonlabtest_sample, tmp_commonlabtest_test, tmp_commonlabtest_type" };
		for (String table : tables) {
			try {
				targetDb.runCommandWithException(CommandType.TRUNCATE, "TRUNCATE TABLE " + table);
			} catch (Exception e) {
				log.warning("Table: " + table + " not found in data warehouse!");
			}
		}
	}

	/**
	 * Load data from common-lab module tables into data warehouse
	 * 
	 * @param remoteDb
	 * @param implementationId
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 */
	public void importLabModuleData(DatabaseUtil remoteDb, int implementationId)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		String database = remoteDb.getDbName();
		String insertQuery;
		String updateQuery;
		String selectQuery;
		String tableName;
		try {
			tableName = "commonlabtest_type";
			// Insert into tmp_commonlabtest_type table...
			insertQuery = "INSERT INTO tmp_" + tableName
					+ " (surrogate_id, implementation_id, test_type_id, name, short_name, test_group, requires_specimen, reference_concept_id, description, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId
					+ "', test_type_id, name, short_name, test_group, requires_specimen, reference_concept_id, description, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, uuid FROM "
					+ database + "." + tableName + " AS t " + filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database + "." + tableName + " into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), targetDb.getConnection());
			// Insert new records
			insertQuery = "INSERT IGNORE INTO " + tableName + " SELECT DISTINCT * FROM tmp_" + tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM " + tableName
					+ " WHERE implementation_id = t.implementation_id AND test_type_id = t.test_type_id)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			// Update the existing records
			updateQuery = "UPDATE " + tableName + " AS a, tmp_" + tableName
					+ " AS t SET a.name = t.name, a.short_name = t.short_name, a.test_group = t.test_group, a.requires_specimen = t.requires_specimen, a.reference_concept_id = t.reference_concept_id, a.description = t.description, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.retired = t.retired, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retire_reason = t.retire_reason WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.uuid = t.uuid";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);

			tableName = "commonlabtest_attribute_type";
			// Insert into tmp_commonlabtest_attribute_type table...
			insertQuery = "INSERT INTO tmp_" + tableName
					+ " (surrogate_id, implementation_id, test_attribute_type_id, test_type_id, name, datatype, min_occurs, max_occurs, datatype_config, handler_config, sort_weight, description, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, preferred_handler, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId
					+ "', test_attribute_type_id, test_type_id, name, datatype, min_occurs, max_occurs, datatype_config, handler_config, sort_weight, description, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, preferred_handler, uuid FROM "
					+ database + "." + tableName + " AS t " + filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database + "." + tableName + " into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), targetDb.getConnection());
			// Insert new records
			insertQuery = "INSERT IGNORE INTO " + tableName + " SELECT DISTINCT * FROM tmp_" + tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM " + tableName
					+ " WHERE implementation_id = t.implementation_id AND test_type_id = t.test_type_id AND test_attribute_type_id = t.test_attribute_type_id)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			// Update the existing records
			updateQuery = "UPDATE " + tableName + " AS a, tmp_" + tableName
					+ " AS t SET a.name = t.name, a.datatype = t.datatype, a.min_occurs = t.min_occurs, a.max_occurs = t.max_occurs, a.datatype_config = t.datatype_config, a.handler_config = t.handler_config, a.sort_weight = t.sort_weight, a.description = t.description, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.retired = t.retired, a.retired_by = t.retired_by, a.date_retired = t.date_retired, a.retire_reason = t.retire_reason, a.preferred_handler = t.preferred_handler WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.uuid = t.uuid";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);

			tableName = "commonlabtest_test";
			// Insert into tmp_commonlabtest_test table...
			insertQuery = "INSERT INTO tmp_" + tableName
					+ " (surrogate_id, implementation_id, test_order_id, test_type_id, lab_reference_number, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason, instructions, report_file_path, result_comments, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId
					+ "', test_order_id, test_type_id, lab_reference_number, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason, instructions, report_file_path, result_comments, uuid FROM "
					+ database + "." + tableName + " AS t " + filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database + "." + tableName + " into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), targetDb.getConnection());
			// Insert new records
			insertQuery = "INSERT IGNORE INTO " + tableName + " SELECT DISTINCT * FROM tmp_" + tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM " + tableName
					+ " WHERE implementation_id = t.implementation_id AND test_type_id = t.test_type_id AND test_order_id = t.test_order_id)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			// Update the existing records
			updateQuery = "UPDATE " + tableName + " AS a, tmp_" + tableName
					+ " AS t SET a.lab_reference_number = t.lab_reference_number, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.voided = t.voided, a.voided_by = t.voided_by, a.date_voided = t.date_voided, a.void_reason = t.void_reason, a.instructions = t.instructions, a.report_file_path = t.report_file_path, a.result_comments = t.result_comments WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.uuid = t.uuid";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);

			tableName = "commonlabtest_sample";
			// Insert into tmp_commonlabtest_sample table...
			insertQuery = "INSERT INTO tmp_" + tableName
					+ " (surrogate_id, implementation_id, test_sample_id, test_order_id, specimen_type, specimen_site, is_expirable, expiry_datetime, lab_sample_identifier, collector, status, comments, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason, collection_date, processed_date, quantity, units, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId
					+ "', test_sample_id, test_order_id, specimen_type, specimen_site, is_expirable, expiry_datetime, lab_sample_identifier, collector, status, comments, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason, collection_date, processed_date, quantity, units, uuid FROM "
					+ database + "." + tableName + " AS t " + filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database + "." + tableName + " into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), targetDb.getConnection());
			// Insert new records
			insertQuery = "INSERT IGNORE INTO " + tableName + " SELECT DISTINCT * FROM tmp_" + tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM " + tableName
					+ " WHERE implementation_id = t.implementation_id AND test_sample_id = t.test_sample_id AND test_order_id = t.test_order_id)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			// Update the existing records
			updateQuery = "UPDATE " + tableName + " AS a, tmp_" + tableName
					+ " AS t SET a.specimen_type = t.specimen_type, a.specimen_site = t.specimen_site, a.is_expirable = t.is_expirable, a.expiry_datetime = t.expiry_datetime, a.lab_sample_identifier = t.lab_sample_identifier, a.collector = t.collector, a.status = t.status, a.comments = t.comments, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.voided = t.voided, a.voided_by = t.voided_by, a.date_voided = t.date_voided, a.void_reason = t.void_reason, a.collection_date = t.collection_date, a.processed_date = t.processed_date, a.quantity = t.quantity, a.units = t.units WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.uuid = t.uuid";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);

			tableName = "commonlabtest_attribute";
			// Insert into tmp_commonlabtest_attribute table...
			insertQuery = "INSERT INTO tmp_" + tableName
					+ " (surrogate_id, implementation_id, test_attribute_id, test_order_id, attribute_type_id, value_reference, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason, uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			selectQuery = "SELECT 0,'" + implementationId
					+ "', test_attribute_id, test_order_id, attribute_type_id, value_reference, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason, uuid FROM "
					+ database + "." + tableName + " AS t " + filter("t.date_created", "t.date_changed");
			log.info("Inserting data from " + database + "." + tableName + " into data warehouse");
			remoteSelectInsert(selectQuery, insertQuery, remoteDb.getConnection(), targetDb.getConnection());
			// Insert new records
			insertQuery = "INSERT IGNORE INTO " + tableName + " SELECT DISTINCT * FROM tmp_" + tableName
					+ " AS t WHERE NOT EXISTS (SELECT * FROM " + tableName
					+ " WHERE implementation_id = t.implementation_id AND test_attribute_id = t.test_attribute_id AND test_order_id = t.test_order_id)";
			targetDb.runCommand(CommandType.INSERT, insertQuery);
			// Update the existing records
			updateQuery = "UPDATE " + tableName + " AS a, tmp_" + tableName
					+ " AS t SET a.test_order_id = t.test_order_id, a.attribute_type_id = t.attribute_type_id, a.value_reference = t.value_reference, a.creator = t.creator, a.date_created = t.date_created, a.changed_by = t.changed_by, a.date_changed = t.date_changed, a.voided = t.voided, a.voided_by = t.voided_by, a.date_voided = t.date_voided, a.void_reason = t.void_reason WHERE a.implementation_id = t.implementation_id = '"
					+ implementationId + "' AND a.uuid = t.uuid";
			targetDb.runCommand(CommandType.UPDATE, updateQuery);
		} catch (SQLException e) {
			log.warning(e.getMessage());
		}
	}

	/**
	 * Load data from facility data module tables into data warehouse
	 * 
	 * @param remoteDb
	 * @param implementationId
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 */
	public void importFacilityModuleData(DatabaseUtil remoteDb, int implementationId)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		// TODO:
	}

}
