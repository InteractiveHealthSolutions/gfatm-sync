/*
Copyright(C) 2016 Interactive Health Solutions, Pvt. Ltd.

This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation; either version 3 of the License (GPLv3), or any later version.
This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
See the GNU General Public License for more details.
You should have received a copy of the GNU General Public License along with this program; if not, write to the Interactive Health Solutions, info@ihsinformatics.com
You can also access the license on the internet at the address: http://www.gnu.org/licenses/gpl-3.0.html
Interactive Health Solutions, hereby disclaims all copyright interest in this program written by the contributors. */
package com.ihsinformatics.gfatmimport;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.logging.Level;

import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.ihsinformatics.util.CommandType;
import com.ihsinformatics.util.DatabaseUtil;
import com.ihsinformatics.util.DateTimeUtil;

/**
 * @author owais.hussain@ihsinformatics.com
 * 
 */
public class ImportJob implements Job {

	final String[] USER_TABLES = {"privilege", "provider_attribute_type",
			"provider", "provider_attribute", "role", "role_privilege",
			"role_role", "users", "user_property", "user_role"};
	final String[] LOCATION_TABLES = {"location_attribute_type", "location",
			"location_attribute"};
	final String[] CONCEPT_TABLES = {"concept_class", "concept_datatype",
			"concept_map_type", "concept_reference_source",
			"concept_stop_word", "concept", "concept_answer",
			"concept_description", "concept_name", "concept_numeric",
			"concept_set"};
	final String[] LAB_MODULE_TABLES = {};
	final String[] OTHER_METADATA_TABLES = {"encounter_role", "encounter_type",
			"field_type", "form_resource", "field", "form_field", "form",
			"field_answer", "hl7_source", "htmlformentry_html_form",
			"patient_identifier_type", "person_attribute_type", "program",
			"program_workflow", "program_workflow_state", "order_type",
			"scheduler_task_config", "scheduler_task_config_property",
			"visit_type", "visit_attribute_type"};

	private DatabaseUtil localDb;
	private DatabaseUtil remoteDb;

	private boolean importUsers = false;
	private boolean importLocations = false;
	private boolean importConcepts = false;
	private boolean importLabData = false;
	private boolean importOtherMetadata;
	private boolean filterDate = true;
	private Date dateFrom;
	private Date dateTo;
	private int progressRange = 0;

	public ImportJob() {
	}

	public void initialize(ImportJob importJob) {
		setLocalDb(importJob.getLocalDb());
		setRemoteDb(importJob.getRemoteDb());
		importUsers = importJob.importUsers;
		importLocations = importJob.importLocations;
		importConcepts = importJob.importConcepts;
		importLabData = importJob.importLabData;
		importOtherMetadata = importJob.importOtherMetadata;
		filterDate = importJob.filterDate;
		dateFrom = importJob.dateFrom;
		dateTo = importJob.dateTo;
		progressRange = importJob.progressRange;
		// Maximum progress is the number of tables * 3 (import, insert and
		// update)
		progressRange = (importUsers ? USER_TABLES.length : 0)
				+ (importLocations ? LOCATION_TABLES.length : 0)
				+ (importConcepts ? CONCEPT_TABLES.length : 0)
				+ (importLabData ? LAB_MODULE_TABLES.length : 0)
				+ (importOtherMetadata ? OTHER_METADATA_TABLES.length : 0)
				* 3;
		GfatmImportMain.gfatmImport.resetProgressBar(0, progressRange);
	}

	public void execute(JobExecutionContext context)
			throws JobExecutionException {
		GfatmImportMain.gfatmImport.setMode(ImportStatus.IMPORTING);
		JobDataMap dataMap = context.getMergedJobDataMap();
		ImportJob importJob = (ImportJob) dataMap.get("importJob");
		initialize(importJob);
		// Disable foreign key check
		try {
			Statement statement = localDb.getConnection().createStatement();
			statement.execute("SET FOREIGN_KEY_CHECKS=0");
		} catch (SQLException e1) {
			e1.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		if (importUsers && GfatmImportMain.mode != ImportStatus.STOPPED) {
			try {
				importUsers();
				updateUsers();
			} catch (Exception e) {
				GfatmImportMain.gfatmImport.log("User data import incomplete. "
						+ e.getMessage(), Level.WARNING);
			}
		}
		if (importLocations && GfatmImportMain.mode != ImportStatus.STOPPED) {
			try {
				importLocations();
				updateLocations();
			} catch (Exception e) {
				GfatmImportMain.gfatmImport.log(
						"Location data import incomplete. " + e.getMessage(),
						Level.WARNING);
			}
		}
		if (importConcepts && GfatmImportMain.mode != ImportStatus.STOPPED) {
			try {
				importConcepts();
				updateConcepts();
			} catch (Exception e) {
				GfatmImportMain.gfatmImport.log(
						"Concept data import incomplete. " + e.getMessage(),
						Level.WARNING);
			}
		}

		if (importLabData && GfatmImportMain.mode != ImportStatus.STOPPED) {
			try {
				importLabMetadata();
				updateLabMetadata();
			} catch (Exception e) {
				GfatmImportMain.gfatmImport.log(
						"Common Lab data import incomplete. " + e.getMessage(),
						Level.WARNING);
			}
		}
		
		if (importOtherMetadata && GfatmImportMain.mode != ImportStatus.STOPPED) {
			try {
				importMetadata();
				updateMetadata();
			} catch (Exception e) {
				GfatmImportMain.gfatmImport.log("Metadata import incomplete. "
						+ e.getMessage(), Level.WARNING);
			}
		}
		// Enable foreign key check
		try {
			Statement statement = localDb.getConnection().createStatement();
			statement.execute("SET FOREIGN_KEY_CHECKS=1");
		} catch (SQLException e1) {
			e1.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		GfatmImportMain.gfatmImport.updateProgress(progressRange);
		GfatmImportMain.gfatmImport.setMode(ImportStatus.WAITING);
	}

	/**
	 * Returns a filter for select queries
	 * 
	 * @param createDateName
	 * @param updateDateName
	 * @return
	 */
	public String filter(String createDateName, String updateDateName) {
		StringBuilder filter = new StringBuilder(" WHERE 1=1 ");
		if (filterDate & getDateFrom() != null & getDateTo() != null) {
			filter.append("AND " + createDateName);
			filter.append(" BETWEEN TIMESTAMP('"
					+ DateTimeUtil.toSqlDateTimeString(getDateFrom()) + "') ");
			filter.append("AND CURRENT_TIMESTAMP()");
			if (updateDateName != null) {
				filter.append(" OR " + updateDateName);
				filter.append(" BETWEEN TIMESTAMP('"
						+ DateTimeUtil.toSqlDateTimeString(getDateFrom()) + "') ");
				filter.append(" AND CURRENT_TIMESTAMP()");
			}
		}
		return filter.toString();
	}

	/**
	 * Import data from user-related tables, including user rights management
	 * tables
	 * 
	 * @throws SQLException
	 */
	public void importUsers() throws SQLException {
		GfatmImportMain.gfatmImport.log("Importing users from remote source",
				Level.INFO);
		String selectQuery = "";
		String insertQuery = "";

		// users
		createTempTable(getLocalDb(), "users");
		insertQuery = "INSERT INTO temp_users(user_id,system_id,username,password,salt,secret_question,secret_answer,creator,date_created,changed_by,date_changed,person_id,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT user_id,system_id,username,password,salt,secret_question,secret_answer,creator,date_created,changed_by,date_changed,person_id,retired,retired_by,date_retired,retire_reason,uuid FROM users "
				+ filter("date_created", "date_changed")
				+ " ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO users(user_id,system_id,username,password,salt,secret_question,secret_answer,creator,date_created,changed_by,date_changed,person_id,retired,retired_by,date_retired,retire_reason,uuid) SELECT user_id,system_id,username,password,salt,secret_question,secret_answer,creator,date_created,changed_by,date_changed,person_id,retired,retired_by,date_retired,retire_reason,uuid FROM temp_users";
		localInsert(insertQuery);

		// person (only for users)
		createTempTable(getLocalDb(), "person");
		insertQuery = "INSERT INTO temp_person(person_id,gender,birthdate,birthdate_estimated,dead,death_date,cause_of_death,creator,date_created,changed_by,date_changed,voided,voided_by,date_voided,void_reason,uuid,deathdate_estimated)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT person_id,gender,birthdate,birthdate_estimated,dead,death_date,cause_of_death,creator,date_created,changed_by,date_changed,voided,voided_by,date_voided,void_reason,uuid,deathdate_estimated FROM person "
				+ filter("date_created", "date_changed")
				+ " AND person_id IN (SELECT person_id FROM users)"
				+ " ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO person(person_id,gender,birthdate,birthdate_estimated,dead,death_date,cause_of_death,creator,date_created,changed_by,date_changed,voided,voided_by,date_voided,void_reason,uuid,deathdate_estimated) SELECT person_id,gender,birthdate,birthdate_estimated,dead,death_date,cause_of_death,creator,date_created,changed_by,date_changed,voided,voided_by,date_voided,void_reason,uuid,deathdate_estimated FROM temp_person";
		localInsert(insertQuery);

		// person_name (only for users)
		createTempTable(getLocalDb(), "person_name");
		insertQuery = "INSERT INTO temp_person_name(person_name_id,preferred,person_id,prefix,given_name,middle_name,family_name_prefix,family_name,family_name2,family_name_suffix,degree,creator,date_created,voided,voided_by,date_voided,void_reason,changed_by,date_changed,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT person_name_id,preferred,person_id,prefix,given_name,middle_name,family_name_prefix,family_name,family_name2,family_name_suffix,degree,creator,date_created,voided,voided_by,date_voided,void_reason,changed_by,date_changed,uuid FROM person_name "
				+ filter("date_created", "date_changed")
				+ " AND person_id IN (SELECT person_id FROM users)"
				+ " ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO person_name(person_name_id,preferred,person_id,prefix,given_name,middle_name,family_name_prefix,family_name,family_name2,family_name_suffix,degree,creator,date_created,voided,voided_by,date_voided,void_reason,changed_by,date_changed,uuid) SELECT person_name_id,preferred,person_id,prefix,given_name,middle_name,family_name_prefix,family_name,family_name2,family_name_suffix,degree,creator,date_created,voided,voided_by,date_voided,void_reason,changed_by,date_changed,uuid FROM temp_person_name";
		localInsert(insertQuery);

		// provider_attribute_type
		createTempTable(getLocalDb(), "provider_attribute_type");
		insertQuery = "INSERT INTO temp_provider_attribute_type(provider_attribute_type_id,name,description,datatype,datatype_config,preferred_handler,handler_config,min_occurs,max_occurs,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT provider_attribute_type_id,name,description,datatype,datatype_config,preferred_handler,handler_config,min_occurs,max_occurs,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid FROM provider_attribute_type "
				+ filter("date_created", "date_changed")
				+ " ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO provider_attribute_type(provider_attribute_type_id,name,description,datatype,datatype_config,preferred_handler,handler_config,min_occurs,max_occurs,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid) SELECT provider_attribute_type_id,name,description,datatype,datatype_config,preferred_handler,handler_config,min_occurs,max_occurs,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid FROM temp_provider_attribute_type";
		localInsert(insertQuery);

		// provider
		createTempTable(getLocalDb(), "provider");
		insertQuery = "INSERT INTO temp_provider(provider_id,person_id,name,identifier,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT provider_id,person_id,name,identifier,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid FROM provider "
				+ filter("date_created", "date_changed")
				+ " ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO provider(provider_id,person_id,name,identifier,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid) SELECT provider_id,person_id,name,identifier,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid FROM temp_provider";
		localInsert(insertQuery);

		// provider_attribute
		createTempTable(getLocalDb(), "provider_attribute");
		insertQuery = "INSERT INTO temp_provider_attribute(provider_attribute_id,provider_id,attribute_type_id,value_reference,creator,date_created,changed_by,date_changed,voided,voided_by,date_voided,void_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT provider_attribute_id,provider_id,attribute_type_id,value_reference,creator,date_created,changed_by,date_changed,voided,voided_by,date_voided,void_reason,uuid FROM provider_attribute "
				+ filter("date_created", "date_changed")
				+ " ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO provider_attribute(provider_attribute_id,provider_id,attribute_type_id,value_reference,creator,date_created,changed_by,date_changed,voided,voided_by,date_voided,void_reason,uuid) SELECT provider_attribute_id,provider_id,attribute_type_id,value_reference,creator,date_created,changed_by,date_changed,voided,voided_by,date_voided,void_reason,uuid FROM temp_provider_attribute";
		localInsert(insertQuery);

		// role
		createTempTable(getLocalDb(), "role");
		insertQuery = "INSERT INTO temp_role(role,description,uuid)VALUES(?,?,?)";
		selectQuery = "SELECT role,description,uuid FROM role";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO role(role,description,uuid) SELECT role,description,uuid FROM temp_role";
		localInsert(insertQuery);

		// role_privilege
		createTempTable(getLocalDb(), "role_privilege");
		insertQuery = "INSERT INTO temp_role_privilege(role,privilege)VALUES(?,?)";
		selectQuery = "SELECT role,privilege FROM role_privilege";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO role_privilege(role,privilege) SELECT role,privilege FROM temp_role_privilege";
		localInsert(insertQuery);

		// role_role
		createTempTable(getLocalDb(), "role_role");
		insertQuery = "INSERT INTO temp_role_role(parent_role,child_role)VALUES(?,?)";
		selectQuery = "SELECT parent_role,child_role FROM role_role";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO role_role(parent_role,child_role) SELECT parent_role,child_role FROM temp_role_role";
		localInsert(insertQuery);

		// user_property
		createTempTable(getLocalDb(), "user_property");
		insertQuery = "INSERT INTO temp_user_property(user_id,property,property_value)VALUES(?,?,?)";
		selectQuery = "SELECT user_id,property,property_value FROM user_property";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO user_property(user_id,property,property_value) SELECT user_id,property,property_value FROM temp_user_property";
		localInsert(insertQuery);

		// user_role
		createTempTable(getLocalDb(), "user_role");
		insertQuery = "INSERT IGNORE INTO temp_user_role(user_id,role)VALUES(?,?)";
		selectQuery = "SELECT user_id,role FROM user_role";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO user_role(user_id,role) SELECT user_id,role FROM temp_user_role";
		localInsert(insertQuery);
	}

	/**
	 * Import data from location-related tables
	 * 
	 * @throws SQLException
	 */
	public void importLocations() throws SQLException {
		GfatmImportMain.gfatmImport.log(
				"Importing locations from remote source", Level.INFO);
		String selectQuery = "";
		String insertQuery = "";

		// location
		createTempTable(getLocalDb(), "location");
		insertQuery = "INSERT INTO temp_location(location_id,name,description,address1,address2,city_village,state_province,postal_code,country,latitude,longitude,creator,date_created,county_district,address3,address4,address5,address6,retired,retired_by,date_retired,retire_reason,parent_location,uuid,changed_by,date_changed)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT location_id,name,description,address1,address2,city_village,state_province,postal_code,country,latitude,longitude,creator,date_created,county_district,address3,address4,address5,address6,retired,retired_by,date_retired,retire_reason,parent_location,uuid,changed_by,date_changed FROM location "
				+ filter("date_created", "date_changed")
				+ " ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO location(location_id,name,description,address1,address2,city_village,state_province,postal_code,country,latitude,longitude,creator,date_created,county_district,address3,address4,address5,address6,retired,retired_by,date_retired,retire_reason,parent_location,uuid,changed_by,date_changed) SELECT location_id,name,description,address1,address2,city_village,state_province,postal_code,country,latitude,longitude,creator,date_created,county_district,address3,address4,address5,address6,retired,retired_by,date_retired,retire_reason,parent_location,uuid,changed_by,date_changed FROM temp_location";
		localInsert(insertQuery);

		// location_attribute_type
		createTempTable(getLocalDb(), "location_attribute_type");
		insertQuery = "INSERT INTO temp_location_attribute_type(location_attribute_type_id,name,description,datatype,datatype_config,preferred_handler,handler_config,min_occurs,max_occurs,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT location_attribute_type_id,name,description,datatype,datatype_config,preferred_handler,handler_config,min_occurs,max_occurs,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid FROM location_attribute_type "
				+ filter("date_created", "date_changed")
				+ " ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO location_attribute_type(location_attribute_type_id,name,description,datatype,datatype_config,preferred_handler,handler_config,min_occurs,max_occurs,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid) SELECT location_attribute_type_id,name,description,datatype,datatype_config,preferred_handler,handler_config,min_occurs,max_occurs,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid FROM temp_location_attribute_type";
		localInsert(insertQuery);

		// location_attribute
		createTempTable(getLocalDb(), "location_attribute");
		insertQuery = "INSERT INTO temp_location_attribute(location_attribute_id,location_id,attribute_type_id,value_reference,uuid,creator,date_created,changed_by,date_changed,voided,voided_by,date_voided,void_reason)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT location_attribute_id,location_id,attribute_type_id,value_reference,uuid,creator,date_created,changed_by,date_changed,voided,voided_by,date_voided,void_reason FROM location_attribute "
				+ filter("date_created", "date_changed")
				+ " ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO location_attribute(location_attribute_id,location_id,attribute_type_id,value_reference,uuid,creator,date_created,changed_by,date_changed,voided,voided_by,date_voided,void_reason) SELECT location_attribute_id,location_id,attribute_type_id,value_reference,uuid,creator,date_created,changed_by,date_changed,voided,voided_by,date_voided,void_reason FROM temp_location_attribute";
		localInsert(insertQuery);

		// location_tag
		createTempTable(getLocalDb(), "location_tag");
		insertQuery = "INSERT INTO temp_location_tag(location_tag_id,name,description,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid,changed_by,date_changed)VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT location_tag_id,name,description,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid,changed_by,date_changed FROM location_tag "
				+ filter("date_created", "date_changed")
				+ " ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO location_tag(location_tag_id,name,description,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid,changed_by,date_changed) SELECT location_tag_id,name,description,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid,changed_by,date_changed FROM temp_location_tag";
		localInsert(insertQuery);

		// location_tag_map
		createTempTable(getLocalDb(), "location_tag_map");
		insertQuery = "INSERT INTO temp_location_tag_map(location_id,location_tag_id)VALUES(?,?)";
		selectQuery = "SELECT location_id,location_tag_id FROM location_tag_map";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO location_tag_map(location_id,location_tag_id) SELECT location_id,location_tag_id FROM temp_location_tag_map";
		localInsert(insertQuery);
	}

	/**
	 * Import data from concept tables
	 * 
	 * @throws SQLException
	 */
	public void importConcepts() throws SQLException {
		GfatmImportMain.gfatmImport.log(
				"Importing metadata from remote source", Level.INFO);
		String selectQuery = "";
		String insertQuery = "";

		// concept_class
		createTempTable(getLocalDb(), "concept_class");
		insertQuery = "INSERT INTO temp_concept_class(concept_class_id,name,description,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT concept_class_id,name,description,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid FROM concept_class "
				+ filter("date_created", null) + " ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO concept_class(concept_class_id,name,description,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid) SELECT concept_class_id,name,description,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid FROM temp_concept_class";
		localInsert(insertQuery);

		// concept_datatype
		createTempTable(getLocalDb(), "concept_datatype");
		insertQuery = "INSERT INTO temp_concept_datatype(concept_datatype_id,name,hl7_abbreviation,description,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT concept_datatype_id,name,hl7_abbreviation,description,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid FROM concept_datatype "
				+ filter("date_created", null) + " ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO concept_datatype(concept_datatype_id,name,hl7_abbreviation,description,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid) SELECT concept_datatype_id,name,hl7_abbreviation,description,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid FROM temp_concept_datatype";
		localInsert(insertQuery);

		// concept_map_type
		createTempTable(getLocalDb(), "concept_map_type");
		insertQuery = "INSERT INTO temp_concept_map_type(concept_map_type_id,name,description,creator,date_created,changed_by,date_changed,is_hidden,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT concept_map_type_id,name,description,creator,date_created,changed_by,date_changed,is_hidden,retired,retired_by,date_retired,retire_reason,uuid FROM concept_map_type "
				+ filter("date_created", null) + " ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO concept_map_type(concept_map_type_id,name,description,creator,date_created,changed_by,date_changed,is_hidden,retired,retired_by,date_retired,retire_reason,uuid) SELECT concept_map_type_id,name,description,creator,date_created,changed_by,date_changed,is_hidden,retired,retired_by,date_retired,retire_reason,uuid FROM temp_concept_map_type";
		localInsert(insertQuery);

		// concept_reference_source
		createTempTable(getLocalDb(), "concept_reference_source");
		insertQuery = "INSERT INTO temp_concept_reference_source(concept_source_id,name,description,hl7_code,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT concept_source_id,name,description,hl7_code,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid FROM concept_reference_source "
				+ filter("date_created", null) + " ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO concept_reference_source(concept_source_id,name,description,hl7_code,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid) SELECT concept_source_id,name,description,hl7_code,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid FROM temp_concept_reference_source";
		localInsert(insertQuery);

		// concept_reference_map
		createTempTable(getLocalDb(), "concept_reference_map");
		insertQuery = "INSERT INTO temp_concept_reference_map(concept_map_id,creator,date_created,concept_id,uuid,concept_reference_term_id,concept_map_type_id,changed_by,date_changed)VALUES(?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT concept_map_id,creator,date_created,concept_id,uuid,concept_reference_term_id,concept_map_type_id,changed_by,date_changed FROM concept_reference_map "
				+ filter("date_created", null) + " ORDER BY date_created";
		// remoteSelectInsert(selectQuery, insertQuery);
		// insertQuery =
		// "INSERT IGNORE INTO concept_reference_map(concept_map_id,creator,date_created,concept_id,uuid,concept_reference_term_id,concept_map_type_id,changed_by,date_changed) SELECT concept_map_id,creator,date_created,concept_id,uuid,concept_reference_term_id,concept_map_type_id,changed_by,date_changed FROM temp_concept_reference_map";
		// localInsert(insertQuery);

		// concept_reference_map
		createTempTable(getLocalDb(), "concept_reference_term");
		insertQuery = "INSERT INTO temp_concept_reference_term(concept_reference_term_id,concept_source_id,name,code,version,description,creator,date_created,date_changed,changed_by,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT concept_reference_term_id,concept_source_id,name,code,version,description,creator,date_created,date_changed,changed_by,retired,retired_by,date_retired,retire_reason,uuid FROM concept_reference_term "
				+ filter("date_created", null) + " ORDER BY date_created";
		// remoteSelectInsert(selectQuery, insertQuery);
		// insertQuery =
		// "INSERT IGNORE INTO concept_reference_term(concept_reference_term_id,concept_source_id,name,code,version,description,creator,date_created,date_changed,changed_by,retired,retired_by,date_retired,retire_reason,uuid) SELECT concept_reference_term_id,concept_source_id,name,code,version,description,creator,date_created,date_changed,changed_by,retired,retired_by,date_retired,retire_reason,uuid FROM temp_concept_reference_term";
		// localInsert(insertQuery);

		// concept_stop_word
		createTempTable(getLocalDb(), "concept_stop_word");
		insertQuery = "INSERT INTO temp_concept_stop_word(concept_stop_word_id,word,locale,uuid)VALUES(?,?,?,?)";
		selectQuery = "SELECT concept_stop_word_id,word,locale,uuid FROM concept_stop_word";
		// remoteSelectInsert(selectQuery, insertQuery);
		// insertQuery =
		// "INSERT IGNORE INTO concept_stop_word(concept_stop_word_id,word,locale,uuid) SELECT concept_stop_word_id,word,locale,uuid FROM temp_concept_stop_word";
		// localInsert(insertQuery);

		// concept
		createTempTable(getLocalDb(), "concept");
		insertQuery = "INSERT INTO temp_concept(concept_id,retired,short_name,description,form_text,datatype_id,class_id,is_set,creator,date_created,version,changed_by,date_changed,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT concept_id,retired,short_name,description,form_text,datatype_id,class_id,is_set,creator,date_created,version,changed_by,date_changed,retired_by,date_retired,retire_reason,uuid FROM concept "
				+ filter("date_created", "date_changed")
				+ " ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO concept(concept_id,retired,short_name,description,form_text,datatype_id,class_id,is_set,creator,date_created,version,changed_by,date_changed,retired_by,date_retired,retire_reason,uuid) SELECT concept_id,retired,short_name,description,form_text,datatype_id,class_id,is_set,creator,date_created,version,changed_by,date_changed,retired_by,date_retired,retire_reason,uuid FROM temp_concept";
		localInsert(insertQuery);

		// concept_description
		createTempTable(getLocalDb(), "concept_description");
		insertQuery = "INSERT INTO temp_concept_description(concept_description_id,concept_id,description,locale,creator,date_created,changed_by,date_changed,uuid)VALUES(?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT concept_description_id,concept_id,description,locale,creator,date_created,changed_by,date_changed,uuid FROM concept_description "
				+ filter("date_created", "date_changed")
				+ " ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO concept_description(concept_description_id,concept_id,description,locale,creator,date_created,changed_by,date_changed,uuid) SELECT concept_description_id,concept_id,description,locale,creator,date_created,changed_by,date_changed,uuid FROM temp_concept_description";
		localInsert(insertQuery);

		// concept_answer
		createTempTable(getLocalDb(), "concept_answer");
		insertQuery = "INSERT INTO temp_concept_answer(concept_answer_id,concept_id,answer_concept,answer_drug,creator,date_created,uuid,sort_weight)VALUES(?,?,?,?,?,?,?,?)";
		// Also import the names whose concepts were changed
		Object[][] data = getLocalDb().getTableData("temp_concept",
				"concept_id", null, true);
		StringBuilder ids = new StringBuilder();
		for (Object[] o : data) {
			ids.append(o[0].toString() + ",");
		}
		ids.append("0");
		selectQuery = "SELECT concept_answer_id,concept_id,answer_concept,answer_drug,creator,date_created,uuid,sort_weight FROM concept_answer "
				+ filter("date_created", null)
				+ " OR concept_id IN ("
				+ ids.toString() + ")" + " ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO concept_answer(concept_answer_id,concept_id,answer_concept,answer_drug,creator,date_created,uuid,sort_weight) SELECT concept_answer_id,concept_id,answer_concept,answer_drug,creator,date_created,uuid,sort_weight FROM temp_concept_answer";
		localInsert(insertQuery);

		// concept_name
		createTempTable(getLocalDb(), "concept_name");
		insertQuery = "INSERT INTO temp_concept_name(concept_id,name,locale,creator,date_created,concept_name_id,voided,voided_by,date_voided,void_reason,uuid,concept_name_type,locale_preferred)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)";
		// Also import the names whose concepts were changed
		selectQuery = "SELECT concept_id,name,locale,creator,date_created,concept_name_id,voided,voided_by,date_voided,void_reason,uuid,concept_name_type,locale_preferred FROM concept_name "
				+ filter("date_created", null)
				+ " OR concept_id IN ("
				+ ids.toString() + ")" + " ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO concept_name(concept_id,name,locale,creator,date_created,concept_name_id,voided,voided_by,date_voided,void_reason,uuid,concept_name_type,locale_preferred) SELECT concept_id,name,locale,creator,date_created,concept_name_id,voided,voided_by,date_voided,void_reason,uuid,concept_name_type,locale_preferred FROM temp_concept_name";
		localInsert(insertQuery);

		// concept_numeric
		createTempTable(getLocalDb(), "concept_numeric");
		insertQuery = "INSERT INTO temp_concept_numeric(concept_id,hi_absolute,hi_critical,hi_normal,low_absolute,low_critical,low_normal,units,precise,display_precision)VALUES(?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT concept_id,hi_absolute,hi_critical,hi_normal,low_absolute,low_critical,low_normal,units,precise,display_precision FROM concept_numeric ";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO concept_numeric(concept_id,hi_absolute,hi_critical,hi_normal,low_absolute,low_critical,low_normal,units,precise,display_precision) SELECT concept_id,hi_absolute,hi_critical,hi_normal,low_absolute,low_critical,low_normal,units,precise,display_precision FROM temp_concept_numeric";
		localInsert(insertQuery);

		// concept_set
		createTempTable(getLocalDb(), "concept_set");
		insertQuery = "INSERT INTO temp_concept_set(concept_set_id,concept_id,concept_set,sort_weight,creator,date_created,uuid)VALUES(?,?,?,?,?,?,?)";
		selectQuery = "SELECT concept_set_id,concept_id,concept_set,sort_weight,creator,date_created,uuid FROM concept_set "
				+ filter("date_created", null) + " ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO concept_set(concept_set_id,concept_id,concept_set,sort_weight,creator,date_created,uuid) SELECT concept_set_id,concept_id,concept_set,sort_weight,creator,date_created,uuid FROM temp_concept_set";
		localInsert(insertQuery);
	}
	
	/**
	 * Import data from lab module metadata tables
	 * 
	 * @throws SQLException
	 */
	public void importLabMetadata() throws SQLException {
		GfatmImportMain.gfatmImport.log(
				"Importing lab module's metadata from remote source", Level.INFO);
		String selectQuery = "";
		String insertQuery = "";

		// commonlabtest_type
		createTempTable(getLocalDb(), "commonlabtest_type");
		insertQuery = "INSERT INTO temp_commonlabtest_type(test_type_id,name,short_name,test_group,requires_specimen,reference_concept_id,description,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT test_type_id,name,short_name,test_group,requires_specimen,reference_concept_id,description,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid FROM commonlabtest_type "
				+ filter("date_created", "date_changed")
				+ " ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO commonlabtest_type(test_type_id,name,short_name,test_group,requires_specimen,reference_concept_id,description,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid) SELECT test_type_id,name,short_name,test_group,requires_specimen,reference_concept_id,description,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid FROM temp_commonlabtest_type";
		localInsert(insertQuery);
		
		// commonlabtest_attribute_type
		createTempTable(getLocalDb(), "commonlabtest_attribute_type");
		insertQuery = "INSERT INTO temp_commonlabtest_attribute_type(test_attribute_type_id,test_type_id,name,datatype,min_occurs,max_occurs,datatype_config,handler_config,sort_weight,description,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid,preferred_handler,hint)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT test_attribute_type_id,test_type_id,name,datatype,min_occurs,max_occurs,datatype_config,handler_config,sort_weight,description,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid,preferred_handler,hint FROM commonlabtest_attribute_type "
				+ filter("date_created", "date_changed")
				+ " ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO commonlabtest_attribute_type(test_attribute_type_id,test_type_id,name,datatype,min_occurs,max_occurs,datatype_config,handler_config,sort_weight,description,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid,preferred_handler,hint) SELECT test_attribute_type_id,test_type_id,name,datatype,min_occurs,max_occurs,datatype_config,handler_config,sort_weight,description,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid,preferred_handler,hint FROM temp_commonlabtest_attribute_type";
		localInsert(insertQuery);
	}

	/**
	 * Import data from metadata tables
	 * 
	 * @throws SQLException
	 */
	public void importMetadata() throws SQLException {
		GfatmImportMain.gfatmImport.log(
				"Importing metadata from remote source", Level.INFO);
		String selectQuery = "";
		String insertQuery = "";

		// encounter_role
		createTempTable(getLocalDb(), "encounter_role");
		insertQuery = "INSERT INTO temp_encounter_role(encounter_role_id,name,description,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT encounter_role_id,name,description,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid FROM encounter_role "
				+ filter("date_created", "date_changed")
				+ " ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO encounter_role(encounter_role_id,name,description,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid) SELECT encounter_role_id,name,description,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid FROM temp_encounter_role";
		localInsert(insertQuery);

		// encounter_type
		createTempTable(getLocalDb(), "encounter_type");
		insertQuery = "INSERT INTO temp_encounter_type(encounter_type_id,name,description,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid,edit_privilege,view_privilege)VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT encounter_type_id,name,description,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid,edit_privilege,view_privilege FROM encounter_type "
				+ filter("date_created", null) + " ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO encounter_type(encounter_type_id,name,description,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid,edit_privilege,view_privilege) SELECT encounter_type_id,name,description,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid,edit_privilege,view_privilege FROM temp_encounter_type";
		localInsert(insertQuery);

		// field_type
		createTempTable(getLocalDb(), "field_type");
		insertQuery = "INSERT INTO temp_field_type(field_type_id,name,description,is_set,creator,date_created,uuid)VALUES(?,?,?,?,?,?,?)";
		selectQuery = "SELECT field_type_id,name,description,is_set,creator,date_created,uuid FROM field_type "
				+ filter("date_created", null) + " ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO field_type(field_type_id,name,description,is_set,creator,date_created,uuid) SELECT field_type_id,name,description,is_set,creator,date_created,uuid FROM temp_field_type";
		localInsert(insertQuery);

		// field
		createTempTable(getLocalDb(), "field");
		insertQuery = "INSERT INTO temp_field(field_id,name,description,field_type,concept_id,table_name,attribute_name,default_value,select_multiple,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT field_id,name,description,field_type,concept_id,table_name,attribute_name,default_value,select_multiple,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid FROM field "
				+ filter("date_created", null) + " ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO field(field_id,name,description,field_type,concept_id,table_name,attribute_name,default_value,select_multiple,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid) SELECT field_id,name,description,field_type,concept_id,table_name,attribute_name,default_value,select_multiple,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid FROM temp_field";
		localInsert(insertQuery);

		// field_answer
		createTempTable(getLocalDb(), "field_answer");
		insertQuery = "INSERT INTO temp_field_answer(field_id,answer_id,creator,date_created,uuid)VALUES(?,?,?,?,?)";
		selectQuery = "SELECT field_id,answer_id,creator,date_created,uuid FROM field_answer "
				+ filter("date_created", null) + " ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO field_answer(field_id,answer_id,creator,date_created,uuid) SELECT field_id,answer_id,creator,date_created,uuid FROM temp_field_answer";
		localInsert(insertQuery);

		// form
		createTempTable(getLocalDb(), "form");
		insertQuery = "INSERT INTO temp_form(form_id,name,version,build,published,xslt,template,description,encounter_type,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retired_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT form_id,name,version,build,published,xslt,template,description,encounter_type,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retired_reason,uuid FROM form "
				+ filter("date_created", "date_changed")
				+ " ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO form(form_id,name,version,build,published,xslt,template,description,encounter_type,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retired_reason,uuid) SELECT form_id,name,version,build,published,xslt,template,description,encounter_type,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retired_reason,uuid FROM temp_form";
		localInsert(insertQuery);

		// form_resource
		createTempTable(getLocalDb(), "form_resource");
		insertQuery = "INSERT INTO temp_form_resource(form_resource_id,form_id,name,value_reference,datatype,datatype_config,preferred_handler,handler_config,uuid)VALUES(?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT form_resource_id,form_id,name,value_reference,datatype,datatype_config,preferred_handler,handler_config,uuid FROM form_resource ";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO form_resource(form_resource_id,form_id,name,value_reference,datatype,datatype_config,preferred_handler,handler_config,uuid) SELECT form_resource_id,form_id,name,value_reference,datatype,datatype_config,preferred_handler,handler_config,uuid FROM temp_form_resource";
		localInsert(insertQuery);

		// form_field
		createTempTable(getLocalDb(), "form_field");
		insertQuery = "INSERT INTO temp_form_field(form_field_id,form_id,field_id,field_number,field_part,page_number,parent_form_field,min_occurs,max_occurs,required,changed_by,date_changed,creator,date_created,sort_weight,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT form_field_id,form_id,field_id,field_number,field_part,page_number,parent_form_field,min_occurs,max_occurs,required,changed_by,date_changed,creator,date_created,sort_weight,uuid FROM form_field "
				+ filter("date_created", "date_changed")
				+ " ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO form_field(form_field_id,form_id,field_id,field_number,field_part,page_number,parent_form_field,min_occurs,max_occurs,required,changed_by,date_changed,creator,date_created,sort_weight,uuid) SELECT form_field_id,form_id,field_id,field_number,field_part,page_number,parent_form_field,min_occurs,max_occurs,required,changed_by,date_changed,creator,date_created,sort_weight,uuid FROM temp_form_field";
		localInsert(insertQuery);

		// hl7_source
		createTempTable(getLocalDb(), "hl7_source");
		insertQuery = "INSERT INTO temp_hl7_source(hl7_source_id,name,description,creator,date_created,uuid)VALUES(?,?,?,?,?,?)";
		selectQuery = "SELECT hl7_source_id,name,description,creator,date_created,uuid FROM hl7_source "
				+ filter("date_created", null) + " ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO hl7_source(hl7_source_id,name,description,creator,date_created,uuid) SELECT hl7_source_id,name,description,creator,date_created,uuid FROM temp_hl7_source";
		localInsert(insertQuery);

		// htmlformentry_html_form
		createTempTable(getLocalDb(), "htmlformentry_html_form");
		insertQuery = "INSERT INTO temp_htmlformentry_html_form(id,form_id,name,xml_data,creator,date_created,changed_by,date_changed,retired,uuid,description,retired_by,date_retired,retire_reason)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT id,form_id,name,xml_data,creator,date_created,changed_by,date_changed,retired,uuid,description,retired_by,date_retired,retire_reason FROM htmlformentry_html_form "
				+ filter("date_created", "date_changed")
				+ " ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO htmlformentry_html_form(id,form_id,name,xml_data,creator,date_created,changed_by,date_changed,retired,uuid,description,retired_by,date_retired,retire_reason) SELECT id,form_id,name,xml_data,creator,date_created,changed_by,date_changed,retired,uuid,description,retired_by,date_retired,retire_reason FROM temp_htmlformentry_html_form";
		localInsert(insertQuery);

		// patient_identifier_type
		createTempTable(getLocalDb(), "patient_identifier_type");
		insertQuery = "INSERT INTO temp_patient_identifier_type(patient_identifier_type_id,name,description,format,check_digit,creator,date_created,required,format_description,validator,location_behavior,retired,retired_by,date_retired,retire_reason,uuid,uniqueness_behavior)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT patient_identifier_type_id,name,description,format,check_digit,creator,date_created,required,format_description,validator,location_behavior,retired,retired_by,date_retired,retire_reason,uuid,uniqueness_behavior FROM patient_identifier_type "
				+ filter("date_created", null) + " ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO patient_identifier_type(patient_identifier_type_id,name,description,format,check_digit,creator,date_created,required,format_description,validator,location_behavior,retired,retired_by,date_retired,retire_reason,uuid,uniqueness_behavior) SELECT patient_identifier_type_id,name,description,format,check_digit,creator,date_created,required,format_description,validator,location_behavior,retired,retired_by,date_retired,retire_reason,uuid,uniqueness_behavior FROM temp_patient_identifier_type";
		localInsert(insertQuery);

		// person_attribute_type
		createTempTable(getLocalDb(), "person_attribute_type");
		insertQuery = "INSERT INTO temp_person_attribute_type(person_attribute_type_id,name,description,format,foreign_key,searchable,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,edit_privilege,sort_weight,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT person_attribute_type_id,name,description,format,foreign_key,searchable,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,edit_privilege,sort_weight,uuid FROM person_attribute_type "
				+ filter("date_created", "date_changed")
				+ " ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO person_attribute_type(person_attribute_type_id,name,description,format,foreign_key,searchable,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,edit_privilege,sort_weight,uuid) SELECT person_attribute_type_id,name,description,format,foreign_key,searchable,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,edit_privilege,sort_weight,uuid FROM temp_person_attribute_type";
		localInsert(insertQuery);

		// privilege
		createTempTable(getLocalDb(), "privilege");
		insertQuery = "INSERT INTO temp_privilege(privilege,description,uuid)VALUES(?,?,?)";
		selectQuery = "SELECT privilege,description,uuid FROM privilege";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO privilege(privilege,description,uuid) SELECT privilege,description,uuid FROM temp_privilege";
		localInsert(insertQuery);

		// program
		createTempTable(getLocalDb(), "program");
		insertQuery = "INSERT INTO temp_program (program_id,concept_id,outcomes_concept_id,creator,date_created,changed_by,date_changed,retired,name,description,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT program_id,concept_id,outcomes_concept_id,creator,date_created,changed_by,date_changed,retired,name,description,uuid FROM program "
				+ filter("date_created", "date_changed")
				+ " ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO program (program_id,concept_id,outcomes_concept_id,creator,date_created,changed_by,date_changed,retired,name,description,uuid) SELECT program_id,concept_id,outcomes_concept_id,creator,date_created,changed_by,date_changed,retired,name,description,uuid FROM temp_program";
		localInsert(insertQuery);

		// program_workflow
		createTempTable(getLocalDb(), "program_workflow");
		insertQuery = "INSERT INTO temp_program_workflow(program_workflow_id,program_id,concept_id,creator,date_created,retired,changed_by,date_changed,uuid)VALUES(?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT program_workflow_id,program_id,concept_id,creator,date_created,retired,changed_by,date_changed,uuid FROM program_workflow "
				+ filter("date_created", null) + " ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO program_workflow(program_workflow_id,program_id,concept_id,creator,date_created,retired,changed_by,date_changed,uuid) SELECT program_workflow_id,program_id,concept_id,creator,date_created,retired,changed_by,date_changed,uuid FROM temp_program_workflow";
		localInsert(insertQuery);

		// program_workflow_state
		createTempTable(getLocalDb(), "program_workflow_state");
		insertQuery = "INSERT INTO temp_program_workflow_state(program_workflow_state_id,program_workflow_id,concept_id,initial,terminal,creator,date_created,retired,changed_by,date_changed,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT program_workflow_state_id,program_workflow_id,concept_id,initial,terminal,creator,date_created,retired,changed_by,date_changed,uuid FROM program_workflow_state "
				+ filter("date_created", null) + " ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO program_workflow_state(program_workflow_state_id,program_workflow_id,concept_id,initial,terminal,creator,date_created,retired,changed_by,date_changed,uuid) SELECT program_workflow_state_id,program_workflow_id,concept_id,initial,terminal,creator,date_created,retired,changed_by,date_changed,uuid FROM temp_program_workflow_state";
		localInsert(insertQuery);

		// order_type
		createTempTable(getLocalDb(), "order_type");
		insertQuery = "INSERT INTO temp_order_type(order_type_id,name,description,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid,java_class_name,parent,changed_by,date_changed)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT order_type_id,name,description,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid,java_class_name,parent,changed_by,date_changed FROM order_type "
				+ filter("date_created", "date_changed")
				+ " ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO order_type(order_type_id,name,description,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid,java_class_name,parent,changed_by,date_changed) SELECT order_type_id,name,description,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid,java_class_name,parent,changed_by,date_changed FROM temp_order_type";
		localInsert(insertQuery);

		// scheduler_task_config
		createTempTable(getLocalDb(), "scheduler_task_config");
		insertQuery = "INSERT INTO temp_scheduler_task_config(task_config_id,name,description,schedulable_class,start_time,start_time_pattern,repeat_interval,start_on_startup,started,created_by,date_created,changed_by,date_changed,last_execution_time,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT task_config_id,name,description,schedulable_class,start_time,start_time_pattern,repeat_interval,start_on_startup,started,created_by,date_created,changed_by,date_changed,last_execution_time,uuid FROM scheduler_task_config "
				+ filter("date_created", "date_changed")
				+ " ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO scheduler_task_config(task_config_id,name,description,schedulable_class,start_time,start_time_pattern,repeat_interval,start_on_startup,started,created_by,date_created,changed_by,date_changed,last_execution_time,uuid) SELECT task_config_id,name,description,schedulable_class,start_time,start_time_pattern,repeat_interval,start_on_startup,started,created_by,date_created,changed_by,date_changed,last_execution_time,uuid FROM temp_scheduler_task_config";
		localInsert(insertQuery);

		// scheduler_task_config_property
		createTempTable(getLocalDb(), "scheduler_task_config_property");
		insertQuery = "INSERT INTO temp_scheduler_task_config_property(task_config_property_id,name,value,task_config_id)VALUES(?,?,?,?)";
		selectQuery = "SELECT task_config_property_id,name,value,task_config_id FROM scheduler_task_config_property";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO scheduler_task_config_property(task_config_property_id,name,value,task_config_id) SELECT task_config_property_id,name,value,task_config_id FROM temp_scheduler_task_config_property";
		localInsert(insertQuery);

		// visit_type
		createTempTable(getLocalDb(), "visit_type");
		insertQuery = "INSERT INTO temp_visit_type(visit_type_id,name,description,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT visit_type_id,name,description,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid FROM visit_type "
				+ filter("date_created", "date_changed")
				+ " ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO visit_type(visit_type_id,name,description,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid) SELECT visit_type_id,name,description,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid FROM temp_visit_type";
		localInsert(insertQuery);

		// visit_attribute_type
		createTempTable(getLocalDb(), "visit_attribute_type");
		insertQuery = "INSERT INTO temp_visit_attribute_type(visit_attribute_type_id,name,description,datatype,datatype_config,preferred_handler,handler_config,min_occurs,max_occurs,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT visit_attribute_type_id,name,description,datatype,datatype_config,preferred_handler,handler_config,min_occurs,max_occurs,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid FROM visit_attribute_type "
				+ filter("date_created", "date_changed")
				+ " ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		insertQuery = "INSERT IGNORE INTO visit_attribute_type(visit_attribute_type_id,name,description,datatype,datatype_config,preferred_handler,handler_config,min_occurs,max_occurs,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid) SELECT visit_attribute_type_id,name,description,datatype,datatype_config,preferred_handler,handler_config,min_occurs,max_occurs,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid FROM temp_visit_attribute_type";
		localInsert(insertQuery);
	}

	/**
	 * Update user-related data from temporary tables into original tables
	 * 
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 */
	private void updateUsers() throws InstantiationException,
			IllegalAccessException, ClassNotFoundException {
		for (String table : USER_TABLES) {
			String query = "INSERT IGNORE INTO " + table
					+ " SELECT * FROM temp_" + table;
			GfatmImportMain.gfatmImport.log("Executing: " + query, Level.INFO);
			localDb.runCommand(CommandType.INSERT, query);
			GfatmImportMain.gfatmImport.updateProgress(1);
		}
		String[] updateQueries = {
				"UPDATE person AS a INNER JOIN temp_person as b ON a.uuid = b.uuid SET b.person_id = a.person_id, b.gender = a.gender, b.birthdate = a.birthdate, b.birthdate_estimated = a.birthdate_estimated, b.dead = a.dead, b.death_date = a.death_date, b.cause_of_death = a.cause_of_death, b.creator = a.creator, b.date_created = a.date_created, b.changed_by = a.changed_by, b.date_changed = a.date_changed, b.voided = a.voided, b.voided_by = a.voided_by, b.date_voided = a.date_voided, b.void_reason = a.void_reason, b.deathdate_estimated = a.deathdate_estimated",
				"UPDATE person_name AS a INNER JOIN temp_person_name as b ON a.uuid = b.uuid SET b.person_name_id = a.person_name_id, b.preferred = a.preferred, b.person_id = a.person_id, b.prefix = a.prefix, b.given_name = a.given_name, b.middle_name = a.middle_name, b.family_name_prefix = a.family_name_prefix, b.family_name = a.family_name, b.family_name2 = a.family_name, b.family_name_suffix = a.family_name_suffix, b.degree = a.degree, b.creator = a.creator, b.date_created = a.date_created, b.voided = a.voided, b.voided_by = a.voided_by, b.date_voided = a.date_voided, b.void_reason = a.void_reason, b.changed_by = a.changed_by, b.date_changed = a.date_changed",
				"UPDATE privilege AS a INNER JOIN temp_privilege as b ON a.uuid = b.uuid SET a.description = b.description",
				"UPDATE provider_attribute_type AS a INNER JOIN temp_provider_attribute_type as b ON a.uuid = b.uuid SET a.name = b.name, a.description = b.description, a.datatype = b.datatype, a.datatype_config = b.datatype_config, a.preferred_handler = b.preferred_handler, a.handler_config = b.handler_config, a.min_occurs = b.min_occurs, a.max_occurs = b.max_occurs, a.creator = b.creator, a.date_created = b.date_created, a.changed_by = b.changed_by, a.date_changed = b.date_changed, a.retired = b.retired, a.retired_by = b.retired_by, a.date_retired = b.date_retired, a.retire_reason = b.retire_reason",
				"UPDATE provider AS a INNER JOIN temp_provider as b ON a.uuid = b.uuid SET a.person_id = b.person_id, a.name = b.name, a.identifier = b.identifier, a.creator = b.creator, a.date_created = b.date_created, a.changed_by = b.changed_by, a.date_changed = b.date_changed, a.retired = b.retired, a.retired_by = b.retired_by, a.date_retired = b.date_retired, a.retire_reason = b.retire_reason",
				"UPDATE provider_attribute AS a INNER JOIN temp_provider_attribute as b ON a.uuid = b.uuid SET a.provider_id = b.provider_id, a.attribute_type_id = b.attribute_type_id, a.value_reference = b.value_reference, a.creator = b.creator, a.date_created = b.date_created, a.changed_by = b.changed_by, a.date_changed = b.date_changed, a.voided = b.voided, a.voided_by = b.voided_by, a.date_voided = b.date_voided, a.void_reason = b.void_reason",
				"UPDATE role AS a INNER JOIN temp_role as b ON a.uuid = b.uuid SET a.description = b.description, a.uuid = b.uuid",
				"UPDATE users AS a INNER JOIN temp_users as b ON a.uuid = b.uuid SET a.system_id = b.system_id, a.username = b.username, a.password = b.password, a.salt = b.salt, a.secret_question = b.secret_question, a.secret_answer = b.secret_answer, a.creator = b.creator, a.date_created = b.date_created, a.changed_by = b.changed_by, a.date_changed = b.date_changed, a.person_id = b.person_id, a.retired = b.retired, a.retired_by = b.retired_by, a.date_retired = b.date_retired, a.retire_reason = b.retire_reason",
				"UPDATE user_property AS a INNER JOIN temp_user_property as b ON a.user_id = b.user_id AND a.property = b.property SET a.property_value = b.property_value"};
		for (String query : updateQueries) {
			GfatmImportMain.gfatmImport.log("Executing: " + query, Level.INFO);
			localDb.runCommand(CommandType.UPDATE, query);
			GfatmImportMain.gfatmImport.updateProgress(1);
		}
	}

	/**
	 * Update location-related data from temporary tables into original tables
	 * 
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 */
	private void updateLocations() throws InstantiationException,
			IllegalAccessException, ClassNotFoundException {
		for (String table : LOCATION_TABLES) {
			String query = "INSERT IGNORE INTO " + table
					+ " SELECT * FROM temp_" + table;
			localDb.runCommand(CommandType.INSERT, query);
		}
		String[] updateQueries = {
				"UPDATE location AS a INNER JOIN temp_location as b ON a.uuid = b.uuid SET a.name = b.name, a.description = b.description, a.address1 = b.address1, a.address2 = b.address2, a.city_village = b.city_village, a.state_province = b.state_province, a.postal_code = b.postal_code, a.country = b.country, a.latitude = b.latitude, a.longitude = b.longitude, a.creator = b.creator, a.date_created = b.date_created, a.county_district = b.county_district, a.address3 = b.address3, a.address4 = b.address4, a.address5 = b.address5, a.address6 = b.address6, a.retired = b.retired, a.retired_by = b.retired_by, a.date_retired = b.date_retired, a.retire_reason = b.retire_reason, a.parent_location = b.parent_location, a.changed_by = b.changed_by, a.date_changed = b.date_changed",
				"UPDATE location_attribute_type AS a INNER JOIN temp_location_attribute_type as b ON a.uuid = b.uuid SET a.name = b.name, a.description = b.description, a.datatype = b.datatype, a.datatype_config = b.datatype_config, a.preferred_handler = b.preferred_handler, a.handler_config = b.handler_config, a.min_occurs = b.min_occurs, a.max_occurs = b.max_occurs, a.creator = b.creator, a.date_created = b.date_created, a.changed_by = b.changed_by, a.date_changed = b.date_changed, a.retired = b.retired, a.retired_by = b.retired_by, a.date_retired = b.date_retired, a.retire_reason = b.retire_reason",
				"UPDATE location_attribute AS a INNER JOIN temp_location_attribute as b ON a.uuid = b.uuid SET a.attribute_type_id = b.attribute_type_id, a.value_reference = b.value_reference, a.creator = b.creator, a.date_created = b.date_created, a.changed_by = b.changed_by, a.date_changed = b.date_changed, a.voided = b.voided, a.voided_by = b.voided_by, a.date_voided = b.date_voided, a.void_reason = b.void_reason",
				"UPDATE location_tag AS a INNER JOIN temp_location_tag as b ON a.uuid = b.uuid SET a.name = b.name, a.description = b.description, a.creator = b.creator, a.date_created = b.date_created, a.retired = b.retired, a.retired_by = b.retired_by, a.date_retired = b.date_retired, a.retire_reason = b.retire_reason, a.changed_by = b.changed_by, a.date_changed = b.date_changed"};
		for (String query : updateQueries) {
			GfatmImportMain.gfatmImport.log("Executing: " + query, Level.INFO);
			localDb.runCommand(CommandType.UPDATE, query);
			GfatmImportMain.gfatmImport.updateProgress(1);
		}
	}

	/**
	 * Update concept-related data from temporary tables into original tables
	 * 
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 */
	private void updateConcepts() throws InstantiationException,
			IllegalAccessException, ClassNotFoundException {
		for (String table : LOCATION_TABLES) {
			String query = "INSERT IGNORE INTO " + table
					+ " SELECT * FROM temp_" + table;
			localDb.runCommand(CommandType.INSERT, query);
		}
		String[] updateQueries = {
				"UPDATE concept_class AS a INNER JOIN temp_concept_class as b ON a.uuid = b.uuid SET a.name = b.name, a.description = b.description, a.creator = b.creator, a.date_created = b.date_created, a.retired = b.retired, a.retired_by = b.retired_by, a.date_retired = b.date_retired, a.retire_reason = b.retire_reason",
				"UPDATE concept_datatype AS a INNER JOIN temp_concept_datatype as b ON a.uuid = b.uuid SET a.name = b.name, a.hl7_abbreviation = b.hl7_abbreviation, a.description = b.description, a.creator = b.creator, a.date_created = b.date_created, a.retired = b.retired, a.retired_by = b.retired_by, a.date_retired = b.date_retired, a.retire_reason = b.retire_reason",
				"UPDATE concept_map_type AS a INNER JOIN temp_concept_map_type as b ON a.uuid = b.uuid SET a.name = b.name, a.description = b.description, a.creator = b.creator, a.date_created = b.date_created, a.changed_by = b.changed_by, a.date_changed = b.date_changed, a.is_hidden = b.is_hidden, a.retired = b.retired, a.retired_by = b.retired_by, a.date_retired = b.date_retired, a.retire_reason = b.retire_reason",
				// "UPDATE concept_reference_map AS a INNER JOIN temp_concept_reference_map as b ON a.uuid = b.uuid SET a.creator = b.creator, a.date_created = b.date_created, a.concept_id = b.concept_id, a.concept_reference_term_id = b.concept_reference_term_id, a.concept_map_type_id = b.concept_map_type_id, a.changed_by = b.changed_by, a.date_changed = b.date_changed",
				// "UPDATE concept_reference_term AS a INNER JOIN concept_reference_term as b ON a.uuid = b.uuid SET a.creator = b.creator, a.date_created = b.date_created, a.concept_id = b.concept_id, a.concept_reference_term_id = b.concept_reference_term_id, a.concept_map_type_id = b.concept_map_type_id, a.changed_by = b.changed_by, a.date_changed = b.date_changed",
				"UPDATE concept_reference_source AS a INNER JOIN temp_concept_reference_source as b ON a.uuid = b.uuid SET a.name = b.name, a.description = b.description, a.hl7_code = b.hl7_code, a.creator = b.creator, a.date_created = b.date_created, a.retired = b.retired, a.retired_by = b.retired_by, a.date_retired = b.date_retired, a.retire_reason = b.retire_reason",
				"UPDATE concept_stop_word AS a INNER JOIN temp_concept_stop_word as b ON a.uuid = b.uuid SET a.word = b.word, a.locale = b.locale",
				"UPDATE concept AS a INNER JOIN temp_concept as b ON a.uuid = b.uuid SET a.retired = b.retired, a.short_name = b.short_name, a.description = b.description, a.form_text = b.form_text, a.datatype_id = b.datatype_id, a.class_id = b.class_id, a.is_set = b.is_set, a.creator = b.creator, a.date_created = b.date_created, a.version = b.version, a.changed_by = b.changed_by, a.date_changed = b.date_changed, a.retired_by = b.retired_by, a.date_retired = b.date_retired, a.retire_reason = b.retire_reason",
				"UPDATE concept_answer AS a INNER JOIN temp_concept_answer as b ON a.uuid = b.uuid SET a.concept_id = b.concept_id, a.answer_concept = b.answer_concept, a.answer_drug = b.answer_drug, a.creator = b.creator, a.date_created = b.date_created, a.sort_weight = b.sort_weight",
				"UPDATE concept_description AS a INNER JOIN temp_concept_description as b ON a.uuid = b.uuid SET a.concept_id = b.concept_id, a.description = b.description, a.locale = b.locale, a.creator = b.creator, a.date_created = b.date_created, a.changed_by = b.changed_by, a.date_changed = b.date_changed",
				"UPDATE concept_name AS a INNER JOIN temp_concept_name as b ON a.uuid = b.uuid SET a.name = b.name, a.locale = b.locale, a.creator = b.creator, a.date_created = b.date_created, a.concept_name_id = b.concept_name_id, a.voided = b.voided, a.voided_by = b.voided_by, a.date_voided = b.date_voided, a.void_reason = b.void_reason, a.concept_name_type = b.concept_name_type, a.locale_preferred = b.locale_preferred",
				"UPDATE concept_numeric AS a INNER JOIN temp_concept_numeric as b ON a.concept_id = b.concept_id SET a.hi_absolute = b.hi_absolute, a.hi_critical = b.hi_critical, a.hi_normal = b.hi_normal, a.low_absolute = b.low_absolute, a.low_critical = b.low_critical, a.low_normal = b.low_normal, a.units = b.units, a.precise = b.precise, a.display_precision = b.display_precision",
				"UPDATE concept_set AS a INNER JOIN temp_concept_set as b ON a.uuid = b.uuid SET a.concept_id = b.concept_id, a.concept_set = b.concept_set, a.sort_weight = b.sort_weight, a.creator = b.creator, a.date_created = b.date_created"};
		for (String query : updateQueries) {
			GfatmImportMain.gfatmImport.log("Executing: " + query, Level.INFO);
			localDb.runCommand(CommandType.UPDATE, query);
			GfatmImportMain.gfatmImport.updateProgress(1);
		}
	}
	
	/**
	 * Update lab module metadata from temporary tables into original tables
	 * 
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 */
	private void updateLabMetadata() throws InstantiationException,
			IllegalAccessException, ClassNotFoundException {
		for (String table : LAB_MODULE_TABLES) {
			String query = "INSERT IGNORE INTO " + table
					+ " SELECT * FROM temp_" + table;
			GfatmImportMain.gfatmImport.log("Executing: " + query, Level.INFO);
			localDb.runCommand(CommandType.INSERT, query);
		}
		String[] updateQueries = {
				"UPDATE commonlabtest_type AS a INNER JOIN temp_commonlabtest_type as b ON a.uuid = b.uuid SET a.name = b.name, a.short_name = b.short_name, a.test_group = b.test_group, a.requires_specimen = b.requires_specimen, a.reference_concept_id = b.reference_concept_id, a.description = b.description, a.creator = b.creator, a.date_created = b.date_created, a.retired = b.retired, a.retired_by = b.retired_by, a.date_retired = b.date_retired, a.retire_reason = b.retire_reason",
				"UPDATE commonlabtest_attribute_type AS a INNER JOIN temp_commonlabtest_attribute_type as b ON a.uuid = b.uuid SET a.test_type_id = b.test_type_id, a.name = b.name, a.datatype = b.datatype, a.min_occurs = b.min_occurs, a.max_occurs = b.max_occurs, a.datatype_config = b.datatype_config, a.handler_config = b.handler_config, a.sort_weight = b.sort_weight, a.description = b.description, a.creator = b.creator, a.date_created = b.date_created, a.changed_by = b.changed_by, a.date_changed = b.date_changed, a.retired = b.retired, a.retired_by = b.retired_by, a.date_retired = b.date_retired, a.retire_reason = b.retire_reason, a.preferred_handler = b.preferred_handler, a.hint = b.hint, a.group_name = b.group_name, a.multiset_name = b.multiset_name"};
		for (String query : updateQueries) {
			GfatmImportMain.gfatmImport.log("Executing: " + query, Level.INFO);
			localDb.runCommand(CommandType.UPDATE, query);
			GfatmImportMain.gfatmImport.updateProgress(1);
		}
	}

	/**
	 * Update other metadata from temporary tables into original tables
	 * 
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 */
	private void updateMetadata() throws InstantiationException,
			IllegalAccessException, ClassNotFoundException {
		for (String table : LOCATION_TABLES) {
			String query = "INSERT IGNORE INTO " + table
					+ " SELECT * FROM temp_" + table;
			GfatmImportMain.gfatmImport.log("Executing: " + query, Level.INFO);
			localDb.runCommand(CommandType.INSERT, query);
		}
		String[] updateQueries = {
				"UPDATE encounter_role AS a INNER JOIN temp_encounter_role as b ON a.uuid = b.uuid SET a.name = b.name, a.description = b.description, a.creator = b.creator, a.date_created = b.date_created, a.changed_by = b.changed_by, a.date_changed = b.date_changed, a.retired = b.retired, a.retired_by = b.retired_by, a.date_retired = b.date_retired, a.retire_reason = b.retire_reason",
				"UPDATE encounter_type AS a INNER JOIN temp_encounter_type as b ON a.uuid = b.uuid SET a.name = b.name, a.description = b.description, a.creator = b.creator, a.date_created = b.date_created, a.retired = b.retired, a.retired_by = b.retired_by, a.date_retired = b.date_retired, a.retire_reason = b.retire_reason, a.edit_privilege = b.edit_privilege, a.view_privilege = b.view_privilege",
				"UPDATE field_type AS a INNER JOIN temp_field_type as b ON a.uuid = b.uuid SET a.name = b.name, a.description = b.description, a.is_set = b.is_set, a.creator = b.creator, a.date_created = b.date_created",
				"UPDATE form_resource AS a INNER JOIN temp_form_resource as b ON a.uuid = b.uuid SET a.form_id = b.form_id, a.name = b.name, a.value_reference = b.value_reference, a.datatype = b.datatype, a.datatype_config = b.datatype_config, a.preferred_handler = b.preferred_handler, a.handler_config = b.handler_config",
				"UPDATE field AS a INNER JOIN temp_field as b ON a.uuid = b.uuid SET a.name = b.name, a.description = b.description, a.field_type = b.field_type, a.concept_id = b.concept_id, a.table_name = b.table_name, a.attribute_name = b.attribute_name, a.default_value = b.default_value, a.select_multiple = b.select_multiple, a.creator = b.creator, a.date_created = b.date_created, a.changed_by = b.changed_by, a.date_changed = b.date_changed, a.retired = b.retired, a.retired_by = b.retired_by, a.date_retired = b.date_retired, a.retire_reason = b.retire_reason",
				"UPDATE form_field AS a INNER JOIN temp_form_field as b ON a.uuid = b.uuid SET a.form_id = b.form_id, a.field_id = b.field_id, a.field_number = b.field_number, a.field_part = b.field_part, a.page_number = b.page_number, a.parent_form_field = b.parent_form_field, a.min_occurs = b.min_occurs, a.max_occurs = b.max_occurs, a.required = b.required, a.changed_by = b.changed_by, a.date_changed = b.date_changed, a.creator = b.creator, a.date_created = b.date_created, a.sort_weight = b.sort_weight",
				"UPDATE form AS a INNER JOIN temp_form as b ON a.uuid = b.uuid SET a.name = b.name, a.version = b.version, a.build = b.build, a.published = b.published, a.xslt = b.xslt, a.template = b.template, a.description = b.description, a.encounter_type = b.encounter_type, a.creator = b.creator, a.date_created = b.date_created, a.changed_by = b.changed_by, a.date_changed = b.date_changed, a.retired = b.retired, a.retired_by = b.retired_by, a.date_retired = b.date_retired, a.retired_reason = b.retired_reason",
				"UPDATE field_answer AS a INNER JOIN temp_field_answer as b ON a.uuid = b.uuid SET a.answer_id = b.answer_id, a.creator = b.creator, a.date_created = b.date_created",
				"UPDATE hl7_source AS a INNER JOIN temp_hl7_source as b ON a.uuid = b.uuid SET a.hl7_source_id = b.hl7_source_id, a.name = b.name, a.description = b.description, a.creator = b.creator, a.date_created = b.date_created",
				"UPDATE htmlformentry_html_form AS a INNER JOIN temp_htmlformentry_html_form as b ON a.uuid = b.uuid SET a.form_id = b.form_id, a.name = b.name, a.xml_data = b.xml_data, a.creator = b.creator, a.date_created = b.date_created, a.changed_by = b.changed_by, a.date_changed = b.date_changed, a.retired = b.retired, a.description = b.description, a.retired_by = b.retired_by, a.date_retired = b.date_retired, a.retire_reason = b.retire_reason",
				"UPDATE patient_identifier_type AS a INNER JOIN temp_patient_identifier_type as b ON a.uuid = b.uuid SET a.name = b.name, a.description = b.description, a.format = b.format, a.check_digit = b.check_digit, a.creator = b.creator, a.date_created = b.date_created, a.required = b.required, a.format_description = b.format_description, a.validator = b.validator, a.location_behavior = b.location_behavior, a.retired = b.retired, a.retired_by = b.retired_by, a.date_retired = b.date_retired, a.retire_reason = b.retire_reason, a.uniqueness_behavior = b.uniqueness_behavior",
				"UPDATE person_attribute_type AS a INNER JOIN temp_person_attribute_type as b ON a.uuid = b.uuid SET a.name = b.name, a.description = b.description, a.format = b.format, a.foreign_key = b.foreign_key, a.searchable = b.searchable, a.creator = b.creator, a.date_created = b.date_created, a.changed_by = b.changed_by, a.date_changed = b.date_changed, a.retired = b.retired, a.retired_by = b.retired_by, a.date_retired = b.date_retired, a.retire_reason = b.retire_reason, a.edit_privilege = b.edit_privilege, a.sort_weight = b.sort_weight",
				"UPDATE program AS a INNER JOIN temp_program as b ON a.uuid = b.uuid SET a.concept_id = b.concept_id, a.outcomes_concept_id = b.outcomes_concept_id, a.creator = b.creator, a.date_created = b.date_created, a.changed_by = b.changed_by, a.date_changed = b.date_changed, a.retired = b.retired, a.name = b.name, a.description = b.description",
				"UPDATE program_workflow AS a INNER JOIN temp_program_workflow as b ON a.uuid = b.uuid SET a.program_id = b.program_id, a.concept_id = b.concept_id, a.creator = b.creator, a.date_created = b.date_created, a.retired = b.retired, a.changed_by = b.changed_by, a.date_changed = b.date_changed",
				"UPDATE program_workflow_state AS a INNER JOIN temp_program_workflow_state as b ON a.uuid = b.uuid SET a.program_workflow_id = b.program_workflow_id, a.concept_id = b.concept_id, a.initial = b.initial, a.terminal = b.terminal, a.creator = b.creator, a.date_created = b.date_created, a.retired = b.retired, a.changed_by = b.changed_by, a.date_changed = b.date_changed",
				"UPDATE order_type AS a INNER JOIN temp_order_type as b ON a.uuid = b.uuid SET a.name = b.name, a.description = b.description, a.creator = b.creator, a.date_created = b.date_created, a.retired = b.retired, a.retired_by = b.retired_by, a.date_retired = b.date_retired, a.retire_reason = b.retire_reason, a.java_class_name = b.java_class_name, a.parent = b.parent, a.changed_by = b.changed_by, a.date_changed = b.date_changed",
				"UPDATE scheduler_task_config AS a INNER JOIN temp_scheduler_task_config as b ON a.uuid = b.uuid SET a.name = b.name, a.description = b.description, a.schedulable_class = b.schedulable_class, a.start_time = b.start_time, a.start_time_pattern = b.start_time_pattern, a.repeat_interval = b.repeat_interval, a.start_on_startup = b.start_on_startup, a.started = b.started, a.created_by = b.created_by, a.date_created = b.date_created, a.changed_by = b.changed_by, a.date_changed = b.date_changed, a.last_execution_time = b.last_execution_time",
				"UPDATE scheduler_task_config_property AS a INNER JOIN temp_scheduler_task_config_property as b ON a.task_config_property_id = b.task_config_property_id SET a.value = b.value, a.task_config_id = b.task_config_id",
				"UPDATE visit_type AS a INNER JOIN temp_visit_type as b ON a.uuid = b.uuid SET a.name = b.name, a.description = b.description, a.creator = b.creator, a.date_created = b.date_created, a.changed_by = b.changed_by, a.date_changed = b.date_changed, a.retired = b.retired, a.retired_by = b.retired_by, a.date_retired = b.date_retired, a.retire_reason = b.retire_reason",
				"UPDATE visit_attribute_type AS a INNER JOIN temp_visit_attribute_type as b ON a.uuid = b.uuid SET a.name = b.name, a.description = b.description, a.datatype = b.datatype, a.datatype_config = b.datatype_config, a.preferred_handler = b.preferred_handler, a.handler_config = b.handler_config, a.min_occurs = b.min_occurs, a.max_occurs = b.max_occurs, a.creator = b.creator, a.date_created = b.date_created, a.changed_by = b.changed_by, a.date_changed = b.date_changed, a.retired = b.retired, a.retired_by = b.retired_by, a.date_retired = b.date_retired, a.retire_reason = b.retire_reason"};
		for (String query : updateQueries) {
			GfatmImportMain.gfatmImport.log("Executing: " + query, Level.INFO);
			localDb.runCommand(CommandType.UPDATE, query);
			GfatmImportMain.gfatmImport.updateProgress(1);
		}
	}

	public void createTempTable(DatabaseUtil db, String sourceTable) {
		try {
			String query = "TRUNCATE TABLE temp_" + sourceTable;
			db.runCommand(CommandType.DROP, query);
			GfatmImportMain.gfatmImport.log("Executing: " + query, Level.INFO);
			// query = "CREATE TABLE temp_" + sourceTable + " LIKE " +
			// sourceTable;
			// db.runCommand(CommandType.CREATE, query);
			// GfatmImportMain.gfatmImport.log("Executing: " + query,
			// Level.INFO);
		} catch (Exception e) {
			e.printStackTrace();
			GfatmImportMain.gfatmImport.log("Exception while executing query: "
					+ e.getMessage(), Level.SEVERE);
		}
	}

	/**
	 * Fetch data from remote database and insert in local database
	 * 
	 * @param selectQuery
	 * @param insertQuery
	 * @return
	 * @throws SQLException
	 */
	public void remoteSelectInsert(String selectQuery, String insertQuery)
			throws SQLException {
		GfatmImportMain.gfatmImport.log("Executing: " + insertQuery + " "
				+ selectQuery, Level.INFO);
		Connection remoteConnection = DriverManager.getConnection(getRemoteDb()
				.getUrl(), getRemoteDb().getUsername(), getRemoteDb()
				.getPassword());
		ResultSet data = remoteConnection.createStatement().executeQuery(
				selectQuery);
		ResultSetMetaData metaData = data.getMetaData();
		Connection localConnection = DriverManager.getConnection(getLocalDb()
				.getUrl(), getLocalDb().getUsername(), getLocalDb()
				.getPassword());
		while (data.next()) {
			PreparedStatement target = localConnection
					.prepareStatement(insertQuery);
			for (int i = 1; i <= metaData.getColumnCount(); i++) {
				target.setString(i, data.getString(i));
			}
			target.executeUpdate();
		}
		// For Progress Bar
		GfatmImportMain.gfatmImport.updateProgress(1);
	}

	/**
	 * Execute insert query on local database
	 * 
	 * @param insertQuery
	 */
	public void localInsert(String insertQuery) {
		GfatmImportMain.gfatmImport
				.log("Executing: " + insertQuery, Level.INFO);
		getLocalDb().runCommand(CommandType.INSERT, insertQuery);
	}

	public DatabaseUtil getLocalDb() {
		return localDb;
	}

	public void setLocalDb(DatabaseUtil localDb) {
		this.localDb = localDb;
	}

	public DatabaseUtil getRemoteDb() {
		return remoteDb;
	}

	public void setRemoteDb(DatabaseUtil remoteDb) {
		this.remoteDb = remoteDb;
	}

	public void setImportUsers(boolean importUsers) {
		this.importUsers = importUsers;
	}

	public void setImportLocations(boolean importLocations) {
		this.importLocations = importLocations;
	}

	public void setImportConcepts(boolean importConcepts) {
		this.importConcepts = importConcepts;
	}
	
	public void setImportLabData(boolean importLabData) {
		this.importLabData = importLabData;
	}

	public void setImportOtherMetadata(boolean importOtherMetadata) {
		this.importOtherMetadata = importOtherMetadata;
	}

	public void setFilterDate(boolean filterDate) {
		this.filterDate = filterDate;
	}

	public Date getDateFrom() {
		return dateFrom;
	}

	public void setDateFrom(Date dateFrom) {
		this.dateFrom = dateFrom;
	}

	public Date getDateTo() {
		return dateTo;
	}

	public void setDateTo(Date dateTo) {
		this.dateTo = dateTo;
	}
}
