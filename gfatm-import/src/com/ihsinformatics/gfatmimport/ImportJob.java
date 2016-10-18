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
import java.util.logging.Level;

import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.ihsinformatics.util.CommandType;
import com.ihsinformatics.util.DatabaseUtil;

/**
 * @author owais.hussain@ihsinformatics.com
 * 
 */
public class ImportJob implements Job {

	private DatabaseUtil localDb;
	private DatabaseUtil remoteDb;

	private boolean importUsers = false;
	private boolean importLocations = false;
	private boolean importConcepts = false;
	private boolean importOtherMetadata;
	private int progressRange = 0;

	public ImportJob() {
	}

	public void initialize(ImportJob importJob) {
		localDb = importJob.localDb;
		remoteDb = importJob.remoteDb;
		importUsers = importJob.importUsers;
		importLocations = importJob.importLocations;
		importConcepts = importJob.importConcepts;
		importOtherMetadata = importJob.importOtherMetadata;
		progressRange = importJob.progressRange;
		// Maximum progress is the number of tables * 2 (insert and update)
		progressRange = (importUsers ? 16 : 0) + (importLocations ? 10 : 0)
				+ (importConcepts ? 26 : 0) + (importOtherMetadata ? 18 : 0);
		GfatmImportMain.gfatmImport.resetProgressBar(0, progressRange);
	}

	public void execute(JobExecutionContext context)
			throws JobExecutionException {
		GfatmImportMain.gfatmImport.setMode(ImportStatus.IMPORTING);
		JobDataMap dataMap = context.getMergedJobDataMap();
		ImportJob importJob = (ImportJob) dataMap.get("importJob");
		initialize(importJob);
		if (importUsers) {
			try {
				importUsers();
			} catch (SQLException e) {
				GfatmImportMain.gfatmImport.log("User data import incomplete. "
						+ e.getMessage(), Level.WARNING);
			}
		}
		if (importLocations) {
			try {
				importLocations();
			} catch (SQLException e) {
				GfatmImportMain.gfatmImport.log(
						"Location data import incomplete. " + e.getMessage(),
						Level.WARNING);
			}
		}
		if (importConcepts) {
			try {
				importConcepts();
			} catch (SQLException e) {
				GfatmImportMain.gfatmImport.log(
						"Concept data import incomplete. " + e.getMessage(),
						Level.WARNING);
			}
		}
		if (importOtherMetadata) {
			try {
				importMetadata();
			} catch (SQLException e) {
				GfatmImportMain.gfatmImport.log("Metadata import incomplete. "
						+ e.getMessage(), Level.WARNING);
			}
		}
		GfatmImportMain.gfatmImport.setMode(ImportStatus.WAITING);
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
		
		// privilege
		createTempTable(localDb, "privilege");
		insertQuery = "INSERT IGNORE INTO temp_privilege(privilege,description,uuid)VALUES(?,?,?)";
		selectQuery = "SELECT privilege,description,uuid FROM privilege";
		remoteSelectInsert(selectQuery, insertQuery);

		// provider
		createTempTable(localDb, "provider");
		insertQuery = "INSERT IGNORE INTO temp_provider(provider_id,person_id,name,identifier,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid,provider_role_id)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT provider_id,person_id,name,identifier,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid,provider_role_id FROM provider ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		
		// role
		createTempTable(localDb, "role");
		insertQuery = "INSERT IGNORE INTO temp_role(role,description,uuid)VALUES(?,?,?)";
		selectQuery = "SELECT role,description,uuid FROM role";
		remoteSelectInsert(selectQuery, insertQuery);
		
		// role_privilege
		createTempTable(localDb, "role_privilege");
		insertQuery = "INSERT IGNORE INTO temp_role_privilege(role,privilege)VALUES(?,?)";
		selectQuery = "SELECT role,privilege FROM role_privilege";
		remoteSelectInsert(selectQuery, insertQuery);

		// role_role
		createTempTable(localDb, "role_role");
		insertQuery = "INSERT IGNORE INTO temp_role_role(parent_role,child_role)VALUES(?,?)";
		selectQuery = "SELECT parent_role,child_role FROM role_role";
		remoteSelectInsert(selectQuery, insertQuery);
		
		// users
		createTempTable(localDb, "users");
		insertQuery = "INSERT IGNORE INTO temp_users(user_id,system_id,username,password,salt,secret_question,secret_answer,creator,date_created,changed_by,date_changed,person_id,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT user_id,system_id,username,password,salt,secret_question,secret_answer,creator,date_created,changed_by,date_changed,person_id,retired,retired_by,date_retired,retire_reason,uuid FROM users ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		
		// user_property
		createTempTable(localDb, "user_property");
		insertQuery = "INSERT IGNORE INTO temp_user_property(user_id,property,property_value)VALUES(?,?,?)";
		selectQuery = "SELECT user_id,property,property_value FROM user_property";
		remoteSelectInsert(selectQuery, insertQuery);
		
		// user_role
		createTempTable(localDb, "user_role");
		insertQuery = "INSERT IGNORE INTO temp_user_role(user_id,role)VALUES(?,?)";
		selectQuery = "SELECT user_id,role FROM user_role";
		remoteSelectInsert(selectQuery, insertQuery);
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
		createTempTable(localDb, "location");
		insertQuery = "INSERT IGNORE INTO temp_location(location_id,name,description,address1,address2,city_village,state_province,postal_code,country,latitude,longitude,creator,date_created,county_district,address3,address4,address5,address6,retired,retired_by,date_retired,retire_reason,parent_location,uuid,changed_by,date_changed)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT location.location_id,location.name,location.description,location.address1,location.address2,location.city_village,location.state_province,location.postal_code,location.country,location.latitude,location.longitude,location.creator,location.date_created,location.county_district,location.address3,location.address4,location.address5,location.address6,location.retired,location.retired_by,location.date_retired,location.retire_reason,location.parent_location,location.uuid,location.changed_by,location.date_changed FROM location ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);

		// location_attribute_type
		createTempTable(localDb, "location_attribute_type");
		insertQuery = "INSERT IGNORE INTO temp_location_attribute_type(location_attribute_type_id,name,description,datatype,datatype_config,preferred_handler,handler_config,min_occurs,max_occurs,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT location_attribute_type.location_attribute_type_id,location_attribute_type.name,location_attribute_type.description,location_attribute_type.datatype,location_attribute_type.datatype_config,location_attribute_type.preferred_handler,location_attribute_type.handler_config,location_attribute_type.min_occurs,location_attribute_type.max_occurs,location_attribute_type.creator,location_attribute_type.date_created,location_attribute_type.changed_by,location_attribute_type.date_changed,location_attribute_type.retired,location_attribute_type.retired_by,location_attribute_type.date_retired,location_attribute_type.retire_reason,location_attribute_type.uuid FROM location_attribute_type ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		
		// location_attribute
		createTempTable(localDb, "location_attribute");
		insertQuery = "INSERT IGNORE INTO temp_location_attribute(location_attribute_id,location_id,attribute_type_id,value_reference,uuid,creator,date_created,changed_by,date_changed,voided,voided_by,date_voided,void_reason)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT location_attribute.location_attribute_id,location_attribute.location_id,location_attribute.attribute_type_id,location_attribute.value_reference,location_attribute.uuid,location_attribute.creator,location_attribute.date_created,location_attribute.changed_by,location_attribute.date_changed,location_attribute.voided,location_attribute.voided_by,location_attribute.date_voided,location_attribute.void_reason FROM location_attribute ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);

		// location_tag
		createTempTable(localDb, "location_tag");
		insertQuery = "INSERT IGNORE INTO temp_location_tag(location_tag_id,name,description,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid,changed_by,date_changed)VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT location_tag.location_tag_id,location_tag.name,location_tag.description,location_tag.creator,location_tag.date_created,location_tag.retired,location_tag.retired_by,location_tag.date_retired,location_tag.retire_reason,location_tag.uuid,location_tag.changed_by,location_tag.date_changed FROM location_tag ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);

		// location_tag_map
		createTempTable(localDb, "location_tag_map");
		insertQuery = "INSERT IGNORE INTO temp_location_tag_map(location_id,location_tag_id)VALUES(?,?)";
		selectQuery = "SELECT location_tag_map.location_id,location_tag_map.location_tag_id FROM location_tag_map";
		remoteSelectInsert(selectQuery, insertQuery);
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

		// concept
		insertQuery = "INSERT IGNORE INTO openmrs.concept(concept_id,retired,short_name,description,form_text,datatype_id,class_id,is_set,creator,date_created,version,changed_by,date_changed,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT concept.concept_id,concept.retired,concept.short_name,concept.description,concept.form_text,concept.datatype_id,concept.class_id,concept.is_set,concept.creator,concept.date_created,concept.version,concept.changed_by,concept.date_changed,concept.retired_by,concept.date_retired,concept.retire_reason,concept.uuid FROM openmrs.concept ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		
		// concept_answer
		insertQuery = "INSERT IGNORE INTO openmrs.concept_answer(concept_answer_id,concept_id,answer_concept,answer_drug,creator,date_created,uuid,sort_weight)VALUES(?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT concept_answer.concept_answer_id,concept_answer.concept_id,concept_answer.answer_concept,concept_answer.answer_drug,concept_answer.creator,concept_answer.date_created,concept_answer.uuid,concept_answer.sort_weight FROM openmrs.concept_answer ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		
		// concept_class
		insertQuery = "INSERT IGNORE INTO openmrs.concept_class(concept_class_id,name,description,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT concept_class.concept_class_id,concept_class.name,concept_class.description,concept_class.creator,concept_class.date_created,concept_class.retired,concept_class.retired_by,concept_class.date_retired,concept_class.retire_reason,concept_class.uuid FROM openmrs.concept_class ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		
		// concept_datatype
		insertQuery = "INSERT IGNORE INTO openmrs.concept_datatype(concept_datatype_id,name,hl7_abbreviation,description,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT concept_datatype.concept_datatype_id,concept_datatype.name,concept_datatype.hl7_abbreviation,concept_datatype.description,concept_datatype.creator,concept_datatype.date_created,concept_datatype.retired,concept_datatype.retired_by,concept_datatype.date_retired,concept_datatype.retire_reason,concept_datatype.uuid FROM openmrs.concept_datatype ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		
		// concept_description
		insertQuery = "INSERT IGNORE INTO openmrs.concept_description(concept_description_id,concept_id,description,locale,creator,date_created,changed_by,date_changed,uuid)VALUES(?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT concept_description.concept_description_id,concept_description.concept_id,concept_description.description,concept_description.locale,concept_description.creator,concept_description.date_created,concept_description.changed_by,concept_description.date_changed,concept_description.uuid FROM openmrs.concept_description ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		
		// concept_map_type
		insertQuery = "INSERT IGNORE INTO openmrs.concept_map_type(concept_map_type_id,name,description,creator,date_created,changed_by,date_changed,is_hidden,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT concept_map_type.concept_map_type_id,concept_map_type.name,concept_map_type.description,concept_map_type.creator,concept_map_type.date_created,concept_map_type.changed_by,concept_map_type.date_changed,concept_map_type.is_hidden,concept_map_type.retired,concept_map_type.retired_by,concept_map_type.date_retired,concept_map_type.retire_reason,concept_map_type.uuid FROM openmrs.concept_map_type ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		
		// concept_name
		insertQuery = "INSERT IGNORE INTO openmrs.concept_name(concept_id,name,locale,creator,date_created,concept_name_id,voided,voided_by,date_voided,void_reason,uuid,concept_name_type,locale_preferred)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT concept_name.concept_id,concept_name.name,concept_name.locale,concept_name.creator,concept_name.date_created,concept_name.concept_name_id,concept_name.voided,concept_name.voided_by,concept_name.date_voided,concept_name.void_reason,concept_name.uuid,concept_name.concept_name_type,concept_name.locale_preferred FROM openmrs.concept_name ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		
		// concept_numeric
		insertQuery = "INSERT IGNORE INTO openmrs.concept_numeric(concept_id,hi_absolute,hi_critical,hi_normal,low_absolute,low_critical,low_normal,units,precise,display_precision)VALUES(?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT concept_numeric.concept_id,concept_numeric.hi_absolute,concept_numeric.hi_critical,concept_numeric.hi_normal,concept_numeric.low_absolute,concept_numeric.low_critical,concept_numeric.low_normal,concept_numeric.units,concept_numeric.precise,concept_numeric.display_precision FROM openmrs.concept_numeric";
		remoteSelectInsert(selectQuery, insertQuery);
		
		// concept_reference_map
		insertQuery = "INSERT IGNORE INTO openmrs.concept_reference_map(concept_map_id,creator,date_created,concept_id,uuid,concept_reference_term_id,concept_map_type_id,changed_by,date_changed)VALUES(?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT concept_reference_map.concept_map_id,concept_reference_map.creator,concept_reference_map.date_created,concept_reference_map.concept_id,concept_reference_map.uuid,concept_reference_map.concept_reference_term_id,concept_reference_map.concept_map_type_id,concept_reference_map.changed_by,concept_reference_map.date_changed FROM openmrs.concept_reference_map ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		
		// concept_reference_source
		insertQuery = "INSERT IGNORE INTO openmrs.concept_reference_source(concept_source_id,name,description,hl7_code,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT concept_reference_source.concept_source_id,concept_reference_source.name,concept_reference_source.description,concept_reference_source.hl7_code,concept_reference_source.creator,concept_reference_source.date_created,concept_reference_source.retired,concept_reference_source.retired_by,concept_reference_source.date_retired,concept_reference_source.retire_reason,concept_reference_source.uuid FROM openmrs.concept_reference_source ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		
		// concept_reference_map
		insertQuery = "INSERT IGNORE INTO openmrs.concept_reference_term(concept_reference_term_id,concept_source_id,name,code,version,description,creator,date_created,date_changed,changed_by,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT concept_reference_term.concept_reference_term_id,concept_reference_term.concept_source_id,concept_reference_term.name,concept_reference_term.code,concept_reference_term.version,concept_reference_term.description,concept_reference_term.creator,concept_reference_term.date_created,concept_reference_term.date_changed,concept_reference_term.changed_by,concept_reference_term.retired,concept_reference_term.retired_by,concept_reference_term.date_retired,concept_reference_term.retire_reason,concept_reference_term.uuid FROM openmrs.concept_reference_term ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		
		// concept_set
		insertQuery = "INSERT IGNORE INTO openmrs.concept_set(concept_set_id,concept_id,concept_set,sort_weight,creator,date_created,uuid)VALUES(?,?,?,?,?,?,?)";
		selectQuery = "SELECT concept_set.concept_set_id,concept_set.concept_id,concept_set.concept_set,concept_set.sort_weight,concept_set.creator,concept_set.date_created,concept_set.uuid FROM openmrs.concept_set ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		
		// concept_stop_word
		insertQuery = "INSERT IGNORE INTO openmrs.concept_stop_word(concept_stop_word_id,word,locale,uuid)VALUES(?,?,?,?)";
		selectQuery = "SELECT concept_stop_word.concept_stop_word_id,concept_stop_word.word,concept_stop_word.locale,concept_stop_word.uuid FROM openmrs.concept_stop_word";
		remoteSelectInsert(selectQuery, insertQuery);
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
		// active_list_type
		insertQuery = "INSERT IGNORE INTO openmrs.active_list_type(active_list_type_id,name,description,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT active_list_type_id,name,description,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid FROM openmrs.active_list_type ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		// encounter_role
		insertQuery = "INSERT IGNORE INTO openmrs.encounter_role(encounter_role_id,name,description,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT encounter_role_id,name,description,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid FROM openmrs.encounter_role ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		// encounter_type
		insertQuery = "INSERT IGNORE INTO openmrs.encounter_type(encounter_type_id,name,description,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid,edit_privilege,view_privilege)VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT encounter_type_id,name,description,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid,edit_privilege,view_privilege FROM openmrs.encounter_type ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		// field_type
		insertQuery = "INSERT IGNORE INTO openmrs.field_type(field_type_id,name,description,is_set,creator,date_created,uuid)VALUES(?,?,?,?,?,?,?)";
		selectQuery = "SELECT field_type_id,name,description,is_set,creator,date_created,uuid FROM openmrs.field_type ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		// hl7_source
		insertQuery = "INSERT IGNORE INTO openmrs.hl7_source(hl7_source_id,name,description,creator,date_created,uuid)VALUES(?,?,?,?,?,?)";
		selectQuery = "SELECT hl7_source_id,name,description,creator,date_created,uuid FROM openmrs.hl7_source ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		// htmlformentry_html_form
		insertQuery = "INSERT IGNORE INTO openmrs.htmlformentry_html_form(id,form_id,name,xml_data,creator,date_created,changed_by,date_changed,retired,uuid,description,retired_by,date_retired,retire_reason)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT id,form_id,name,xml_data,creator,date_created,changed_by,date_changed,retired,uuid,description,retired_by,date_retired,retire_reason FROM openmrs.htmlformentry_html_form ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		// patient_identifier_type
		insertQuery = "INSERT INTO openmrs.patient_identifier_type(patient_identifier_type_id,name,description,format,check_digit,creator,date_created,required,format_description,validator,location_behavior,retired,retired_by,date_retired,retire_reason,uuid,uniqueness_behavior)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT patient_identifier_type_id,name,description,format,check_digit,creator,date_created,required,format_description,validator,location_behavior,retired,retired_by,date_retired,retire_reason,uuid,uniqueness_behavior FROM openmrs.patient_identifier_type ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		// person_attribute_type
		insertQuery = "INSERT INTO openmrs.person_attribute_type(person_attribute_type_id,name,description,format,foreign_key,searchable,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,edit_privilege,sort_weight,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT person_attribute_type_id,name,description,format,foreign_key,searchable,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,edit_privilege,sort_weight,uuid FROM openmrs.person_attribute_type ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		// order_type
		insertQuery = "INSERT IGNORE INTO openmrs.order_type(order_type_id,name,description,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid,java_class_name,parent,changed_by,date_changed)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT order_type_id,name,description,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid,java_class_name,parent,changed_by,date_changed FROM openmrs.order_type ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		// scheduler_task_config
		insertQuery = "INSERT IGNORE INTO openmrs.scheduler_task_config(task_config_id,name,description,schedulable_class,start_time,start_time_pattern,repeat_interval,start_on_startup,started,created_by,date_created,changed_by,date_changed,last_execution_time,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT task_config_id,name,description,schedulable_class,start_time,start_time_pattern,repeat_interval,start_on_startup,started,created_by,date_created,changed_by,date_changed,last_execution_time,uuid FROM openmrs.scheduler_task_config ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
		// visit_type
		insertQuery = "INSERT IGNORE INTO openmrs.visit_type(visit_type_id,name,description,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT visit_type_id,name,description,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid FROM openmrs.visit_type ORDER BY date_created";
		remoteSelectInsert(selectQuery, insertQuery);
	}

	public void createTempTable(DatabaseUtil db, String sourceTable) {
		try {
			db.runCommand(CommandType.DROP, "DROP TABLE IF EXISTS temp_"
					+ sourceTable);
			db.runCommand(CommandType.CREATE, "CREATE TABLE temp_"
					+ sourceTable + " LIKE " + sourceTable);
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
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
		Connection remoteConnection = remoteDb.getConnection();
		ResultSet data = remoteConnection.createStatement().executeQuery(
				selectQuery);
		ResultSetMetaData metaData = data.getMetaData();
		Connection localConnection = DriverManager.getConnection(
				localDb.getUrl(), localDb.getUsername(), localDb.getPassword());
		while (data.next()) {
			PreparedStatement target = localConnection
					.prepareStatement(insertQuery);
			for (int i = 1; i <= metaData.getColumnCount(); i++) {
				target.setString(i, data.getString(i));
			}
			target.executeUpdate();
		}
		GfatmImportMain.gfatmImport.updateProgress();
	}

	/**
	 * @return the localDb
	 */
	public DatabaseUtil getLocalDb() {
		return localDb;
	}

	/**
	 * @param localDb
	 *            the localDb to set
	 */
	public void setLocalDb(DatabaseUtil localDb) {
		this.localDb = localDb;
	}

	/**
	 * @return the remoteDb
	 */
	public DatabaseUtil getRemoteDb() {
		return remoteDb;
	}

	/**
	 * @param remoteDb
	 *            the remoteDb to set
	 */
	public void setRemoteDb(DatabaseUtil remoteDb) {
		this.remoteDb = remoteDb;
	}

	/**
	 * @return the importUsers
	 */
	public boolean isImportUsers() {
		return importUsers;
	}

	/**
	 * @param importUsers
	 *            the importUsers to set
	 */
	public void setImportUsers(boolean importUsers) {
		this.importUsers = importUsers;
	}

	/**
	 * @return the importLocations
	 */
	public boolean isImportLocations() {
		return importLocations;
	}

	/**
	 * @param importLocations
	 *            the importLocations to set
	 */
	public void setImportLocations(boolean importLocations) {
		this.importLocations = importLocations;
	}

	/**
	 * @return the importConcepts
	 */
	public boolean isImportConcepts() {
		return importConcepts;
	}

	/**
	 * @param importConcepts
	 *            the importConcepts to set
	 */
	public void setImportConcepts(boolean importConcepts) {
		this.importConcepts = importConcepts;
	}

	/**
	 * @return the importOtherMetadata
	 */
	public boolean isImportOtherMetadata() {
		return importOtherMetadata;
	}

	/**
	 * @param importOtherMetadata
	 *            the importOtherMetadata to set
	 */
	public void setImportOtherMetadata(boolean importOtherMetadata) {
		this.importOtherMetadata = importOtherMetadata;
	}

	/**
	 * @return the progressRange
	 */
	public int getProgressRange() {
		return progressRange;
	}

	/**
	 * @param progressRange
	 *            the progressRange to set
	 */
	public void setProgressRange(int progressRange) {
		this.progressRange = progressRange;
	}
}