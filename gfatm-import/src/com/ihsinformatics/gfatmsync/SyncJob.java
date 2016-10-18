package com.ihsinformatics.gfatmsync;

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

public class SyncJob implements Job {

	private DatabaseUtil localDb;
	private DatabaseUtil remoteDb;

	private boolean syncUsers = false;
	private boolean syncLocations = false;
	private boolean syncConcepts = false;
	private boolean syncOtherMetadata;
	private int progressRange = 0;

	public SyncJob() {
	}
	
	public void initialize(SyncJob syncJob) {
		localDb = syncJob.localDb;
		remoteDb = syncJob.remoteDb;
		syncUsers = syncJob.syncUsers;
		syncLocations = syncJob.syncLocations;
		syncConcepts = syncJob.syncConcepts;
		syncOtherMetadata = syncJob.syncOtherMetadata;
		progressRange = syncJob.progressRange;
		// Maximum progress is the number of tables * 2 (insert and update)
		progressRange = (syncUsers ? 16 : 0) + (syncLocations ? 10 : 0)
				+ (syncConcepts ? 26 : 0) + (syncOtherMetadata ? 18 : 0);
		GfatmSyncUi.resetProgressBar(0, progressRange);
	}

	public void execute(JobExecutionContext context)
			throws JobExecutionException {
		JobDataMap dataMap = context.getMergedJobDataMap();
		SyncJob syncJob = (SyncJob) dataMap.get("syncJob");
		initialize(syncJob);
		// First, disable foreign key checks on local database
		try {
			localDb.runCommand(CommandType.SET, "SET FOREIGN_KEY_CHECKS=0");
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (syncUsers) {
			try {
				importUsers();
			} catch (SQLException e) {
				GfatmSyncUi.log(
						"User data import incomplete. " + e.getMessage(),
						Level.WARNING);
			}
		}
		if (syncLocations) {
			try {
				importLocations();
			} catch (SQLException e) {
				GfatmSyncUi.log(
						"Location data import incomplete. " + e.getMessage(),
						Level.WARNING);
			}
		}
		if (syncConcepts) {
			try {
				importConcepts();
			} catch (SQLException e) {
				GfatmSyncUi.log(
						"Concept data import incomplete. " + e.getMessage(),
						Level.WARNING);
			}
		}
		if (syncOtherMetadata) {
			try {
				importMetadata();
			} catch (SQLException e) {
				GfatmSyncUi.log(
						"Metadata import incomplete. " + e.getMessage(),
						Level.WARNING);
			}
		}
		// Enable foreign key checks on local database
		try {
			localDb.runCommand(CommandType.SET, "SET FOREIGN_KEY_CHECKS=1");
		} catch (Exception e) {
			GfatmSyncUi.log(
					"Cannot re-enable Foreign key constraints. "
							+ e.getMessage(), Level.SEVERE);
		}
	}

	/**
	 * Import data from user-related tables, including user rights management
	 * tables
	 * 
	 * @throws SQLException
	 */
	public void importUsers() throws SQLException {
		GfatmSyncUi.log("Importing users from remote source", Level.INFO);
		String selectQuery = "";
		String insertQuery = "";
		// privilege
		insertQuery = "INSERT IGNORE INTO openmrs.privilege(privilege,description,uuid)VALUES(?,?,?)";
		selectQuery = "SELECT privilege.privilege,privilege.description,privilege.uuid FROM openmrs.privilege";
		remoteSelectInsert(selectQuery, insertQuery);
		// provider
		insertQuery = "INSERT IGNORE INTO openmrs.provider(provider_id,person_id,name,identifier,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid,provider_role_id)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT provider.provider_id,provider.person_id,provider.name,provider.identifier,provider.creator,provider.date_created,provider.changed_by,provider.date_changed,provider.retired,provider.retired_by,provider.date_retired,provider.retire_reason,provider.uuid,provider.provider_role_id FROM openmrs.provider";
		remoteSelectInsert(selectQuery, insertQuery);
		// role
		insertQuery = "INSERT IGNORE INTO openmrs.role(role,description,uuid)VALUES(?,?,?)";
		selectQuery = "SELECT role.role,role.description,role.uuid FROM openmrs.role";
		remoteSelectInsert(selectQuery, insertQuery);
		// role_privilege
		insertQuery = "INSERT IGNORE INTO openmrs.role_privilege(role,privilege)VALUES(?,?)";
		selectQuery = "SELECT role_privilege.role,role_privilege.privilege FROM openmrs.role_privilege";
		remoteSelectInsert(selectQuery, insertQuery);
		// role_role
		insertQuery = "INSERT IGNORE INTO openmrs.role_role(parent_role,child_role)VALUES(?,?)";
		selectQuery = "SELECT role_role.parent_role,role_role.child_role FROM openmrs.role_role";
		remoteSelectInsert(selectQuery, insertQuery);
		// users
		insertQuery = "INSERT IGNORE INTO openmrs.users(user_id,system_id,username,password,salt,secret_question,secret_answer,creator,date_created,changed_by,date_changed,person_id,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT users.user_id,users.system_id,users.username,users.password,users.salt,users.secret_question,users.secret_answer,users.creator,users.date_created,users.changed_by,users.date_changed,users.person_id,users.retired,users.retired_by,users.date_retired,users.retire_reason,users.uuid FROM openmrs.users";
		remoteSelectInsert(selectQuery, insertQuery);
		// user_property
		insertQuery = "INSERT IGNORE INTO openmrs.user_property(user_id,property,property_value)VALUES(?,?,?)";
		selectQuery = "SELECT user_property.user_id,user_property.property,user_property.property_value FROM openmrs.user_property";
		remoteSelectInsert(selectQuery, insertQuery);
		// user_role
		insertQuery = "INSERT IGNORE INTO openmrs.user_role(user_id,role)VALUES(?,?)";
		selectQuery = "SELECT user_role.user_id,user_role.role FROM openmrs.user_role";
		remoteSelectInsert(selectQuery, insertQuery);
	}

	/**
	 * Import data from location-related tables
	 * 
	 * @throws SQLException
	 */
	public void importLocations() throws SQLException {
		GfatmSyncUi.log("Importing locations from remote source", Level.INFO);
		String selectQuery = "";
		String insertQuery = "";
		// location
		insertQuery = "INSERT IGNORE INTO openmrs.location(location_id,name,description,address1,address2,city_village,state_province,postal_code,country,latitude,longitude,creator,date_created,county_district,address3,address4,address5,address6,retired,retired_by,date_retired,retire_reason,parent_location,uuid,changed_by,date_changed)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT location.location_id,location.name,location.description,location.address1,location.address2,location.city_village,location.state_province,location.postal_code,location.country,location.latitude,location.longitude,location.creator,location.date_created,location.county_district,location.address3,location.address4,location.address5,location.address6,location.retired,location.retired_by,location.date_retired,location.retire_reason,location.parent_location,location.uuid,location.changed_by,location.date_changed FROM openmrs.location";
		remoteSelectInsert(selectQuery, insertQuery);
		// location_attribute_type
		insertQuery = "INSERT IGNORE INTO openmrs.location_attribute_type(location_attribute_type_id,name,description,datatype,datatype_config,preferred_handler,handler_config,min_occurs,max_occurs,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT location_attribute_type.location_attribute_type_id,location_attribute_type.name,location_attribute_type.description,location_attribute_type.datatype,location_attribute_type.datatype_config,location_attribute_type.preferred_handler,location_attribute_type.handler_config,location_attribute_type.min_occurs,location_attribute_type.max_occurs,location_attribute_type.creator,location_attribute_type.date_created,location_attribute_type.changed_by,location_attribute_type.date_changed,location_attribute_type.retired,location_attribute_type.retired_by,location_attribute_type.date_retired,location_attribute_type.retire_reason,location_attribute_type.uuid FROM openmrs.location_attribute_type";
		remoteSelectInsert(selectQuery, insertQuery);
		// location_attribute
		insertQuery = "INSERT IGNORE INTO openmrs.location_attribute(location_attribute_id,location_id,attribute_type_id,value_reference,uuid,creator,date_created,changed_by,date_changed,voided,voided_by,date_voided,void_reason)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT location_attribute.location_attribute_id,location_attribute.location_id,location_attribute.attribute_type_id,location_attribute.value_reference,location_attribute.uuid,location_attribute.creator,location_attribute.date_created,location_attribute.changed_by,location_attribute.date_changed,location_attribute.voided,location_attribute.voided_by,location_attribute.date_voided,location_attribute.void_reason FROM openmrs.location_attribute";
		remoteSelectInsert(selectQuery, insertQuery);
		// location_tag
		insertQuery = "INSERT IGNORE INTO openmrs.location_tag(location_tag_id,name,description,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid,changed_by,date_changed)VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT location_tag.location_tag_id,location_tag.name,location_tag.description,location_tag.creator,location_tag.date_created,location_tag.retired,location_tag.retired_by,location_tag.date_retired,location_tag.retire_reason,location_tag.uuid,location_tag.changed_by,location_tag.date_changed FROM openmrs.location_tag";
		remoteSelectInsert(selectQuery, insertQuery);
		// location_tag_map
		insertQuery = "INSERT IGNORE INTO openmrs.location_tag_map(location_id,location_tag_id)VALUES(?,?)";
		selectQuery = "SELECT location_tag_map.location_id,location_tag_map.location_tag_id FROM openmrs.location_tag_map";
		remoteSelectInsert(selectQuery, insertQuery);
	}

	/**
	 * Import data from concept tables
	 * 
	 * @throws SQLException
	 */
	public void importConcepts() throws SQLException {
		GfatmSyncUi.log("Importing metadata from remote source", Level.INFO);
		String selectQuery = "";
		String insertQuery = "";
		// concept
		insertQuery = "INSERT IGNORE INTO openmrs.concept(concept_id,retired,short_name,description,form_text,datatype_id,class_id,is_set,creator,date_created,version,changed_by,date_changed,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT concept.concept_id,concept.retired,concept.short_name,concept.description,concept.form_text,concept.datatype_id,concept.class_id,concept.is_set,concept.creator,concept.date_created,concept.version,concept.changed_by,concept.date_changed,concept.retired_by,concept.date_retired,concept.retire_reason,concept.uuid FROM openmrs.concept";
		remoteSelectInsert(selectQuery, insertQuery);
		// concept_answer
		insertQuery = "INSERT IGNORE INTO openmrs.concept_answer(concept_answer_id,concept_id,answer_concept,answer_drug,creator,date_created,uuid,sort_weight)VALUES(?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT concept_answer.concept_answer_id,concept_answer.concept_id,concept_answer.answer_concept,concept_answer.answer_drug,concept_answer.creator,concept_answer.date_created,concept_answer.uuid,concept_answer.sort_weight FROM openmrs.concept_answer";
		remoteSelectInsert(selectQuery, insertQuery);
		// concept_class
		insertQuery = "INSERT IGNORE INTO openmrs.concept_class(concept_class_id,name,description,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT concept_class.concept_class_id,concept_class.name,concept_class.description,concept_class.creator,concept_class.date_created,concept_class.retired,concept_class.retired_by,concept_class.date_retired,concept_class.retire_reason,concept_class.uuid FROM openmrs.concept_class";
		remoteSelectInsert(selectQuery, insertQuery);
		// concept_datatype
		insertQuery = "INSERT IGNORE INTO openmrs.concept_datatype(concept_datatype_id,name,hl7_abbreviation,description,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT concept_datatype.concept_datatype_id,concept_datatype.name,concept_datatype.hl7_abbreviation,concept_datatype.description,concept_datatype.creator,concept_datatype.date_created,concept_datatype.retired,concept_datatype.retired_by,concept_datatype.date_retired,concept_datatype.retire_reason,concept_datatype.uuid FROM openmrs.concept_datatype";
		remoteSelectInsert(selectQuery, insertQuery);
		// concept_description
		insertQuery = "INSERT IGNORE INTO openmrs.concept_description(concept_description_id,concept_id,description,locale,creator,date_created,changed_by,date_changed,uuid)VALUES(?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT concept_description.concept_description_id,concept_description.concept_id,concept_description.description,concept_description.locale,concept_description.creator,concept_description.date_created,concept_description.changed_by,concept_description.date_changed,concept_description.uuid FROM openmrs.concept_description";
		remoteSelectInsert(selectQuery, insertQuery);
		// concept_map_type
		insertQuery = "INSERT IGNORE INTO openmrs.concept_map_type(concept_map_type_id,name,description,creator,date_created,changed_by,date_changed,is_hidden,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT concept_map_type.concept_map_type_id,concept_map_type.name,concept_map_type.description,concept_map_type.creator,concept_map_type.date_created,concept_map_type.changed_by,concept_map_type.date_changed,concept_map_type.is_hidden,concept_map_type.retired,concept_map_type.retired_by,concept_map_type.date_retired,concept_map_type.retire_reason,concept_map_type.uuid FROM openmrs.concept_map_type";
		remoteSelectInsert(selectQuery, insertQuery);
		// concept_name
		insertQuery = "INSERT IGNORE INTO openmrs.concept_name(concept_id,name,locale,creator,date_created,concept_name_id,voided,voided_by,date_voided,void_reason,uuid,concept_name_type,locale_preferred)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT concept_name.concept_id,concept_name.name,concept_name.locale,concept_name.creator,concept_name.date_created,concept_name.concept_name_id,concept_name.voided,concept_name.voided_by,concept_name.date_voided,concept_name.void_reason,concept_name.uuid,concept_name.concept_name_type,concept_name.locale_preferred FROM openmrs.concept_name";
		remoteSelectInsert(selectQuery, insertQuery);
		// concept_numeric
		insertQuery = "INSERT IGNORE INTO openmrs.concept_numeric(concept_id,hi_absolute,hi_critical,hi_normal,low_absolute,low_critical,low_normal,units,precise,display_precision)VALUES(?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT concept_numeric.concept_id,concept_numeric.hi_absolute,concept_numeric.hi_critical,concept_numeric.hi_normal,concept_numeric.low_absolute,concept_numeric.low_critical,concept_numeric.low_normal,concept_numeric.units,concept_numeric.precise,concept_numeric.display_precision FROM openmrs.concept_numeric";
		remoteSelectInsert(selectQuery, insertQuery);
		// concept_reference_map
		insertQuery = "INSERT IGNORE INTO openmrs.concept_reference_map(concept_map_id,creator,date_created,concept_id,uuid,concept_reference_term_id,concept_map_type_id,changed_by,date_changed)VALUES(?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT concept_reference_map.concept_map_id,concept_reference_map.creator,concept_reference_map.date_created,concept_reference_map.concept_id,concept_reference_map.uuid,concept_reference_map.concept_reference_term_id,concept_reference_map.concept_map_type_id,concept_reference_map.changed_by,concept_reference_map.date_changed FROM openmrs.concept_reference_map";
		remoteSelectInsert(selectQuery, insertQuery);
		// concept_reference_source
		insertQuery = "INSERT IGNORE INTO openmrs.concept_reference_source(concept_source_id,name,description,hl7_code,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT concept_reference_source.concept_source_id,concept_reference_source.name,concept_reference_source.description,concept_reference_source.hl7_code,concept_reference_source.creator,concept_reference_source.date_created,concept_reference_source.retired,concept_reference_source.retired_by,concept_reference_source.date_retired,concept_reference_source.retire_reason,concept_reference_source.uuid FROM openmrs.concept_reference_source";
		remoteSelectInsert(selectQuery, insertQuery);
		// concept_reference_map
		insertQuery = "INSERT IGNORE INTO openmrs.concept_reference_term(concept_reference_term_id,concept_source_id,name,code,version,description,creator,date_created,date_changed,changed_by,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT concept_reference_term.concept_reference_term_id,concept_reference_term.concept_source_id,concept_reference_term.name,concept_reference_term.code,concept_reference_term.version,concept_reference_term.description,concept_reference_term.creator,concept_reference_term.date_created,concept_reference_term.date_changed,concept_reference_term.changed_by,concept_reference_term.retired,concept_reference_term.retired_by,concept_reference_term.date_retired,concept_reference_term.retire_reason,concept_reference_term.uuid FROM openmrs.concept_reference_term";
		remoteSelectInsert(selectQuery, insertQuery);
		// concept_set
		insertQuery = "INSERT IGNORE INTO openmrs.concept_set(concept_set_id,concept_id,concept_set,sort_weight,creator,date_created,uuid)VALUES(?,?,?,?,?,?,?)";
		selectQuery = "SELECT concept_set.concept_set_id,concept_set.concept_id,concept_set.concept_set,concept_set.sort_weight,concept_set.creator,concept_set.date_created,concept_set.uuid FROM openmrs.concept_set";
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
		GfatmSyncUi.log("Importing metadata from remote source", Level.INFO);
		String selectQuery = "";
		String insertQuery = "";
		// active_list_type
		insertQuery = "INSERT IGNORE INTO openmrs.active_list_type(active_list_type_id,name,description,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT active_list_type.active_list_type_id,active_list_type.name,active_list_type.description,active_list_type.creator,active_list_type.date_created,active_list_type.retired,active_list_type.retired_by,active_list_type.date_retired,active_list_type.retire_reason,active_list_type.uuid FROM openmrs.active_list_type";
		remoteSelectInsert(selectQuery, insertQuery);
		// encounter_role
		insertQuery = "INSERT IGNORE INTO openmrs.encounter_role(encounter_role_id,name,description,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT encounter_role.encounter_role_id,encounter_role.name,encounter_role.description,encounter_role.creator,encounter_role.date_created,encounter_role.changed_by,encounter_role.date_changed,encounter_role.retired,encounter_role.retired_by,encounter_role.date_retired,encounter_role.retire_reason,encounter_role.uuid FROM openmrs.encounter_role";
		remoteSelectInsert(selectQuery, insertQuery);
		// encounter_type
		insertQuery = "INSERT IGNORE INTO openmrs.encounter_type(encounter_type_id,name,description,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid,edit_privilege,view_privilege)VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT encounter_type.encounter_type_id,encounter_type.name,encounter_type.description,encounter_type.creator,encounter_type.date_created,encounter_type.retired,encounter_type.retired_by,encounter_type.date_retired,encounter_type.retire_reason,encounter_type.uuid,encounter_type.edit_privilege,encounter_type.view_privilege FROM openmrs.encounter_type";
		remoteSelectInsert(selectQuery, insertQuery);
		// field_type
		insertQuery = "INSERT IGNORE INTO openmrs.field_type(field_type_id,name,description,is_set,creator,date_created,uuid)VALUES(?,?,?,?,?,?,?)";
		selectQuery = "SELECT field_type.field_type_id,field_type.name,field_type.description,field_type.is_set,field_type.creator,field_type.date_created,field_type.uuid FROM openmrs.field_type";
		remoteSelectInsert(selectQuery, insertQuery);
		// hl7_source
		insertQuery = "INSERT IGNORE INTO openmrs.hl7_source(hl7_source_id,name,description,creator,date_created,uuid)VALUES(?,?,?,?,?,?)";
		selectQuery = "SELECT hl7_source.hl7_source_id,hl7_source.name,hl7_source.description,hl7_source.creator,hl7_source.date_created,hl7_source.uuid FROM openmrs.hl7_source";
		remoteSelectInsert(selectQuery, insertQuery);
		// htmlformentry_html_form
		insertQuery = "INSERT IGNORE INTO openmrs.htmlformentry_html_form(id,form_id,name,xml_data,creator,date_created,changed_by,date_changed,retired,uuid,description,retired_by,date_retired,retire_reason)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT htmlformentry_html_form.id,htmlformentry_html_form.form_id,htmlformentry_html_form.name,htmlformentry_html_form.xml_data,htmlformentry_html_form.creator,htmlformentry_html_form.date_created,htmlformentry_html_form.changed_by,htmlformentry_html_form.date_changed,htmlformentry_html_form.retired,htmlformentry_html_form.uuid,htmlformentry_html_form.description,htmlformentry_html_form.retired_by,htmlformentry_html_form.date_retired,htmlformentry_html_form.retire_reason FROM openmrs.htmlformentry_html_form";
		remoteSelectInsert(selectQuery, insertQuery);
		// patient_identifier_type
		insertQuery = "INSERT INTO openmrs.patient_identifier_type(patient_identifier_type_id,name,description,format,check_digit,creator,date_created,required,format_description,validator,location_behavior,retired,retired_by,date_retired,retire_reason,uuid,uniqueness_behavior)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT patient_identifier_type.patient_identifier_type_id,patient_identifier_type.name,patient_identifier_type.description,patient_identifier_type.format,patient_identifier_type.check_digit,patient_identifier_type.creator,patient_identifier_type.date_created,patient_identifier_type.required,patient_identifier_type.format_description,patient_identifier_type.validator,patient_identifier_type.location_behavior,patient_identifier_type.retired,patient_identifier_type.retired_by,patient_identifier_type.date_retired,patient_identifier_type.retire_reason,patient_identifier_type.uuid,patient_identifier_type.uniqueness_behavior FROM openmrs.patient_identifier_type";
		remoteSelectInsert(selectQuery, insertQuery);
		// person_attribute_type
		insertQuery = "INSERT INTO openmrs.person_attribute_type(person_attribute_type_id,name,description,format,foreign_key,searchable,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,edit_privilege,sort_weight,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT person_attribute_type.person_attribute_type_id,person_attribute_type.name,person_attribute_type.description,person_attribute_type.format,person_attribute_type.foreign_key,person_attribute_type.searchable,person_attribute_type.creator,person_attribute_type.date_created,person_attribute_type.changed_by,person_attribute_type.date_changed,person_attribute_type.retired,person_attribute_type.retired_by,person_attribute_type.date_retired,person_attribute_type.retire_reason,person_attribute_type.edit_privilege,person_attribute_type.sort_weight,person_attribute_type.uuid FROM openmrs.person_attribute_type";
		remoteSelectInsert(selectQuery, insertQuery);
		// order_type
		insertQuery = "INSERT IGNORE INTO openmrs.order_type(order_type_id,name,description,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid,java_class_name,parent,changed_by,date_changed)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT order_type.order_type_id,order_type.name,order_type.description,order_type.creator,order_type.date_created,order_type.retired,order_type.retired_by,order_type.date_retired,order_type.retire_reason,order_type.uuid,order_type.java_class_name,order_type.parent,order_type.changed_by,order_type.date_changed FROM openmrs.order_type";
		remoteSelectInsert(selectQuery, insertQuery);
		// scheduler_task_config
		insertQuery = "INSERT IGNORE INTO openmrs.scheduler_task_config(task_config_id,name,description,schedulable_class,start_time,start_time_pattern,repeat_interval,start_on_startup,started,created_by,date_created,changed_by,date_changed,last_execution_time,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT scheduler_task_config.task_config_id,scheduler_task_config.name,scheduler_task_config.description,scheduler_task_config.schedulable_class,scheduler_task_config.start_time,scheduler_task_config.start_time_pattern,scheduler_task_config.repeat_interval,scheduler_task_config.start_on_startup,scheduler_task_config.started,scheduler_task_config.created_by,scheduler_task_config.date_created,scheduler_task_config.changed_by,scheduler_task_config.date_changed,scheduler_task_config.last_execution_time,scheduler_task_config.uuid FROM openmrs.scheduler_task_config";
		remoteSelectInsert(selectQuery, insertQuery);
		// visit_type
		insertQuery = "INSERT IGNORE INTO openmrs.visit_type(visit_type_id,name,description,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid)VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";
		selectQuery = "SELECT visit_type.visit_type_id,visit_type.name,visit_type.description,visit_type.creator,visit_type.date_created,visit_type.changed_by,visit_type.date_changed,visit_type.retired,visit_type.retired_by,visit_type.date_retired,visit_type.retire_reason,visit_type.uuid FROM openmrs.visit_type";
		remoteSelectInsert(selectQuery, insertQuery);
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
		GfatmSyncUi.log("Executing: " + insertQuery + " " + selectQuery,
				Level.INFO);
		Connection remoteConnection = remoteDb.getConnection();
		ResultSet data = remoteConnection.createStatement().executeQuery(selectQuery);
		ResultSetMetaData metaData = data.getMetaData();
		Connection localConnection = DriverManager.getConnection(localDb.getUrl(), localDb.getUsername(), localDb.getPassword());
		while (data.next()) {
			PreparedStatement target = localConnection.prepareStatement(insertQuery);
			for (int i = 1; i <= metaData.getColumnCount(); i++) {
				target.setString(i, data.getString(i));
			}
			target.executeUpdate();
		}
		GfatmSyncUi.updateProgress();
	}

	/**
	 * @return the localDb
	 */
	public DatabaseUtil getLocalDb() {
		return localDb;
	}

	/**
	 * @param localDb the localDb to set
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
	 * @param remoteDb the remoteDb to set
	 */
	public void setRemoteDb(DatabaseUtil remoteDb) {
		this.remoteDb = remoteDb;
	}

	/**
	 * @return the syncUsers
	 */
	public boolean isSyncUsers() {
		return syncUsers;
	}

	/**
	 * @param syncUsers the syncUsers to set
	 */
	public void setSyncUsers(boolean syncUsers) {
		this.syncUsers = syncUsers;
	}

	/**
	 * @return the syncLocations
	 */
	public boolean isSyncLocations() {
		return syncLocations;
	}

	/**
	 * @param syncLocations the syncLocations to set
	 */
	public void setSyncLocations(boolean syncLocations) {
		this.syncLocations = syncLocations;
	}

	/**
	 * @return the syncConcepts
	 */
	public boolean isSyncConcepts() {
		return syncConcepts;
	}

	/**
	 * @param syncConcepts the syncConcepts to set
	 */
	public void setSyncConcepts(boolean syncConcepts) {
		this.syncConcepts = syncConcepts;
	}

	/**
	 * @return the syncOtherMetadata
	 */
	public boolean isSyncOtherMetadata() {
		return syncOtherMetadata;
	}

	/**
	 * @param syncOtherMetadata the syncOtherMetadata to set
	 */
	public void setSyncOtherMetadata(boolean syncOtherMetadata) {
		this.syncOtherMetadata = syncOtherMetadata;
	}

	/**
	 * @return the progressRange
	 */
	public int getProgressRange() {
		return progressRange;
	}

	/**
	 * @param progressRange the progressRange to set
	 */
	public void setProgressRange(int progressRange) {
		this.progressRange = progressRange;
	}
}
