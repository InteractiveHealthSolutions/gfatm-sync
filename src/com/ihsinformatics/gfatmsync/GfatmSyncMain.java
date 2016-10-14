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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
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
import com.ihsinformatics.util.VersionUtil;

/**
 * @author owais.hussain@ihsinformatics.com
 *
 */
public class GfatmSyncMain {

	public static final Logger log = Logger.getLogger(Class.class.getName());
	public static final String createWarehouseFile = "res/create_datawarehouse.sql";
	public static final VersionUtil version = new VersionUtil(true, false,
			false, 0, 1, 1);
	public static String propertiesFile = "res/gfatm-sync.properties";
	public Properties props;
	public String dataPath;
	public String dwSchema;
	public DatabaseUtil localDb;
	public Boolean includeMetadata = true;
	public String remoteSchema;
	public DatabaseUtil remoteDb;

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		// Check arguments first
		if (args[0] == null || args.length == 0) {
			System.out
					.println("Arguments are invalid. Arguments must be provided as:");
			System.out.println("-p path to properties file");
			System.out
					.println("-r to hard reset warehouse (Extract/Load > Transform > Dimensional modeling > Fact tables)");
			System.out
					.println("-d to create data warehouse dimentions and facts");
			System.out.println("-u to update data warehouse (nightly run)");
			System.out.println("-i to import metadata from remote host");
			return;
		}
		System.out.println(version.toString());
		boolean doReset = false, doCreateDw = false, doUpdateDw = false, doImport = false;
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-p")) {
				propertiesFile = args[i + 1];
			} else if (args[i].equalsIgnoreCase("-r")) {
				doReset = doCreateDw = true;
			} else if (args[i].equalsIgnoreCase("-d")) {
				doCreateDw = true;
			} else if (args[i].equalsIgnoreCase("-u")) {
				doUpdateDw = true;
			} else if (args[i].equalsIgnoreCase("-i")) {
				doImport = true;
			}
		}
		// Read properties file
		GfatmSyncMain gfatm = new GfatmSyncMain();
		gfatm.readProperties(propertiesFile);
		if (doReset) {
			gfatm.resetDatawarehouse();
			gfatm.insertData();
		}
		if (doUpdateDw) {
			gfatm.insertData();
			gfatm.updateData();
		}
		if (doCreateDw) {
			gfatm.dimensionModeling();
			gfatm.factModeling();
		}
		if (doImport) {
			gfatm.importMetadata();
		}
		System.exit(0);
	}

	public GfatmSyncMain() {
		dataPath = System.getProperty("user.home") + File.separatorChar;
		localDb = new DatabaseUtil();
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
	public void readProperties(String propertiesFile) {
		try {
			InputStream propFile = new FileInputStream(propertiesFile);
			if (propFile != null) {
				props.load(propFile);
				String url = props.getProperty("local.connection.url",
						"jdbc:mysql://localhost:3306/gfatm_dw");
				String driverName = props.getProperty(
						"local.connection.driver_class", "com.mysql.jdbc.Driver");
				dwSchema = props.getProperty("local.connection.database",
						"gfatm_dw");
				String username = props.getProperty("local.connection.username",
						"root");
				String password = props.getProperty("local.connection.password");
				localDb.setConnection(url, dwSchema, driverName, username,
						password);
				System.out.println(localDb.tryConnection());
				includeMetadata = props.getProperty("local.include_metadata", "true").equalsIgnoreCase("true");
			}
		} catch (IOException e) {
			e.printStackTrace();
			log.severe("Properties file not found in class path.");
		}
	}

	/**
	 * Fetch data from remote database and insert into local database
	 * 
	 * @param selectQuery
	 * @param insertQuery
	 * @return
	 * @throws SQLException 
	 */
	public void remoteSelectInsert(String selectQuery, String insertQuery) throws SQLException{
		Connection remoteConnection = remoteDb.getConnection();
		Connection localConnection = localDb.getConnection();
		PreparedStatement source = remoteConnection.prepareStatement(selectQuery);
		PreparedStatement target = localConnection.prepareStatement(insertQuery);
		ResultSet data = source.executeQuery();
		ResultSetMetaData metaData = data.getMetaData();
		while (data.next()) {
			for (int i = 1; i <= metaData.getColumnCount(); i++) {
				target.setString(i, data.getString(i));
			}
			target.executeUpdate();
		}
	}
	
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
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Creates/Updates all fact tables
	 */
	public void factModeling() {
		try {
			// Chillar facts
			// Users
			StringBuilder query = new StringBuilder("insert ignore into fact_user ");
			query.append("select u.implementation_id, t.*, count(*) users from dim_user as u ");
			query.append("inner join dim_datetime as t on date(t.full_date) = date(u.date_created) ");
			query.append("group by u.implementation_id, t.datetime_id ");
			log.info("Inserting concept facts");
			localDb.runCommand(CommandType.INSERT, query.toString());
			// Concepts
			query = new StringBuilder("insert ignore into fact_concept ");
			query.append("select c.implementation_id, ");
			query.append("(select count(*) from dim_concept where implementation_id = c.implementation_id and retired = 0) as active, ");
			query.append("(select count(*) from dim_concept where implementation_id = c.implementation_id and data_type in ('Numeric','Boolean','Date','Time','Datetime')) as real_valued, ");
			query.append("(select count(*) from dim_concept where implementation_id = c.implementation_id and data_type in ('N/A','Text')) as open_text, ");
			query.append("(select count(*) from dim_concept where implementation_id = c.implementation_id and data_type = 'Coded') as coded, ");
			query.append("(select count(DISTINCT uuid) from dim_concept) as unique_concepts, ");
			query.append("count(*) as total from dim_concept as c ");
			query.append("group by c.implementation_id ");
			log.info("Inserting concept facts");
			localDb.runCommand(CommandType.INSERT, query.toString());
			// Locations
			query = new StringBuilder("insert ignore into fact_location ");
			query.append("select l.implementation_id, t.*, count(*) as total from dim_location as l ");
			query.append("inner join dim_datetime as t on date(t.full_date) = date(l.date_created) ");
			query.append("group by l.implementation_id, t.datetime_id ");
			log.info("Inserting location facts");
			localDb.runCommand(CommandType.INSERT, query.toString());
			
			// TODO: Add more facts
			// THIS IS WHERE I LEFT LAST
			
			log.info("Finished fact modeling");
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Creates/Updates all dimension tables
	 */
	public void dimensionModeling() {
		try {
			StringBuilder query = new StringBuilder();
			// Fill in datetime dimension table
			// Detect the last date in dimension and begin from there
			Object lastSqlDate = localDb.runCommand(CommandType.SELECT,
					"select max(full_date) as latest from dim_datetime");
			Calendar start = Calendar.getInstance();
			start.set(Calendar.YEAR, 2000);
			start.set(Calendar.MONTH, Calendar.JANUARY);
			start.set(Calendar.DATE, 1);
			if (lastSqlDate != null) {
				Date latestDate = DateTimeUtil.getDateFromString(
						lastSqlDate.toString(), DateTimeUtil.SQL_DATETIME);
				start.setTime(latestDate);
			}
			start.add(Calendar.DATE, 1);
			Calendar end = Calendar.getInstance();
			end.set(Calendar.HOUR, 0);
			query = new StringBuilder("insert into dim_datetime values ");
			while (start.getTime().before(end.getTime())) {
				String sqlDate = "'" + DateTimeUtil.getSqlDate(start.getTime())
						+ "'";
				query.append("(0, " + sqlDate + ", ");
				query.append("year(" + sqlDate + "), ");
				query.append("month(" + sqlDate + "), ");
				query.append("day(" + sqlDate + "), ");
				query.append("dayname(" + sqlDate + "), ");
				query.append("monthname(" + sqlDate + ")),");
				start.add(Calendar.DATE, 1);
			}
			query.setCharAt(query.length() - 1, ';');
			localDb.runCommand(CommandType.INSERT, query.toString());

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
			localDb.runCommand(CommandType.INSERT, query.toString());
			query = new StringBuilder(
					"update dim_concept set full_name = 'Yes', concept = 'Yes' where concept_id = 1");
			log.info("Setting names of Yes/No concepts.");
			localDb.runCommand(CommandType.UPDATE, query.toString());
			query = new StringBuilder(
					"update dim_concept set full_name = 'No', concept = 'No' where concept_id = 2");
			localDb.runCommand(CommandType.UPDATE, query.toString());

			// Fill the location dimension data
			query = new StringBuilder(
					"insert ignore into dim_location (surrogate_id, implementation_id, location_id, location_name, description, address1, address2, city_village, state_province, postal_code, country, latitude, longitude, creator, date_created, retired, parent_location, uuid) ");
			query.append("select l.surrogate_id, l.implementation_id, l.location_id, l.name as location_name, l.description, l.address1, l.address2, l.city_village, l.state_province, l.postal_code, l.country, l.latitude, l.longitude, l.creator, l.date_created, l.retired, l.parent_location, l.uuid from location as l ");
			log.info("Inserting new locations to dimension.");
			localDb.runCommand(CommandType.INSERT, query.toString());

			// Fill the user dimension data
			query = new StringBuilder(
					"insert ignore into dim_user (surrogate_id, implementation_id, user_id, username, person_id, identifier, secret_question, secret_answer, creator, date_created, changed_by, date_changed, retired, retire_reason, uuid) ");
			query.append("select u.surrogate_id, u.implementation_id, u.user_id, u.username, u.person_id, p.identifier, u.secret_question, u.secret_answer, u.creator, u.date_created, u.changed_by, u.date_changed, u.retired, u.retire_reason, u.uuid from users as u ");
			query.append("left outer join provider as p on p.implementation_id = u.implementation_id and p.person_id = u.person_id");
			log.info("Inserting new users to dimension.");
			localDb.runCommand(CommandType.INSERT, query.toString());

			// Collect latest patient identifiers
			log.info("Setting preferred identifiers to OpenMRS ID (with check digit).");
			localDb.runCommand(CommandType.UPDATE,
					"update patient_identifier set preferred = 1 where identifier_type = 3");
			log.info("Setting identifiers other than OpenMRS ID to non-preferred.");
			localDb.runCommand(CommandType.UPDATE,
					"update patient_identifier set preferred = 0 where identifier_type <> 3");
			log.info("Selecting patient identifiers.");
			localDb.runCommand(CommandType.DROP,
					"drop table if exists patient_latest_identifier");
			query = new StringBuilder("create table patient_latest_identifier ");
			query.append("select * from patient_identifier as a having a.patient_id = (select max(patient_id) from patient_identifier where implementation_id = a.implementation_id and patient_id = a.patient_id and preferred = 1 and voided = 0) union ");
			query.append("select * from patient_identifier as a having a.patient_id = (select max(patient_id) from patient_identifier where implementation_id = a.implementation_id and patient_id = a.patient_id and preferred = 0 and voided = 0)");
			localDb.runCommand(CommandType.CREATE, query.toString());
			localDb.runCommand(
					CommandType.ALTER,
					"alter table patient_latest_identifier add primary key surrogate_id (surrogate_id)");

			// Collect latest person names
			log.info("Selecting people names (preferred/latest).");
			localDb.runCommand(CommandType.DROP,
					"drop table if exists person_latest_name");
			query = new StringBuilder("create table person_latest_name ");
			query.append("select * from person_name as a where a.person_name_id = (select max(person_name_id) from person_name where implementation_id = a.implementation_id and person_id = a.person_id and preferred = 1)");
			localDb.runCommand(CommandType.CREATE, query.toString());
			query = new StringBuilder("insert into person_latest_name ");
			query.append("select * from person_name as a where a.person_name_id = (select max(person_name_id) from person_name where implementation_id = a.implementation_id and person_id = a.person_id and preferred = 0)");
			localDb.runCommand(CommandType.INSERT, query.toString());
			localDb.runCommand(CommandType.ALTER,
					"alter table person_latest_name add primary key surrogate_id (surrogate_id)");

			// Collect latest person addresses
			log.info("Selecting people addresses (preferred/latest).");
			localDb.runCommand(CommandType.DROP,
					"drop table if exists person_latest_address");
			query = new StringBuilder("create table person_latest_address ");
			query.append("select * from person_address as a where a.person_address_id = (select max(person_address_id) from person_address where implementation_id = a.implementation_id and person_id = a.person_id and preferred = 1)");
			localDb.runCommand(CommandType.CREATE, query.toString());
			query = new StringBuilder("insert into person_latest_address ");
			query.append("select * from person_address as a where a.person_address_id = (select max(person_address_id) from person_address where implementation_id = a.implementation_id and person_id = a.person_id and preferred = 0)");
			localDb.runCommand(CommandType.INSERT, query.toString());
			localDb.runCommand(CommandType.ALTER,
					"alter table person_latest_address add primary key surrogate_id (surrogate_id)");

			// Recreate person attributes
			log.info("Transforming people attribute.");
			localDb.runCommand(CommandType.DROP,
					"drop table if exists person_attribute_merged");
			Object[][] attributeTypes = localDb.getTableData(
					"person_attribute_type", "person_attribute_type_id,name",
					null, true);
			StringBuilder groupConcat = new StringBuilder();
			for (Object[] type : attributeTypes) {
				String typeId = type[0].toString();
				String typeName = type[1].toString().replace(" ", "_")
						.replace("'", "").replace("(\\W|^_)*", "_")
						.toLowerCase();
				groupConcat
						.append("group_concat(if(a.person_attribute_type_id = "
								+ typeId + ", a.value, null)) as " + typeName
								+ ", ");
			}
			groupConcat.append("'' as BLANK ");
			query = new StringBuilder("create table person_attribute_merged ");
			query.append("select a.implementation_id, a.person_id, a.creator, a.date_created, a.uuid, ");
			query.append(groupConcat.toString());
			query.append("from person_attribute as a where a.voided = 0 ");
			query.append("group by a.implementation_id, a.person_id");
			localDb.runCommand(CommandType.CREATE, query.toString());
			localDb.runCommand(
					CommandType.ALTER,
					"alter table person_attribute_merged add primary key (implementation_id, person_id)");

			// Fill the patient dimension data
			query = new StringBuilder(
					"insert ignore into dim_patient (surrogate_id, implementation_id, patient_id, identifier, secondary_identifier, gender, birthdate, birthdate_estimated, dead, first_name, middle_name, last_name, race, birthplace, citizenship, mother_name, civil_status, health_district, health_center, primary_mobile, secondary_mobile, primary_phone, secondary_phone, primary_mobile_owner, secondary_mobile_owner, primary_phone_owner, secondary_phone_owner, address1, address2, city_village, state_province, postal_code, country, creator, date_created, changed_by, date_changed, voided, uuid) ");
			query.append("select p.surrogate_id, p.implementation_id, p.patient_id, i.identifier, pr.gender, pr.birthdate, pr.birthdate_estimated, pr.dead, n.given_name as first_name, n.middle_name, n.family_name as last_name, pa.*, a.address1, a.address2, a.city_village, a.state_province, a.postal_code, a.country, p.creator, p.date_created, p.changed_by, p.date_changed, p.voided, pr.uuid from patient as p ");
			query.append("inner join person as pr on pr.implementation_id = p.implementation_id and pr.person_id = p.patient_id ");
			query.append("left outer join patient_latest_identifier as i on i.implementation_id = p.implementation_id and i.patient_id = p.patient_id ");
			query.append("left outer join person_latest_name as n on n.implementation_id = p.implementation_id and n.person_id = pr.person_id ");
			query.append("left outer join person_latest_address as a on a.implementation_id = p.implementation_id and a.person_id = p.patient_id ");
			query.append("left outer join person_attribute_merged as pa on pa.implementation_id = p.implementation_id and pa.person_id = p.patient_id ");
			query.append("where p.voided = 0");
			log.info("Inserting new patients to dimension.");
			localDb.runCommand(CommandType.INSERT, query.toString());

			// Fill the encounter dimension data
			query = new StringBuilder("insert ignore into dim_encounter ");
			query.append("select e.surrogate_id, e.implementation_id, e.encounter_id, e.encounter_type, et.name as encounter_name, et.description, e.patient_id, e.location_id, p.identifier as provider, e.encounter_datetime as date_entered, e.creator, e.date_created as date_start, e.changed_by, e.date_changed, e.date_created as date_end, e.uuid from encounter as e ");
			query.append("inner join encounter_type as et on et.encounter_type_id = e.encounter_type ");
			query.append("left outer join encounter_provider as ep on ep.encounter_id = e.encounter_id ");
			query.append("left outer join provider as p on p.person_id = ep.provider_id ");
			query.append("where e.voided = 0");
			log.info("Inserting new encounters to dimension.");
			localDb.runCommand(CommandType.INSERT, query.toString());

			// Fill the observation dimension data
			query = new StringBuilder("insert ignore into dim_obs ");
			query.append("select e.surrogate_id, e.implementation_id, e.encounter_id, e.encounter_type, e.patient_id, p.identifier, e.provider, o.obs_id, o.concept_id, c.concept as question, obs_datetime, o.location_id, concat(ifnull(o.value_boolean, ''), ifnull(ifnull(c2.concept, c2.full_name), ''), ifnull(o.value_datetime, ''), ifnull(o.value_numeric, ''), ifnull(o.value_text, '')) as answer, o.value_boolean, o.value_coded, o.value_datetime, o.value_numeric, o.value_text, o.creator, o.date_created, o.voided, o.uuid from obs as o ");
			query.append("inner join dim_concept as c on c.implementation_id = o.implementation_id and c.concept_id = o.concept_id ");
			query.append("inner join dim_encounter as e on e.implementation_id = o.implementation_id and e.encounter_id = o.encounter_id ");
			query.append("inner join dim_patient as p on p.implementation_id = e.implementation_id and p.patient_id = e.patient_id ");
			query.append("left outer join dim_concept as c2 on c2.implementation_id = o.implementation_id and c2.concept_id = o.value_coded ");
			query.append("where o.voided = 0");
			log.info("Inserting new observations to dimension.");
			localDb.runCommand(CommandType.INSERT, query.toString());

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
	 * Delete all dimension and fact tables and recreate
	 */
	public void resetDatawarehouse() {
		FileUtil fu = new FileUtil();
		String[] queries = fu.getLines(createWarehouseFile);
		for (String query : queries) {
			try {
				if (query.startsWith("DROP TABLE")) {
					localDb.runCommand(CommandType.DROP, query);
				} else if (query.startsWith("CREATE TABLE")) {
					localDb.runCommand(CommandType.CREATE, query);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Insert data from all sources into data warehouse
	 */
	public void insertData() {
		// Fetch source databases from _implementation table
		Object[][] data = localDb.getTableData("_implementation",
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
				insertPeopleData(implementationId, databases.get(i));
				// Enable only if metadata tables are to be included
				if (includeMetadata) {
					insertUserData(implementationId, databases.get(i));
					insertLocationData(implementationId, databases.get(i));
					insertConceptData(implementationId, databases.get(i));
				}
				insertPatientData(implementationId, databases.get(i));
				insertEncounterData(implementationId, databases.get(i));
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
	public void insertPeopleData(Integer implementationId, String database)
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
			localDb.runCommand(CommandType.INSERT, query.toString());
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
	public void insertUserData(Integer implementationId, String database)
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
			localDb.runCommand(CommandType.INSERT, query.toString());
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
		localDb.runCommand(CommandType.INSERT, query.toString());
		query = new StringBuilder();
		query.append("INSERT INTO role_privilege ");
		query.append("SELECT 0,'" + implementationId + "', t.* FROM "
				+ database + ".role_privilege AS t ");
		query.append("WHERE CONCAT(t.role, t.privilege) NOT IN (SELECT CONCAT(role, privilege) FROM role_privilege WHERE implementation_id="
				+ implementationId + ")");
		log.info("Inserting data from " + database
				+ ".role_privilege into data warehouse");
		localDb.runCommand(CommandType.INSERT, query.toString());
		query = new StringBuilder();
		query.append("INSERT INTO user_property ");
		query.append("SELECT 0,'" + implementationId + "', t.* FROM "
				+ database + ".user_property AS t ");
		query.append("WHERE CONCAT(t.user_id, t.property) NOT IN (SELECT CONCAT(user_id, property) FROM user_role WHERE implementation_id="
				+ implementationId + ")");
		log.info("Inserting data from " + database
				+ ".user_property into data warehouse");
		localDb.runCommand(CommandType.INSERT, query.toString());
		query = new StringBuilder();
		query.append("INSERT INTO user_role ");
		query.append("SELECT 0,'" + implementationId + "', t.* FROM "
				+ database + ".user_role AS t ");
		query.append("WHERE CONCAT(t.user_id, t.role) NOT IN (SELECT CONCAT(user_id, role) FROM user_role WHERE implementation_id="
				+ implementationId + ")");
		log.info("Inserting data from " + database
				+ ".user_role into data warehouse");
		localDb.runCommand(CommandType.INSERT, query.toString());
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
	public void insertLocationData(Integer implementationId, String database)
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
			localDb.runCommand(CommandType.INSERT, query.toString());
		}
		query = new StringBuilder();
		query.append("INSERT INTO location_tag_map ");
		query.append("SELECT 0,'" + implementationId + "', t.* FROM "
				+ database + ".location_tag_map AS t ");
		query.append("WHERE CONCAT(t.location_id, t.location_tag_id) NOT IN (SELECT CONCAT(location_id, location_tag_id) FROM user_role WHERE implementation_id="
				+ implementationId + ")");
		log.info("Inserting data from " + database
				+ ".location_tag_map into data warehouse");
		localDb.runCommand(CommandType.INSERT, query.toString());
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
	public void insertConceptData(Integer implementationId, String database)
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
			localDb.runCommand(CommandType.INSERT, query.toString());
		}
		query = new StringBuilder();
		query.append("INSERT INTO concept_numeric ");
		query.append("SELECT 0,'" + implementationId + "', t.* FROM "
				+ database + ".concept_numeric AS t ");
		query.append("WHERE t.concept_id NOT IN (SELECT concept_id FROM user_role WHERE implementation_id="
				+ implementationId + ")");
		log.info("Inserting data from " + database
				+ ".concept_numeric into data warehouse");
		localDb.runCommand(CommandType.INSERT, query.toString());
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
	public void insertPatientData(Integer implementationId, String database)
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
	public void insertEncounterData(Integer implementationId, String database)
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
			localDb.runCommand(CommandType.INSERT, query.toString());
		}
	}

	/**
	 * Update recently changed data from all sources into data warehouse
	 */
	public void updateData() {
		// Fetch source databases from _implementation table
		Object[][] data = localDb.getTableData("_implementation",
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
	public void updateUserData(Integer implementationId, String database)
			throws InstantiationException, IllegalAccessException,
			ClassNotFoundException {
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
	public void updateLocationData(Integer implementationId, String database)
			throws InstantiationException, IllegalAccessException,
			ClassNotFoundException {
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
	public void updateConceptData(Integer implementationId, String database)
			throws InstantiationException, IllegalAccessException,
			ClassNotFoundException {
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
	public void updatePatientData(Integer implementationId, String database)
			throws InstantiationException, IllegalAccessException,
			ClassNotFoundException {
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
	public void updateEncounterData(Integer implementationId, String database)
			throws InstantiationException, IllegalAccessException,
			ClassNotFoundException {
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
}
