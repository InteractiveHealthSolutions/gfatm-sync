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
import java.util.Calendar;
import java.util.Date;
import java.util.logging.Logger;

import com.ihsinformatics.util.CollectionsUtil;
import com.ihsinformatics.util.CommandType;
import com.ihsinformatics.util.DatabaseUtil;
import com.ihsinformatics.util.DateTimeUtil;
import com.ihsinformatics.util.StringUtil;

/**
 * @author owais.hussain@ihsinformatics.com
 *
 */
public class DimensionController {

	private static final Logger log = Logger.getLogger(Class.class.getName());
	private DatabaseUtil db;

	public static void main(String[] args) {
		DatabaseUtil myDb = new DatabaseUtil();
		myDb.setConnection("jdbc:mysql://127.0.0.1:3306/gfatm_dw?autoReconnect=true&useSSL=false", "gfatm_dw", "com.mysql.jdbc.Driver", "root", "jingle94");
		myDb.tryConnection();
		DimensionController dc = new DimensionController(myDb);
		dc.modelDimensions();
	}
	
	public DimensionController(DatabaseUtil db) {
		this.db = db;
	}

	/**
	 * Perform dimension modeling
	 */
	public void modelDimensions() {
		Calendar from = Calendar.getInstance();
		from.set(2000, 0, 1);
		Calendar to = Calendar.getInstance();
		Object[][] sources = db.getTableData("_implementation", "implementation_id", 
				"active=1 AND status='STOPPED' " 
		// TODO: Enable on production + "AND date(last_updated) = current_date()"
				);
		// For each source, import all data
		// TODO: Restrict by date
		for (Object[] source : sources) {
			int implementationId = Integer.parseInt(source[0].toString());
			modelDimensions(from.getTime(), to.getTime(), implementationId);
		}
	}
	
	public void modelDimensions(Date from, Date to, int implementationId) {
		try {
			log.info("Starting dimension modeling");
			timeDimension();
			//conceptDimension(from, to, implementationId);
			locationDimension(from, to, implementationId);
			userDimension(from, to, implementationId);
			patientDimension(from, to, implementationId);
			encounterAndObsDimension(from, to, implementationId);
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
	 * Fill in Date/Time dimension table by detecting the last date in dimension
	 * 
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 * @throws ParseException
	 */
	public void timeDimension() throws InstantiationException,
			IllegalAccessException, ClassNotFoundException, ParseException {
		Object lastSqlDate = db.runCommand(CommandType.SELECT,
				"select max(full_date) as latest from dim_datetime");
		Calendar start = Calendar.getInstance();
		start.set(Calendar.YEAR, 2000);
		start.set(Calendar.MONTH, Calendar.JANUARY);
		start.set(Calendar.DATE, 1);
		if (lastSqlDate != null) {
			Date latestDate = DateTimeUtil.getDateFromString(
					lastSqlDate.toString(), DateTimeUtil.SQL_DATE);
			start.setTime(latestDate);
		}
		start.add(Calendar.DATE, 1);
		Calendar end = Calendar.getInstance();
		end.set(Calendar.HOUR, 0);
		if (!start.getTime().before(end.getTime())) {
			return;
		}
		StringBuilder query = new StringBuilder(
				"insert into dim_datetime values ");
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
		db.runCommand(CommandType.INSERT, query.toString());
	}

	/**
	 * Fill in Concept dimension table
	 * 
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 */
	public void conceptDimension(Date from, Date to, int implementationId) throws InstantiationException,
			IllegalAccessException, ClassNotFoundException {
		// First, remove existing concepts for this implementation
		String deleteQuery = "delete from dim_concept where implementation_id = " + implementationId;
		log.info("Deleting existing concepts.");
		db.runCommand(CommandType.DELETE, deleteQuery);
		StringBuilder query = new StringBuilder();
		query.append("insert ignore into dim_concept (surrogate_key, implementation_id, concept_id, full_name, concept, description, retired, data_type, class, hi_absolute, hi_critical, hi_normal, low_absolute, low_critical, low_normal, creator, date_created, version, changed_by, date_changed, uuid) ");
		query.append("select c.surrogate_key, c.implementation_id, c.concept_id, n1.name as full_name, n2.name as concept, d.description, c.retired, dt.name as data_type, cl.name as class, cn.hi_absolute, cn.hi_critical, cn.hi_normal, cn.low_absolute, cn.low_critical, cn.low_normal, c.creator, c.date_created, c.version, c.changed_by, c.date_changed, c.uuid from concept as c ");
		query.append("left outer join concept_datatype as dt on dt.implementation_id = c.implementation_id and dt.concept_datatype_id = c.datatype_id ");
		query.append("left outer join concept_class as cl on cl.implementation_id = c.implementation_id and cl.concept_class_id = c.class_id ");
		query.append("left outer join concept_name as n1 on n1.implementation_id = c.implementation_id and n1.concept_id = c.concept_id and n1.locale = 'en' and n1.voided = 0 and n1.concept_name_type = 'FULLY_SPECIFIED' ");
		query.append("left outer join concept_name as n2 on n2.implementation_id = c.implementation_id and n2.concept_id = c.concept_id and n2.locale = 'en' and n2.voided = 0 and n2.concept_name_type <> 'FULLY_SPECIFIED' ");
		query.append("left outer join concept_description as d on d.implementation_id = c.implementation_id and d.concept_id = c.concept_id and d.locale = 'en' ");
		query.append("left outer join concept_numeric as cn on cn.implementation_id = c.implementation_id and cn.concept_id = c.concept_id ");
		query.append("where c.implementation_id = '" + implementationId + "' ");
		query.append("and c.surrogate_key not in (select surrogate_key from dim_concept where implementation_id = c.implementation_id)");
		log.info("Inserting new concepts to dimension.");
		db.runCommand(CommandType.INSERT, query.toString());
		query = new StringBuilder(
				"update dim_concept set full_name = 'Yes', concept = 'Yes' where concept_id = 1");
		log.info("Setting names of Yes/No concepts.");
		db.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder(
				"update dim_concept set full_name = 'No', concept = 'No' where concept_id = 2");
		db.runCommand(CommandType.UPDATE, query.toString());
	}

	/**
	 * Fill in Location dimension table
	 * 
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 * @throws SQLException 
	 */
	public void locationDimension(Date from, Date to, int implementationId) throws InstantiationException,
			IllegalAccessException, ClassNotFoundException {
		StringBuilder query = new StringBuilder("delete from dim_location where implementation_id = " + implementationId);
		log.info("Deleting existing locations.");
		db.runCommand(CommandType.DELETE, query.toString());
		query = new StringBuilder(
				"update location_attribute set value_reference = 'Yes' where value_reference = 'true'");
		db.runCommand(CommandType.UPDATE, query.toString());
		query = new StringBuilder(
				"update location_attribute set value_reference = 'No' where value_reference = 'false'");
		db.runCommand(CommandType.UPDATE, query.toString());
		log.info("Transforming location attributes.");
		db.runCommand(CommandType.DROP,
				"drop table if exists location_attribute_merged");
		Object[][] attributeTypes = db.getTableData("location_attribute_type", "location_attribute_type_id,name", null, true);
		StringBuilder groupConcat = new StringBuilder();
		for (Object[] type : attributeTypes) {
			String typeId = type[0].toString();
			String typeName = type[1].toString().replace(" ", "_")
					.replace("'", "").replace("(\\W|^_)*", "_").toLowerCase();
			groupConcat.append("group_concat(if(a.attribute_type_id = "
					+ typeId + ", a.value_reference, null)) as " + typeName + ", ");
		}
		groupConcat.append("'' as BLANK ");
		query = new StringBuilder("create table location_attribute_merged ");
		query.append("select a.implementation_id, a.location_id, ");
		query.append(groupConcat.toString());
		query.append("from location_attribute as a ");
		query.append("where a.voided = 0 and a.implementation_id = '" + implementationId + "' ");
		query.append("group by a.location_id");
		db.runCommand(CommandType.CREATE, query.toString());
		db.runCommand(CommandType.ALTER, "alter table location_attribute_merged add primary key (implementation_id, location_id)");
		/* Dear reader! Kindly don't judge my coding skills based on the lines below. I'm not proud of this mess :-| */
		String[] columnList;
		StringBuilder columns = new StringBuilder();
		String aliasPrefix = "lam";
		try {
			// Fetch list of columns in newly created table
			columnList = db.getColumnNames("location_attribute_merged");
			ArrayList<String> dimColumns = CollectionsUtil.toArrayList(db.getColumnNames("dim_location"));
			for (int i = 2; i < columnList.length - 1; i++) { // Skipping undesired columns
				columns.append(aliasPrefix + ".");
				columns.append(columnList[i] +  ",");
				// Additionally, hunt for missing columns in dim_location and create any missing ones
				if (!dimColumns.contains(columnList[i])) {
					log.info("Creating missing column " + columnList[i] + " in dim_location.");
					db.addColumn("dim_location", columnList[i], "VARCHAR(255)");
				}
			}
			columns.deleteCharAt(columns.lastIndexOf(","));
		} catch (SQLException e) {
			e.printStackTrace();
		}
		query = new StringBuilder(
				"insert ignore into dim_location (surrogate_key, implementation_id, location_id, location_name, description, address1, address2, city_village, state_province, postal_code, country, latitude, longitude, creator, date_created, retired, parent_location, uuid, " + columns.toString().replace(aliasPrefix + ".", "") + ") ");
		query.append("select l.surrogate_key, l.implementation_id, l.location_id, l.name as location_name, l.description, l.address1, l.address2, l.city_village, l.state_province, l.postal_code, l.country, l.latitude, l.longitude, l.creator, l.date_created, l.retired, l.parent_location, l.uuid, " + columns + " from location as l ");
		query.append("left outer join location_attribute_merged as lam using (implementation_id, location_id) ");
		query.append("where l.implementation_id = '" + implementationId + "' ");
		query.append("and l.surrogate_key not in (select surrogate_key from dim_location where implementation_id = l.implementation_id)");
		log.info("Inserting new locations to dimension.");
		db.runCommand(CommandType.INSERT, query.toString());
	}

	/**
	 * Fill in user dimension table
	 * 
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 */
	public void userDimension(Date from, Date to, int implementationId) throws InstantiationException,
			IllegalAccessException, ClassNotFoundException {
		StringBuilder query = new StringBuilder(
				"insert ignore into dim_user (surrogate_key, implementation_id, user_id, username, person_id, identifier, secret_question, secret_answer, creator, date_created, changed_by, date_changed, retired, retire_reason, uuid) ");
		query.append("select u.surrogate_key, u.implementation_id, u.user_id, u.username, u.person_id, p.identifier, u.secret_question, u.secret_answer, u.creator, u.date_created, u.changed_by, u.date_changed, u.retired, u.retire_reason, u.uuid from users as u ");
		query.append("left outer join provider as p on p.implementation_id = u.implementation_id and p.person_id = u.person_id");
		log.info("Inserting new users to dimension.");
		db.runCommand(CommandType.INSERT, query.toString());
	}

	/**
	 * Prepare patient data and fill in Patient dimension
	 * 
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 */
	public void patientDimension(Date from, Date to, int implementationId) throws InstantiationException,
			IllegalAccessException, ClassNotFoundException {
		// Collect latest patient identifiers
		log.info("Setting preferred identifiers to OpenMRS ID (with check digit).");
		db.runCommand(CommandType.UPDATE,
				"update patient_identifier set preferred = 1 where identifier_type = 3");
		log.info("Setting identifiers other than OpenMRS ID to non-preferred.");
		db.runCommand(CommandType.UPDATE,
				"update patient_identifier set preferred = 0 where identifier_type <> 3");
		log.info("Selecting patient identifiers.");
		db.runCommand(CommandType.DROP,
				"drop table if exists patient_latest_identifier");
		StringBuilder query = new StringBuilder(
				"create table patient_latest_identifier ");
		query.append("select * from patient_identifier as a having a.patient_id = (select max(patient_id) from patient_identifier where implementation_id = a.implementation_id and patient_id = a.patient_id and preferred = 1 and voided = 0) union ");
		query.append("select * from patient_identifier as a having a.patient_id = (select max(patient_id) from patient_identifier where implementation_id = a.implementation_id and patient_id = a.patient_id and preferred = 0 and voided = 0)");
		db.runCommand(CommandType.CREATE, query.toString());
		db.runCommand(
				CommandType.ALTER,
				"alter table patient_latest_identifier add primary key surrogate_key (surrogate_key)");

		// Collect latest person names
		log.info("Selecting people names (preferred/latest).");
		db.runCommand(CommandType.DROP,
				"drop table if exists person_latest_name");
		query = new StringBuilder("create table person_latest_name ");
		query.append("select * from person_name as a where a.person_name_id = (select max(person_name_id) from person_name where implementation_id = a.implementation_id and person_id = a.person_id and preferred = 1)");
		db.runCommand(CommandType.CREATE, query.toString());
		query = new StringBuilder("insert into person_latest_name ");
		query.append("select * from person_name as a where a.person_name_id = (select max(person_name_id) from person_name where implementation_id = a.implementation_id and person_id = a.person_id and preferred = 0)");
		db.runCommand(CommandType.INSERT, query.toString());
		db.runCommand(CommandType.ALTER,
				"alter table person_latest_name add primary key surrogate_key (surrogate_key)");

		// Collect latest person addresses
		log.info("Selecting people addresses (preferred/latest).");
		db.runCommand(CommandType.DROP,
				"drop table if exists person_latest_address");
		query = new StringBuilder("create table person_latest_address ");
		query.append("select * from person_address as a where a.person_address_id = (select max(person_address_id) from person_address where implementation_id = a.implementation_id and person_id = a.person_id and preferred = 1)");
		db.runCommand(CommandType.CREATE, query.toString());
		query = new StringBuilder("insert into person_latest_address ");
		query.append("select * from person_address as a where a.person_address_id = (select max(person_address_id) from person_address where implementation_id = a.implementation_id and person_id = a.person_id and preferred = 0)");
		db.runCommand(CommandType.INSERT, query.toString());
		db.runCommand(CommandType.ALTER,
				"alter table person_latest_address add primary key surrogate_key (surrogate_key)");

		// Recreate person attributes
		log.info("Transforming people attribute.");
		db.runCommand(CommandType.DROP,
				"drop table if exists person_attribute_merged");
		Object[][] attributeTypes = db.getTableData(
				"person_attribute_type", "person_attribute_type_id,name", null,
				true);
		StringBuilder groupConcat = new StringBuilder();
		for (Object[] type : attributeTypes) {
			String typeId = type[0].toString();
			String typeName = type[1].toString().replace(" ", "_")
					.replace("'", "").replace("(\\W|^_)*", "_").toLowerCase();
			groupConcat.append("group_concat(if(a.person_attribute_type_id = "
					+ typeId + ", a.value, null)) as " + typeName + ", ");
		}
		groupConcat.append("'' as BLANK ");
		query = new StringBuilder("create table person_attribute_merged ");
		query.append("select a.implementation_id, a.person_id, a.creator, a.date_created, a.uuid, ");
		query.append(groupConcat.toString());
		query.append("from person_attribute as a where a.voided = 0 ");
		query.append("group by a.implementation_id, a.person_id");
		db.runCommand(CommandType.CREATE, query.toString());
		db.runCommand(
				CommandType.ALTER,
				"alter table person_attribute_merged add primary key (implementation_id, person_id)");

		// Fill the patient dimension data
		query = new StringBuilder(
				"insert ignore into dim_patient (surrogate_key, implementation_id, patient_id, identifier, secondary_identifier, gender, birthdate, birthdate_estimated, dead, first_name, middle_name, last_name, race, birthplace, citizenship, mother_name, civil_status, health_district, health_center, primary_mobile, secondary_mobile, primary_phone, secondary_phone, primary_mobile_owner, secondary_mobile_owner, primary_phone_owner, secondary_phone_owner, address1, address2, city_village, state_province, postal_code, country, creator, date_created, changed_by, date_changed, voided, uuid) ");
		query.append("select p.surrogate_key, p.implementation_id, p.patient_id, i.identifier, pr.gender, pr.birthdate, pr.birthdate_estimated, pr.dead, n.given_name as first_name, n.middle_name, n.family_name as last_name, pa.*, a.address1, a.address2, a.city_village, a.state_province, a.postal_code, a.country, p.creator, p.date_created, p.changed_by, p.date_changed, p.voided, pr.uuid from patient as p ");
		query.append("inner join person as pr on pr.implementation_id = p.implementation_id and pr.person_id = p.patient_id ");
		query.append("left outer join patient_latest_identifier as i on i.implementation_id = p.implementation_id and i.patient_id = p.patient_id ");
		query.append("left outer join person_latest_name as n on n.implementation_id = p.implementation_id and n.person_id = pr.person_id ");
		query.append("left outer join person_latest_address as a on a.implementation_id = p.implementation_id and a.person_id = p.patient_id ");
		query.append("left outer join person_attribute_merged as pa on pa.implementation_id = p.implementation_id and pa.person_id = p.patient_id ");
		query.append("where p.voided = 0");
		log.info("Inserting new patients to dimension.");
		db.runCommand(CommandType.INSERT, query.toString());
	}

	/**
	 * Fill in encounters and observations in respective dimensions
	 * 
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 */
	public void encounterAndObsDimension(Date from, Date to, int implementationId) throws InstantiationException,
			IllegalAccessException, ClassNotFoundException {
		// Fill the encounter dimension data
		StringBuilder query = new StringBuilder(
				"insert ignore into dim_encounter ");
		query.append("select e.surrogate_key, e.implementation_id, e.encounter_id, e.encounter_type, et.name as encounter_name, et.description, e.patient_id, e.location_id, p.identifier as provider, e.encounter_datetime as date_entered, e.creator, e.date_created as date_start, e.changed_by, e.date_changed, e.date_created as date_end, e.uuid from encounter as e ");
		query.append("inner join encounter_type as et on et.encounter_type_id = e.encounter_type ");
		query.append("left outer join encounter_provider as ep on ep.encounter_id = e.encounter_id ");
		query.append("left outer join provider as p on p.person_id = ep.provider_id ");
		query.append("where e.voided = 0");
		log.info("Inserting new encounters to dimension.");
		db.runCommand(CommandType.INSERT, query.toString());

		// Fill the observation dimension data
		query = new StringBuilder("insert ignore into dim_obs ");
		query.append("select e.surrogate_key, e.implementation_id, e.encounter_id, e.encounter_type, e.patient_id, p.identifier, e.provider, o.obs_id, o.concept_id, c.concept as question, obs_datetime, o.location_id, concat(ifnull(o.value_boolean, ''), ifnull(ifnull(c2.concept, c2.full_name), ''), ifnull(o.value_datetime, ''), ifnull(o.value_numeric, ''), ifnull(o.value_text, '')) as answer, o.value_boolean, o.value_coded, o.value_datetime, o.value_numeric, o.value_text, o.creator, o.date_created, o.voided, o.uuid from obs as o ");
		query.append("inner join dim_concept as c on c.implementation_id = o.implementation_id and c.concept_id = o.concept_id ");
		query.append("inner join dim_encounter as e on e.implementation_id = o.implementation_id and e.encounter_id = o.encounter_id ");
		query.append("inner join dim_patient as p on p.implementation_id = e.implementation_id and p.patient_id = e.patient_id ");
		query.append("left outer join dim_concept as c2 on c2.implementation_id = o.implementation_id and c2.concept_id = o.value_coded ");
		query.append("where o.voided = 0");
		log.info("Inserting new observations to dimension.");
		db.runCommand(CommandType.INSERT, query.toString());
	}
}
