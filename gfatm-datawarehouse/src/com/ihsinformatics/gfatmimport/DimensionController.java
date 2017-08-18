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

/**
 * @author owais.hussain@ihsinformatics.com
 *
 */
public class DimensionController {

    private static final Logger log = Logger.getLogger(Class.class.getName());
    private DatabaseUtil db;

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
	Object[][] sources = db.getTableData("_implementation",
		"implementation_id", "active=1 AND status='STOPPED' "
			+ "AND date(last_updated) = current_date()");
	// For each source, import all data
	// TODO: Restrict by date
	for (Object[] source : sources) {
	    int implementationId = Integer.parseInt(source[0].toString());
	    modelDimensions(from.getTime(), to.getTime(), implementationId);
	}
    }

    public void modelDimensions(Date from, Date to, int implementationId) {
	log.info("Starting dimension modeling");
	try {
	    timeDimension();
	} catch (Exception e) {
	    e.printStackTrace();
	}
	try {
	    conceptDimension(from, to, implementationId);
	} catch (Exception e) {
	    e.printStackTrace();
	}
	try {
	    locationDimension(from, to, implementationId);
	} catch (Exception e) {
	    e.printStackTrace();
	}
	try {
	    userDimension(from, to, implementationId);
	} catch (Exception e) {
	    e.printStackTrace();
	}
	try {
	    userDimension(from, to, implementationId);
	} catch (Exception e) {
	    e.printStackTrace();
	}
	try {
	    patientDimension(from, to, implementationId);
	} catch (Exception e) {
	    e.printStackTrace();
	}
	try {
	    encounterAndObsDimension(from, to, implementationId);
	} catch (Exception e) {
	    e.printStackTrace();
	}
	log.info("Finished dimension modeling");
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
    public void conceptDimension(Date from, Date to, int implementationId)
	    throws InstantiationException, IllegalAccessException,
	    ClassNotFoundException {
	// First, remove existing concepts for this implementation
	String deleteQuery = "delete from dim_concept where implementation_id = "
		+ implementationId;
	log.info("Deleting existing concepts.");
	db.runCommand(CommandType.DELETE, deleteQuery);

	StringBuilder query = new StringBuilder();
	// Creating table for relevant concept names
	log.info("Selecting concept names (preferred/latest).");
	db.runCommand(CommandType.DROP,
		"drop table if exists concept_latest_name");
	query = new StringBuilder("create table concept_latest_name ");
	query.append("select c.implementation_id, c.concept_id, ");
	query.append("(select max(concept_name_id) from concept_name where implementation_id = c.implementation_id and concept_id = c.concept_id and locale = 'en' and voided = 0 and concept_name_type is null) as default_name, ");
	query.append("(select max(concept_name_id) from concept_name where implementation_id = c.implementation_id and concept_id = c.concept_id and locale = 'en' and voided = 0 and concept_name_type = 'SHORT') as short_name, ");
	query.append("(select max(concept_name_id) from concept_name where implementation_id = c.implementation_id and concept_id = c.concept_id and locale = 'en' and voided = 0 and concept_name_type = 'FULLY_SPECIFIED') as full_name from concept as c ");
	query.append("having concat(ifnull(default_name, ''), ifnull(short_name, ''), ifnull(full_name, '')) <> '' ");
	db.runCommand(CommandType.CREATE, query.toString());

	db.runCommand(
		CommandType.ALTER,
		"alter table concept_latest_name add primary key composite_id (implementation_id, concept_id)");
	// Fill in concept dimension data
	query = new StringBuilder(
		"insert ignore into dim_concept (surrogate_id, implementation_id, concept_id, full_name, short_name, default_name, description, retired, data_type, class, hi_absolute, hi_critical, hi_normal, low_absolute, low_critical, low_normal, creator, date_created, version, changed_by, date_changed, uuid) ");
	query.append("select c.surrogate_id, c.implementation_id, c.concept_id, cnf.name as full_name, cns.name as short_name, cnd.name as default_name, d.description, c.retired, dt.name as data_type, cl.name as class, cn.hi_absolute, cn.hi_critical, cn.hi_normal, cn.low_absolute, cn.low_critical, cn.low_normal, c.creator, c.date_created, c.version, c.changed_by, c.date_changed, c.uuid from concept as c ");
	query.append("left outer join concept_datatype as dt on dt.implementation_id = c.implementation_id and dt.concept_datatype_id = c.datatype_id ");
	query.append("left outer join concept_class as cl on cl.implementation_id = c.implementation_id and cl.concept_class_id = c.class_id ");
	query.append("inner join concept_latest_name as nm on nm.implementation_id = c.implementation_id and nm.concept_id = c.concept_id ");
	query.append("left outer join concept_name as cnf on cnf.implementation_id = c.implementation_id and cnf.concept_name_id = nm.full_name ");
	query.append("left outer join concept_name as cns on cns.implementation_id = c.implementation_id and cns.concept_name_id = nm.short_name ");
	query.append("left outer join concept_name as cnd on cnd.implementation_id = c.implementation_id and cnd.concept_name_id = nm.default_name ");
	query.append("left outer join concept_description as d on d.implementation_id = c.implementation_id and d.concept_id = c.concept_id and d.locale = 'en' ");
	query.append("left outer join concept_numeric as cn on cn.implementation_id = c.implementation_id and cn.concept_id = c.concept_id ");
	query.append("where c.implementation_id = '" + implementationId + "' ");
	query.append("and c.surrogate_id not in (select surrogate_id from dim_concept where implementation_id = c.implementation_id)");
	log.info("Inserting new concepts to dimension.");
	db.runCommand(CommandType.INSERT, query.toString());
	query = new StringBuilder(
		"update dim_concept set full_name = 'Yes', short_name = 'Yes', default_name = 'Yes' where concept_id = 1");
	log.info("Setting names of Yes/No concepts.");
	db.runCommand(CommandType.UPDATE, query.toString());
	query = new StringBuilder(
		"update dim_concept set full_name = 'No', short_name = 'No', default_name = 'No' where concept_id = 2");
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
    public void locationDimension(Date from, Date to, int implementationId)
	    throws InstantiationException, IllegalAccessException,
	    ClassNotFoundException {
	StringBuilder query = new StringBuilder(
		"delete from dim_location where implementation_id = "
			+ implementationId);
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
	Object[][] attributeTypes = db.getTableData("location_attribute_type",
		"location_attribute_type_id,name", null, true);
	StringBuilder groupConcat = new StringBuilder();
	for (Object[] type : attributeTypes) {
	    String typeId = type[0].toString();
	    String typeName = type[1].toString().replace(" ", "_")
		    .replace("'", "").replace("(\\W|^_)*", "_").toLowerCase();
	    groupConcat.append("group_concat(if(a.attribute_type_id = "
		    + typeId + ", a.value_reference, null)) as " + typeName
		    + ", ");
	}
	groupConcat.append("'' as BLANK ");
	query = new StringBuilder("create table location_attribute_merged ");
	query.append("select a.implementation_id, a.location_id, ");
	query.append(groupConcat.toString());
	query.append("from location_attribute as a ");
	query.append("where a.voided = 0 and a.implementation_id = '"
		+ implementationId + "' ");
	query.append("group by a.location_id");
	db.runCommand(CommandType.CREATE, query.toString());
	db.runCommand(
		CommandType.ALTER,
		"alter table location_attribute_merged add primary key (implementation_id, location_id)");
	/*
	 * Dear reader! Kindly don't judge my coding skills based on the lines
	 * below. I'm not proud of this mess :-|
	 */
	String[] columnList;
	StringBuilder columns = new StringBuilder();
	String aliasPrefix = "lam";
	try {
	    // Fetch list of columns in newly created table
	    columnList = db.getColumnNames("location_attribute_merged");
	    ArrayList<String> dimColumns = CollectionsUtil.toArrayList(db
		    .getColumnNames("dim_location"));
	    for (int i = 2; i < columnList.length - 1; i++) { // Skipping
							      // undesired
							      // columns
		columns.append(aliasPrefix + ".");
		columns.append(columnList[i] + ",");
		// Additionally, hunt for missing columns in dim_location and
		// create any missing ones
		if (!dimColumns.contains(columnList[i])) {
		    log.info("Creating missing column " + columnList[i]
			    + " in dim_location.");
		    db.addColumn("dim_location", columnList[i], "VARCHAR(255)");
		}
	    }
	    columns.deleteCharAt(columns.lastIndexOf(","));
	} catch (SQLException e) {
	    e.printStackTrace();
	}
	query = new StringBuilder(
		"insert ignore into dim_location (surrogate_id, implementation_id, location_id, location_name, description, address1, address2, city_village, state_province, postal_code, country, latitude, longitude, creator, date_created, retired, parent_location, uuid, "
			+ columns.toString().replace(aliasPrefix + ".", "")
			+ ") ");
	query.append("select l.surrogate_id, l.implementation_id, l.location_id, l.name as location_name, l.description, l.address1, l.address2, l.city_village, l.state_province, l.postal_code, l.country, l.latitude, l.longitude, l.creator, l.date_created, l.retired, l.parent_location, l.uuid, "
		+ columns + " from location as l ");
	query.append("left outer join location_attribute_merged as lam using (implementation_id, location_id) ");
	query.append("where l.implementation_id = '" + implementationId + "' ");
	query.append("and l.surrogate_id not in (select surrogate_id from dim_location where implementation_id = l.implementation_id)");
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
    public void userDimension(Date from, Date to, int implementationId)
	    throws InstantiationException, IllegalAccessException,
	    ClassNotFoundException {
	log.info("Transforming user roles.");
	db.runCommand(CommandType.DROP, "drop table if exists user_role_merged");
	Object[][] roles = db.getTableData("role", "role", null, true);
	StringBuilder groupConcat = new StringBuilder();
	for (Object[] role : roles) {
	    String roleName = role[0].toString().replace(" ", "_")
		    .replace("'", "").replace("(\\W|^_)*", "_").toLowerCase();
	    groupConcat.append("group_concat(if(a.role = '"
		    + role[0].toString() + "', 'Yes', null)) as " + roleName
		    + ", ");
	}
	groupConcat.append("'' as BLANK ");
	StringBuilder query = new StringBuilder(
		"create table user_role_merged ");
	query.append("select a.implementation_id, a.user_id, ");
	query.append(groupConcat.toString());
	query.append("from user_role as a ");
	query.append("where a.implementation_id = '" + implementationId + "' ");
	query.append("group by a.user_id");
	db.runCommand(CommandType.CREATE, query.toString());
	db.runCommand(CommandType.ALTER,
		"alter table user_role_merged add primary key (implementation_id, user_id)");
	/* Again, not the best example of my code */
	String[] columnList;
	StringBuilder columns = new StringBuilder();
	String aliasPrefix = "urm";
	try {
	    // Fetch list of columns in newly created table
	    columnList = db.getColumnNames("user_role_merged");
	    ArrayList<String> dimColumns = CollectionsUtil.toArrayList(db
		    .getColumnNames("dim_user"));
	    for (int i = 2; i < columnList.length - 1; i++) { // Skipping
							      // undesired
							      // columns
		columns.append(aliasPrefix + ".");
		columns.append(columnList[i] + ",");
		// Additionally, hunt for missing columns in dim_location and
		// create any missing ones
		if (!dimColumns.contains(columnList[i])) {
		    log.info("Creating missing column " + columnList[i]
			    + " in dim_user.");
		    db.addColumn("dim_user", columnList[i], "VARCHAR(255)");
		}
	    }
	    columns.deleteCharAt(columns.lastIndexOf(","));
	} catch (SQLException e) {
	    e.printStackTrace();
	}
	query = new StringBuilder(
		"insert ignore into dim_user (surrogate_id, implementation_id, user_id, username, person_id, identifier, secret_question, secret_answer, creator, date_created, changed_by, date_changed, retired, retire_reason, uuid, "
			+ columns.toString().replace(aliasPrefix + ".", "")
			+ ") ");
	query.append("select u.surrogate_id, u.implementation_id, u.user_id, u.username, u.person_id, p.identifier, u.secret_question, pa1.value_reference as intervention, u.creator, u.date_created, u.changed_by, u.date_changed, u.retired, u.retire_reason, u.uuid, "
		+ columns + " from users as u ");
	query.append("left outer join provider as p on p.implementation_id = u.implementation_id and p.person_id = u.person_id ");
	query.append("left outer join provider_attribute as pa1 on pa1.implementation_id = u.implementation_id and pa1.provider_id = p.provider_id and pa1.attribute_type_id = 1 ");
	query.append("left outer join user_role_merged as urm on urm.implementation_id = u.implementation_id and urm.user_id = u.user_id ");
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
    public void patientDimension(Date from, Date to, int implementationId)
	    throws InstantiationException, IllegalAccessException,
	    ClassNotFoundException {
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
		"alter table patient_latest_identifier add primary key surrogate_id (surrogate_id)");

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
		"alter table person_latest_name add primary key surrogate_id (surrogate_id)");

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
		"alter table person_latest_address add primary key surrogate_id (surrogate_id)");

	// Recreate person attributes
	log.info("Transforming people attribute.");
	db.runCommand(CommandType.DROP,
		"drop table if exists person_attribute_merged");
	Object[][] attributeTypes = db.getTableData("person_attribute_type",
		"person_attribute_type_id,name", null, true);
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
	query.append("select a.implementation_id, a.person_id, ");
	query.append(groupConcat.toString());
	query.append("from person_attribute as a where a.voided = 0 ");
	query.append("group by a.implementation_id, a.person_id");
	db.runCommand(CommandType.CREATE, query.toString());
	db.runCommand(
		CommandType.ALTER,
		"alter table person_attribute_merged add primary key (implementation_id, person_id)");
	/* Repeat ... repeat */
	String[] columnList;
	StringBuilder columns = new StringBuilder();
	String aliasPrefix = "pam";
	try {
	    // Fetch list of columns in newly created table
	    columnList = db.getColumnNames("person_attribute_merged");
	    ArrayList<String> dimColumns = CollectionsUtil.toArrayList(db
		    .getColumnNames("dim_patient"));
	    for (int i = 2; i < columnList.length - 1; i++) { // Skipping
							      // undesired
							      // columns
		columns.append(aliasPrefix + ".");
		columns.append(columnList[i] + ",");
		// Additionally, hunt for missing columns in dim_location and
		// create any missing ones
		if (!dimColumns.contains(columnList[i])) {
		    log.info("Creating missing column " + columnList[i]
			    + " in dim_patient.");
		    db.addColumn("dim_patient", columnList[i], "VARCHAR(255)");
		}
	    }
	    columns.deleteCharAt(columns.lastIndexOf(","));
	} catch (SQLException e) {
	    e.printStackTrace();
	}

	// Fill the patient dimension data
	query = new StringBuilder(
		"insert ignore into dim_patient (surrogate_id, implementation_id, patient_id, patient_identifier, enrs, external_id, gender, birthdate, birthdate_estimated, dead, first_name, middle_name, last_name, address1, address2, city_village, state_province, postal_code, country, creator, date_created, changed_by, date_changed, voided, uuid, "
			+ columns.toString().replace(aliasPrefix + ".", "")
			+ ") ");
	query.append("select p.surrogate_id, p.implementation_id, p.patient_id, pid.identifier as patient_identifier, enrs.identifier as enrs, eid.identifier as external_id, pr.gender, pr.birthdate, pr.birthdate_estimated, pr.dead, n.given_name as first_name, n.middle_name, n.family_name as last_name, a.address1, a.address2, a.city_village, a.state_province, a.postal_code, a.country, p.creator, p.date_created, p.changed_by, p.date_changed, p.voided, pr.uuid, "
		+ columns + " from patient as p ");
	query.append("inner join person as pr on pr.implementation_id = p.implementation_id and pr.person_id = p.patient_id ");
	query.append("inner join patient_latest_identifier as pid on pid.implementation_id = p.implementation_id and pid.patient_id = p.patient_id and pid.identifier_type = 3 ");
	query.append("left outer join patient_latest_identifier as enrs on enrs.implementation_id = p.implementation_id and enrs.patient_id = p.patient_id and enrs.identifier_type = 4 ");
	query.append("left outer join patient_latest_identifier as eid on eid.implementation_id = p.implementation_id and eid.patient_id = p.patient_id and eid.identifier_type = 5 ");
	query.append("inner join person_latest_name as n on n.implementation_id = p.implementation_id and n.person_id = pr.person_id ");
	query.append("left outer join person_latest_address as a on a.implementation_id = p.implementation_id and a.person_id = p.patient_id ");
	query.append("left outer join person_attribute_merged as pam on pam.implementation_id = p.implementation_id and pam.person_id = p.patient_id ");
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
    public void encounterAndObsDimension(Date from, Date to,
	    int implementationId) throws InstantiationException,
	    IllegalAccessException, ClassNotFoundException {
	// Fill the encounter dimension data
	StringBuilder query = new StringBuilder(
		"insert ignore into dim_encounter ");
	query.append("select e.surrogate_id, e.implementation_id, e.encounter_id, e.encounter_type, et.name as encounter_name, et.description, e.patient_id, e.location_id, p.identifier as provider, e.encounter_datetime as date_entered, e.creator, e.date_created as date_start, e.changed_by, e.date_changed, e.date_created as date_end, e.uuid from encounter as e ");
	query.append("inner join encounter_type as et on et.encounter_type_id = e.encounter_type ");
	query.append("left outer join encounter_provider as ep on ep.encounter_id = e.encounter_id ");
	query.append("left outer join provider as p on p.person_id = ep.provider_id ");
	StringBuilder filter = new StringBuilder(" where e.voided = 0 ");
	filter.append("and (e.date_created between timestamp('"
		+ DateTimeUtil.getSqlDateTime(from) + "') ");
	filter.append("and timestamp('" + DateTimeUtil.getSqlDateTime(to)
		+ "')) ");
	filter.append(" or (e.date_changed between timestamp('"
		+ DateTimeUtil.getSqlDateTime(from) + "') ");
	filter.append("and timestamp('" + DateTimeUtil.getSqlDateTime(to)
		+ "')) ");
	query.append(filter.toString());
	log.info("Inserting new encounters to dimension.");
	db.runCommand(CommandType.INSERT, query.toString());

	// Fill the observation dimension data
	filter = new StringBuilder(
		" where o.voided = 0 and o.previous_version is null ");
	filter.append("and (o.date_created between timestamp('"
		+ DateTimeUtil.getSqlDateTime(from) + "') ");
	filter.append("and timestamp('" + DateTimeUtil.getSqlDateTime(to)
		+ "')) ");
	query = new StringBuilder("insert ignore into dim_obs ");
	query.append("select o.surrogate_id, o.implementation_id, e.encounter_id, e.encounter_type, e.patient_id, p.patient_identifier, e.provider, o.obs_id, o.obs_group_id, o.concept_id, c.short_name as question, obs_datetime, o.location_id, concat(ifnull(ifnull(ifnull(c2.short_name, c2.default_name), c2.full_name), ''), ifnull(o.value_boolean, ''), ifnull(o.value_datetime, ''), ifnull(o.value_numeric, ''), ifnull(o.value_text, '')) as answer, o.value_boolean, o.value_coded, o.value_datetime, o.value_numeric, o.value_text, o.creator, o.date_created, o.voided, o.uuid from obs as o ");
	query.append("inner join dim_concept as c on c.implementation_id = o.implementation_id and c.concept_id = o.concept_id ");
	query.append("inner join dim_encounter as e on e.implementation_id = o.implementation_id and e.encounter_id = o.encounter_id ");
	query.append("inner join dim_patient as p on p.implementation_id = e.implementation_id and p.patient_id = e.patient_id ");
	query.append("left outer join dim_concept as c2 on c2.implementation_id = o.implementation_id and c2.concept_id = o.value_coded ");
	query.append(filter.toString());
	log.info("Inserting new observations to dimension.");
	db.runCommand(CommandType.INSERT, query.toString());

	// Fill multi-select options as comma-separated in parent obs
	log.info("Note! Multi-select answers are saved in parent observation as comma-separated values");
	db.runCommand(CommandType.DROP, "drop table if exists tmp_group_obs");
	query = new StringBuilder("create table tmp_group_obs ");
	query.append("select implementation_id, encounter_type, obs_group_id, question, group_concat(case answer when '' then null else answer end) as answer from dim_obs ");
	query.append("where obs_group_id is not null ");
	query.append("group by implementation_id, obs_group_id, encounter_type ");
	query.append("having answer is not null");
	db.runCommand(CommandType.CREATE, query.toString());
	query = new StringBuilder("update dim_obs as o, tmp_group_obs as t ");
	query.append("set o.answer = t.answer ");
	query.append("where o.implementation_id = t.implementation_id and o.obs_id = t.obs_group_id");
	db.runCommand(CommandType.UPDATE, query.toString());
	deencounterize();
    }

    /**
     * Transforms the encounters and observations into separate tables
     */
    public void deencounterize() {
	// Create a temporary table to save questions for each encounter type
	db.runCommand(CommandType.DROP, "drop table if exists tmp");
	db.runCommand(
		CommandType.CREATE,
		"create table tmp select distinct encounter_type, concept_id, question from dim_obs");
	// Fetch encounter types and names
	Object[][] encounterTypes = db.getTableData("dim_encounter",
		"distinct encounter_type, encounter_name", null);
	if (encounterTypes == null) {
	    log.severe("Encounter types could not be fetched");
	    return;
	}
	for (Object[] encounterType : encounterTypes) {
	    StringBuilder query = new StringBuilder();
	    // Create a de-encounterized table
	    Object[][] data = db.getTableData("tmp", "question",
		    "encounter_type=" + encounterType[0].toString());
	    ArrayList<String> elements = new ArrayList<String>();
	    for (int i = 0; i < data.length; i++) {
		if (data[i][0] == null) {
		    continue;
		}
		elements.add(data[i][0].toString());
	    }
	    StringBuilder groupConcat = new StringBuilder();
	    for (Object element : elements) {
		String str = element.toString().replaceAll("[^A-Za-z0-9]", "_")
			.toLowerCase();
		groupConcat.append("group_concat(if(o.question = '" + element
			+ "', o.answer, NULL)) AS " + str + ", ");
	    }
	    String encounterName = encounterType[1].toString().toLowerCase()
		    .replace(" ", "_").replace("-", "_");
	    query.append("create table enc_" + encounterName + " ");
	    query.append("select e.surrogate_id, e.implementation_id, e.encounter_id,  e.provider, e.location_id, l.location_name, e.patient_id, e.date_entered, ");
	    query.append(groupConcat.toString());
	    query.append("'' as BLANK from dim_encounter as e ");
	    query.append("inner join dim_obs as o on o.encounter_id = e.encounter_id and o.voided = 0 ");
	    query.append("inner join dim_location as l on l.location_id = e.location_id ");
	    query.append("where e.encounter_type = '"
		    + encounterType[0].toString() + "' ");
	    // Filter out all child observations (e.g. multi-select)
	    query.append("and o.obs_group_id is null ");
	    query.append("group by e.surrogate_id, e.implementation_id, e.encounter_id, e.patient_id, e.provider, e.location_id, l.location_name, e.patient_id, e.date_entered");
	    // Drop previous table
	    db.runCommand(CommandType.DROP, "drop table if exists enc_"
		    + encounterName);
	    log.info("Generating table for " + encounterType[1].toString());
	    // Insert new data
	    Object result = db.runCommand(CommandType.CREATE, query.toString());
	    if (result == null) {
		log.warning("No data imported for Encounter "
			+ encounterType[1].toString());
	    }
	    // Creating Primary key
	    db.runCommand(CommandType.ALTER, "alter table enc_" + encounterName
		    + " add primary key surrogate_id (surrogate_id)");
	}
    }
}
