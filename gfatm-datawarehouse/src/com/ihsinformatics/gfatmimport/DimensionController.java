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
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

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
		Object[][] sources = db.getTableData("_implementation", "implementation_id", "active=1 AND status='RUNNING'");
		// For each source, model dimensions
		// TODO: Restrict by date
		for (Object[] source : sources) {
			int implementationId = Integer.parseInt(source[0].toString());
			modelDimensions(from.getTime(), to.getTime(), implementationId);
		}
	}

	public void modelDimensions(Date from, Date to, int implementationId) {
		try {
			log.info("Creating time dimension");
			timeDimension();
		} catch (Exception e) {
			log.warning(e.getMessage());
		}
		try {
			log.info("Creating concept dimension");
			conceptDimension(from, to, implementationId);
		} catch (Exception e) {
			log.warning(e.getMessage());
		}
		try {
			log.info("Creating location dimension");
			locationDimension(from, to, implementationId);
		} catch (Exception e) {
			log.warning(e.getMessage());
		}
		try {
			log.info("Creating user dimension");
			userDimension(from, to, implementationId);
		} catch (Exception e) {
			log.warning(e.getMessage());
		}
		try {
			log.info("Creating patient dimension");
			patientDimension(from, to, implementationId);
		} catch (Exception e) {
			log.warning(e.getMessage());
		}
		try {
			log.info("Creating encounter and observation dimensions");
			encounterAndObsDimension(from, to, implementationId);
		} catch (Exception e) {
			log.warning(e.getMessage());
		}
		try {
			log.info("Creating user form and result dimensions");
			userFormAndResultDimension(from, to, implementationId);
		} catch (Exception e) {
			log.warning(e.getMessage());
		}
		try {
			log.info("Creating user form and result dimensions");
			userFormAndResultDimension(from, to, implementationId);
		} catch (Exception e) {
			log.warning(e.getMessage());
		}
		try {
			log.info("Creating lab result dimensions");
			commonLabResultDimension(from, to, implementationId);
		} catch (Exception e) {
			log.warning(e.getMessage());
		}
		try {
			log.info("Starting deencounterizing process");
			deencounterizeOpenMrs();
			deencounterizeGfatm();
			denormalizeOpenMrsExtended();
			log.info("Deencounterizing process complete");
		} catch (Exception e) {
			log.warning(e.getMessage());
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
	public void timeDimension()
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, ParseException {
		Object lastSqlDate = db.runCommand(CommandType.SELECT, "select max(full_date) as latest from dim_datetime");
		Calendar start = Calendar.getInstance();
		start.set(Calendar.YEAR, 2000);
		start.set(Calendar.MONTH, Calendar.JANUARY);
		start.set(Calendar.DATE, 1);
		if (lastSqlDate != null) {
			Date latestDate = DateTimeUtil.fromSqlDateString(lastSqlDate.toString());
			start.setTime(latestDate);
		}
		start.add(Calendar.DATE, 1);
		Calendar end = Calendar.getInstance();
		end.set(Calendar.HOUR, 0);
		if (!start.getTime().before(end.getTime())) {
			return;
		}
		StringBuilder query = new StringBuilder("insert into dim_datetime values ");
		while (start.getTime().before(end.getTime())) {
			String sqlDate = "'" + DateTimeUtil.toSqlDateString(start.getTime()) + "'";
			query.append("(0, " + sqlDate + ", ");
			query.append("year(" + sqlDate + "), ");
			query.append("month(" + sqlDate + "), ");
			query.append("day(" + sqlDate + "), ");
			query.append("dayname(" + sqlDate + "), ");
			query.append("monthname(" + sqlDate + ")),");
			start.add(Calendar.DATE, 1);
		}
		query.setCharAt(query.length() - 1, ';');
		log.info("Executing: " + query.toString());
		db.runCommand(CommandType.INSERT, query.toString());
	}

	/**
	 * Fill in Concept dimension table
	 * 
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public void conceptDimension(Date from, Date to, int implementationId)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		log.info("Transforming concept attributes.");
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("impl_id", implementationId);
		db.runStoredProcedure("concept_dimension", params);
		log.info("Concept transformation complete.");
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
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		log.info("Transforming location attributes.");
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("impl_id", implementationId);
		db.runStoredProcedure("location_dimension", params);
		log.info("Location transformation complete.");
	}

	/**
	 * Fill in user dimension table
	 * 
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public void userDimension(Date from, Date to, int implementationId)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		log.info("Transforming user attributes.");
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("impl_id", implementationId);
		db.runStoredProcedure("user_dimension", params);
		log.info("User transformation complete.");
	}

	/**
	 * Prepare patient data and fill in Patient dimension
	 * 
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public void patientDimension(Date from, Date to, int implementationId)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		log.info("Transforming patient attributes.");
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("impl_id", implementationId);
		params.put("date_from", from);
		params.put("date_to", to);
		db.runStoredProcedure("patient_dimension", params);
		log.info("Patient transformation complete.");
	}

	/**
	 * Fill in encounters and observations in respective dimensions
	 * 
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public void encounterAndObsDimension(Date from, Date to, int implementationId)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		log.info("Transforming encounter attributes.");
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("impl_id", implementationId);
		params.put("date_from", from);
		params.put("date_to", to);
		db.runStoredProcedure("encounter_dimension", params);
		log.info("Encounter transformation complete.");
	}

	/**
	 * Fill in user forms results in respective dimensions
	 * 
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public void userFormAndResultDimension(Date from, Date to, int implementationId)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		log.info("Transforming user form attributes.");
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("impl_id", implementationId);
		params.put("date_from", from);
		params.put("date_to", to);
		db.runStoredProcedure("user_form_dimension", params);
		log.info("User Form transformation complete.");
	}

	/**
	 * Fill in common lab results in respective dimensions
	 * 
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public void commonLabResultDimension(Date from, Date to, int implementationId)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		log.info("Transforming common lab module attributes.");
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("impl_id", implementationId);
		params.put("date_from", from);
		params.put("date_to", to);
		db.runStoredProcedure("lab_test_dimension", params);
		log.info("Common lab module transformation complete.");
	}

	public void deencounterizeGfatm() {
		// Create a temporary table to save questions for each user form type
		db.runCommand(CommandType.DROP, "drop table if exists tmp");
		db.runCommand(CommandType.CREATE,
				"create table tmp select distinct user_form_type_id, element_id, element_name as question from dim_user_form_result");
		// Fetch user form types and names
		Object[][] userFormTypes = db.getTableData("dim_user_form", "distinct user_form_type_id, user_form_type", null);
		if (userFormTypes == null) {
			log.severe("User Form types could not be fetched");
			return;
		}
		for (Object[] userFormType : userFormTypes) {
			StringBuilder query = new StringBuilder();
			// Create a de-encounterized table
			Object[][] data = db.getTableData("tmp", "question", "user_form_type_id=" + userFormType[0].toString());
			ArrayList<String> elements = new ArrayList<String>();
			for (int i = 0; i < data.length; i++) {
				if (data[i][0] == null) {
					continue;
				}
				elements.add(data[i][0].toString());
			}
			StringBuilder groupConcat = new StringBuilder();
			for (Object element : elements) {
				String str = element.toString().replaceAll("[^A-Za-z0-9]", "_").toLowerCase();
				groupConcat.append(
						"group_concat(if(ufr.element_name = '" + element + "', ufr.result, NULL)) AS " + str + ", ");
			}
			String userFormName = userFormType[1].toString().toLowerCase().replace(" ", "_").replace("-", "_");
			query.append("create table uform_" + userFormName + " ");
			query.append(
					"select uf.surrogate_id, uf.implementation_id, uf.user_form_id, uf.user_id, u.username, uf.location_id, l.location_name, uf.date_entered, ");
			query.append(groupConcat.toString());
			query.append("'' as BLANK from dim_user_form as uf ");
			query.append("inner join dim_user_form_result as ufr on ufr.user_form_id = uf.user_form_id ");
			query.append(
					"left outer join gfatm_users as u on u.implementation_id = uf.implementation_id and u.user_id = uf.user_id ");
			query.append(
					"left outer join gfatm_location as l on l.implementation_id = uf.implementation_id and l.location_id = uf.location_id ");
			query.append("where uf.user_form_type_id = '" + userFormType[0].toString() + "' ");
			query.append(
					"group by uf.surrogate_id, uf.implementation_id, uf.user_form_id, uf.user_id, u.username, uf.location_id, l.location_name, uf.date_entered");
			// Drop previous table
			db.runCommand(CommandType.DROP, "drop table if exists uform_" + userFormName);
			log.info("Generating table for " + userFormType[1].toString());
			try {
				// Insert new data
				Object result = db.runCommand(CommandType.CREATE, query.toString());
				if (result == null) {
					log.warning("No data imported for User Form " + userFormType[1].toString());
				}
				// Creating Primary key
				db.runCommand(CommandType.ALTER,
						"alter table uform_" + userFormName + " add primary key surrogate_id (surrogate_id)");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Transforms the encounters, observations and forms data into separate
	 * tables
	 */
	public void deencounterizeOpenMrs() {
		// Create a temporary table to save questions for each encounter type
		db.runCommand(CommandType.DROP, "drop table if exists tmp");
		db.runCommand(CommandType.CREATE,
				"create table tmp select distinct encounter_type, concept_id, question from dim_obs");
		// Fetch encounter types and names
		Object[][] encounterTypes = db.getTableData("dim_encounter", "distinct encounter_type, encounter_name",
				"where encounter_type=162");
		if (encounterTypes == null) {
			log.severe("Encounter types could not be fetched");
			return;
		}
		for (Object[] encounterType : encounterTypes) {
			StringBuilder query = new StringBuilder();
			// Create a de-encounterized table
			Object[][] data = db.getTableData("tmp", "question", "encounter_type=" + encounterType[0].toString());
			ArrayList<String> elements = new ArrayList<String>();
			for (int i = 0; i < data.length; i++) {
				if (data[i][0] == null) {
					continue;
				}
				elements.add(data[i][0].toString());
			}
			if (elements.isEmpty()) {
				continue;
			}
			StringBuilder groupConcat = new StringBuilder();
			for (Object element : elements) {
				String str = element.toString().replaceAll("[^A-Za-z0-9]", "_").toLowerCase();
				groupConcat.append("group_concat(if(o.question = '" + element + "', o.answer, NULL)) AS " + str + ", ");
			}
			String encounterName = encounterType[1].toString().toLowerCase().replace(" ", "_").replace("-", "_");
			query.append("create table enc_" + encounterName + " engine=InnoDB ");
			query.append(
					"select e.surrogate_id, e.implementation_id, e.encounter_id,  e.provider, e.location_id, l.location_name, e.patient_id, e.date_entered, ");
			query.append(groupConcat.toString());
			query.append("'' as BLANK from dim_encounter as e ");
			query.append("inner join dim_obs as o on o.encounter_id = e.encounter_id and o.voided = 0 ");
			query.append("inner join dim_location as l on l.location_id = e.location_id ");
			query.append("where e.encounter_type = '" + encounterType[0].toString() + "' ");
			// Filter out all child observations (e.g. multi-select)
			query.append("and o.obs_group_id is null ");
			query.append(
					"group by e.surrogate_id, e.implementation_id, e.encounter_id, e.patient_id, e.provider, e.location_id, e.date_entered");
			// Drop previous table
			db.runCommand(CommandType.DROP, "drop table if exists enc_" + encounterName);
			log.info("Generating table for " + encounterType[1].toString());
			try {
				log.info("Executing: " + query.toString());
				// Insert new data
				Object result = db.runCommand(CommandType.CREATE, query.toString());
				if (result == null) {
					log.warning("No data imported for Encounter " + encounterType[1].toString());
				}
				// Creating Primary key
				db.runCommand(CommandType.ALTER, "alter table enc_" + encounterName
						+ " add primary key surrogate_id (surrogate_id), add key patient_id (patient_id), add key encounter_id (encounter_id)");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Transforms the common-lab module data into separate tables
	 */
	public void denormalizeOpenMrsExtended() {
		// Create a temporary table to save questions for each encounter type
		db.runCommand(CommandType.DROP, "drop table if exists commonlab_tmp");
		db.runCommand(CommandType.CREATE,
				"create table commonlab_tmp select distinct test_type_id, attribute_type_id, attribute_type_name from dim_lab_test_result");
		// Fetch types and names
		Object[][] testTypes = db.getTableData("commonlabtest_type", "distinct test_type_id, short_name", null);
		if (testTypes == null) {
			log.severe("Lab test types could not be fetched");
			return;
		}
		for (Object[] testType : testTypes) {
			StringBuilder query = new StringBuilder();
			// Create a de-encounterized table
			Object[][] data = db.getTableData("commonlab_tmp", "attribute_type_name",
					"test_type_id=" + testType[0].toString());
			ArrayList<String> elements = new ArrayList<String>();
			for (int i = 0; i < data.length; i++) {
				if (data[i][0] == null) {
					continue;
				}
				elements.add(data[i][0].toString());
			}
			if (elements.isEmpty()) {
				continue;
			}
			StringBuilder groupConcat = new StringBuilder();
			for (Object element : elements) {
				String str = element.toString().replaceAll("[^A-Za-z0-9]", "_").toLowerCase();
				groupConcat.append("group_concat(if(r.attribute_type_name = '" + element
						+ "', r.value_reference, NULL)) AS " + str + ", ");
			}
			String labTestType = testType[1].toString().toLowerCase().replace(" ", "_").replace("-", "_");
			query.append("create table lab_" + labTestType + " engine=InnoDB ");
			query.append(
					"select t.surrogate_id, t.implementation_id, t.test_order_id, t.orderer, t.patient_id, t.encounter_id, t.lab_reference_number, t.order_date, ");
			query.append(groupConcat.toString());
			query.append("'' as BLANK from dim_lab_test as t ");
			query.append("inner join dim_lab_test_result as r on r.test_order_id = t.test_order_id ");
			query.append("where t.test_type_id = '" + testType[0].toString() + "' ");
			query.append(
					"group by t.surrogate_id, t.implementation_id, t.test_order_id, t.orderer, t.patient_id, t.encounter_id, t.lab_reference_number, t.order_date");
			// Drop previous table
			db.runCommand(CommandType.DROP, "drop table if exists lab_" + labTestType);
			log.info("Generating table for " + testType[1].toString());
			try {
				log.info("Executing: " + query.toString());
				// Insert new data
				Object result = db.runCommand(CommandType.CREATE, query.toString());
				if (result == null) {
					log.warning("No data imported for Lab Test " + testType[1].toString());
				}
				// Creating Primary key
				db.runCommand(CommandType.ALTER, "alter table lab_" + labTestType
						+ " add primary key surrogate_id (surrogate_id), add key patient_id (patient_id), add key test_order_id (test_order_id)");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * This method splits max number into n equal partitions
	 * 
	 * @param maxNumber
	 * @param nParts
	 * @return
	 */
	public static long[] split(long maxNumber, int nParts) {
		long[] arr = new long[nParts];
		for (int i = 0; i < arr.length; i++)
			maxNumber -= arr[i] = (maxNumber + nParts - i - 1) / (nParts - i);
		return arr;
	}
}
