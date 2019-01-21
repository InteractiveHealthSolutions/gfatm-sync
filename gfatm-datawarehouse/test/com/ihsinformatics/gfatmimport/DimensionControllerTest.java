/* Copyright(C) 2017 Interactive Health Solutions, Pvt. Ltd.

This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License as
published by the Free Software Foundation; either version 3 of the License (GPLv3), or any later version.
This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

See the GNU General Public License for more details. You should have received a copy of the GNU General Public License along with this program; if not, write to the Interactive Health Solutions, info@ihsinformatics.com
You can also access the license on the internet at the address: http://www.gnu.org/licenses/gpl-3.0.html

Interactive Health Solutions, hereby disclaims all copyright interest in this program written by the contributors.
 */

package com.ihsinformatics.gfatmimport;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;

import junit.framework.TestCase;

import org.dbunit.Assertion;
import org.dbunit.IDatabaseTester;
import org.dbunit.JdbcDatabaseTester;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.database.QueryDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.xml.FlatXmlDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ihsinformatics.util.DatabaseUtil;

/**
 * @author owais.hussain@ihsinformatics.com
 *
 */
public class DimensionControllerTest extends TestCase {

	private IDatabaseTester databaseTester;
	private static String dbUrl = "jdbc:mysql://localhost:3306/gfatm_dw";
	private static String driver = "com.mysql.jdbc.Driver";
	private static String username = "root";
	private static String password = "jingle94";

	public DimensionControllerTest(String name) {
		super(name);
		try {
			databaseTester = new JdbcDatabaseTester("org.hsqldb.jdbcDriver",
					"jdbc:hsqldb:sample", "sa", "");
			DatabaseUtil db = new DatabaseUtil(dbUrl, "gfatm_dw", driver, username, password);
			DimensionController dc = new DimensionController(db);
			dc.modelDimensions();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * Run as Java Application to generate XML copy of database
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// database connection
		Class.forName(driver);
		Connection jdbcConnection = DriverManager.getConnection(dbUrl,
				username, password);
		IDatabaseConnection connection = new DatabaseConnection(jdbcConnection);

		// Database export
		String[] transTables = { "_implementation", "encounter",
				"encounter_provider", "encounter_role", "encounter_type",
				"form", "location", "location_attribute",
				"location_attribute_type", "location_tag", "location_tag_map",
				"obs", "patient", "patient_identifier",
				"patient_identifier_type", "patient_program", "patient_state",
				"person", "person_address", "person_attribute",
				"person_attribute_type", "person_name", "privilege",
				"provider", "provider_attribute", "provider_attribute_type",
				"role", "role_privilege", "role_role", "user_property",
				"user_role", "users", "concept", "concept_answer",
				"concept_class", "concept_datatype", "concept_map_type",
				"concept_name", "concept_numeric", "concept_set",
				"concept_stop_word" };
		String[] dimensions = { "dim_concept", "dim_datetime", "dim_encounter",
				"dim_location", "dim_obs", "dim_patient", "dim_user" };
		String[] facts = { "fact_concept", "fact_location", "fact_user" };
		QueryDataSet dataSet = new QueryDataSet(connection);
		for (String s : transTables) {
			dataSet.addTable(s);
		}
		FlatXmlDataSet
				.write(dataSet, new FileOutputStream("res/test_data.xml"));
		dataSet = new QueryDataSet(connection);
		for (String s : dimensions) {
			dataSet.addTable(s);
		}
		FlatXmlDataSet.write(dataSet, new FileOutputStream(
				"res/dimensions_data.xml"));
		dataSet = new QueryDataSet(connection);
		for (String s : facts) {
			dataSet.addTable(s);
		}
		FlatXmlDataSet.write(dataSet,
				new FileOutputStream("res/facts_data.xml"));
		dataSet = new QueryDataSet(connection);
		// full database export
		// IDataSet fullDataSet = connection.createDataSet();
		// FlatXmlDataSet.write(fullDataSet, new
		// FileOutputStream("res/gfatm_dim_test.xml"));
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		IDataSet dataSet = null;
		databaseTester.setDataSet(dataSet);
		databaseTester.onSetup();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		databaseTester.onTearDown();
	}

	public void testMe() throws Exception {
		// Fetch database data after executing your code
		IDataSet databaseDataSet = databaseTester.getConnection()
				.createDataSet();
		ITable actualTable = databaseDataSet.getTable("TABLE_NAME");
		// Load expected data from an XML dataset
		IDataSet expectedDataSet = new FlatXmlDataSetBuilder().build(new File(
				"dimensions_data.xml"));
		ITable expectedTable = expectedDataSet.getTable("TABLE_NAME");
		// Assert actual database table match expected table
		Assertion.assertEquals(expectedTable, actualTable);
	}

	/**
	 * Test method for
	 * {@link com.ihsinformatics.gfatmimport.DimensionController#timeDimension()}
	 * .
	 */
	@Test
	public final void testTimeDimension() {
		fail("Not yet implemented"); // TODO
	}

	/**
	 * Test method for
	 * {@link com.ihsinformatics.gfatmimport.DimensionController#dimensionModeling(java.util.Date, java.util.Date, int)}
	 * .
	 */
	@Test
	public final void testConceptDimension() {
		fail("Not yet implemented"); // TODO
	}

	/**
	 * Test method for
	 * {@link com.ihsinformatics.gfatmimport.DimensionController#locationDimension(java.util.Date, java.util.Date, int)}
	 * .
	 */
	@Test
	public final void testLocationDimension() {
		fail("Not yet implemented"); // TODO
	}

	/**
	 * Test method for
	 * {@link com.ihsinformatics.gfatmimport.DimensionController#userDimension(java.util.Date, java.util.Date, int)}
	 * .
	 */
	@Test
	public final void testUserDimension() {
		fail("Not yet implemented"); // TODO
	}

	/**
	 * Test method for
	 * {@link com.ihsinformatics.gfatmimport.DimensionController#patientDimension(java.util.Date, java.util.Date, int)}
	 * .
	 */
	@Test
	public final void testPatientDimension() {
		fail("Not yet implemented"); // TODO
	}

	/**
	 * Test method for
	 * {@link com.ihsinformatics.gfatmimport.DimensionController#encounterAndObsDimension(java.util.Date, java.util.Date, int)}
	 * .
	 */
	@Test
	public final void testEncounterAndObsDimension() {
		fail("Not yet implemented"); // TODO
	}
}
