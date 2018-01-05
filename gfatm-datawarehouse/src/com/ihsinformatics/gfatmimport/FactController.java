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
import java.util.logging.Logger;

import com.ihsinformatics.util.DatabaseUtil;

/**
 * @author owais.hussain@ihsinformatics.com
 *
 */
public class FactController {

	private static final Logger log = Logger.getLogger(Class.class.getName());
	private DatabaseUtil dwDb;

	public FactController(DatabaseUtil db) {
		this.dwDb = db;
	}

	/**
	 * Perform fact modeling
	 */
	public void modelFacts() {
		try {
			dwDb.runStoredProcedure("fact_modeling", null);
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		log.info("Finished fact modeling");
	}
}
