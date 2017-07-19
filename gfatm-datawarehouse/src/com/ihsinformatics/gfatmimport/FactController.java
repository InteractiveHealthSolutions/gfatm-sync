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
	private static final String factQueriesFile = "fact_queries.sql";
	private DatabaseUtil db;

	public static void main(String[] args) {
		DatabaseUtil myDb = new DatabaseUtil();
		myDb.setConnection(
				"jdbc:mysql://127.0.0.1:3306/gfatm_dw?autoReconnect=true&useSSL=false",
				"gfatm_dw", "com.mysql.jdbc.Driver", "root", "jingle94");
		myDb.tryConnection();
		FactController fc = new FactController(myDb);
		fc.modelFacts();
	}

	public FactController(DatabaseUtil db) {
		this.db = db;
	}

	/**
	 * Perform fact modeling
	 */
	public void modelFacts() {
		try {
			log.info("Starting fact modeling");
			SqlExecuteUtil sqlUtil = new SqlExecuteUtil(db.getUrl(),
					db.getDriverName(), db.getUsername(), db.getPassword());
			sqlUtil.execute(factQueriesFile);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		log.info("Finished fact modeling");
	}
}
