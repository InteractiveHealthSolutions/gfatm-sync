/* Copyright(C) 2016 Interactive Health Solutions, Pvt. Ltd.

This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License as
published by the Free Software Foundation; either version 3 of the License (GPLv3), or any later version.
This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

See the GNU General Public License for more details. You should have received a copy of the GNU General Public License along with this program; if not, write to the Interactive Health Solutions, info@ihsinformatics.com
You can also access the license on the internet at the address: http://www.gnu.org/licenses/gpl-3.0.html

Interactive Health Solutions, hereby disclaims all copyright interest in this program written by the contributors.
 */
package com.ihsinformatics.gfatmimport;

import java.util.logging.Logger;

import com.ihsinformatics.util.CommandType;
import com.ihsinformatics.util.DatabaseUtil;

/**
 * @author owais.hussain@ihsinformatics.com
 *
 */
public class FactController {

	private static final Logger log = Logger.getLogger(Class.class.getName());
	private DatabaseUtil db;

	public FactController(DatabaseUtil db) {
		this.db = db;
	}

	/**
	 * Create user facts
	 * 
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 */
	public void userFacts() throws InstantiationException,
			IllegalAccessException, ClassNotFoundException {
		StringBuilder query = new StringBuilder("insert ignore into fact_user ");
		query.append("select u.implementation_id, t.*, count(*) users from dim_user as u ");
		query.append("inner join dim_datetime as t on date(t.full_date) = date(u.date_created) ");
		query.append("group by u.implementation_id, t.datetime_id ");
		log.info("Inserting concept facts");
		db.runCommand(CommandType.INSERT, query.toString());
	}

	/**
	 * Create facts about concepts
	 * 
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 */
	public void conceptFacts() throws InstantiationException,
			IllegalAccessException, ClassNotFoundException {
		StringBuilder query = new StringBuilder(
				"insert ignore into fact_concept ");
		query.append("select c.implementation_id, ");
		query.append("(select count(*) from dim_concept where implementation_id = c.implementation_id and retired = 0) as active, ");
		query.append("(select count(*) from dim_concept where implementation_id = c.implementation_id and data_type in ('Numeric','Boolean','Date','Time','Datetime')) as real_valued, ");
		query.append("(select count(*) from dim_concept where implementation_id = c.implementation_id and data_type in ('N/A','Text')) as open_text, ");
		query.append("(select count(*) from dim_concept where implementation_id = c.implementation_id and data_type = 'Coded') as coded, ");
		query.append("(select count(DISTINCT uuid) from dim_concept) as unique_concepts, ");
		query.append("count(*) as total from dim_concept as c ");
		query.append("group by c.implementation_id ");
		log.info("Inserting concept facts");
		db.runCommand(CommandType.INSERT, query.toString());
	}

	/**
	 * Create location facts
	 * 
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 */
	public void locationFacts() throws InstantiationException,
			IllegalAccessException, ClassNotFoundException {
		StringBuilder query = new StringBuilder(
				"insert ignore into fact_location ");
		query.append("select l.implementation_id, t.*, count(*) as total from dim_location as l ");
		query.append("inner join dim_datetime as t on date(t.full_date) = date(l.date_created) ");
		query.append("group by l.implementation_id, t.datetime_id ");
		log.info("Inserting location facts");
		db.runCommand(CommandType.INSERT, query.toString());
	}

	/**
	 * Perform fact modeling
	 */
	public void modelFacts() {
		try {
			log.info("Starting fact modeling");
			userFacts();
			conceptFacts();
			locationFacts();

			// TODO: Add more facts

			log.info("Finished fact modeling");
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}
