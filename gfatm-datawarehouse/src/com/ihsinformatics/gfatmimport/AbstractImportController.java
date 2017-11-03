/* Copyright(C) 2017 Interactive Health Solutions, Pvt. Ltd.

This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License as
published by the Free Software Foundation; either version 3 of the License (GPLv3), or any later version.
This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

See the GNU General Public License for more details. You should have received a copy of the GNU General Public License along with this program; if not, write to the Interactive Health Solutions, info@ihsinformatics.com
You can also access the license on the internet at the address: http://www.gnu.org/licenses/gpl-3.0.html

Interactive Health Solutions, hereby disclaims all copyright interest in this program written by the contributors.
 */

package com.ihsinformatics.gfatmimport;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Date;

import com.ihsinformatics.util.DatabaseUtil;
import com.ihsinformatics.util.DateTimeUtil;

/**
 * @author owais.hussain@ihsinformatics.com
 *
 */
public class AbstractImportController {
	protected DatabaseUtil targetDb;
	protected DatabaseUtil sourceDb;
	protected Date fromDate;
	protected Date toDate;

	/**
	 * Returns a filter for select queries
	 * 
	 * @param createDateName
	 * @param updateDateName
	 * @param fromDate
	 * @param toDate
	 * @return
	 */
	public String filter(String createDateName, String updateDateName) {
		StringBuilder filter = new StringBuilder(" WHERE 1=1 ");
		filter.append("AND (" + createDateName);
		filter.append(" BETWEEN TIMESTAMP('"
				+ DateTimeUtil.toSqlDateTimeString(fromDate) + "') ");
		filter.append("AND TIMESTAMP('"
				+ DateTimeUtil.toSqlDateTimeString(toDate) + "')) ");
		if (updateDateName != null) {
			filter.append(" OR (" + updateDateName);
			filter.append(" BETWEEN TIMESTAMP('"
					+ DateTimeUtil.toSqlDateTimeString(fromDate) + "') ");
			filter.append("AND TIMESTAMP('"
					+ DateTimeUtil.toSqlDateTimeString(toDate) + "')) ");
		}
		return filter.toString();
	}

	/**
	 * Fetch data from remote database and insert into local database
	 * 
	 * @param selectQuery
	 * @param insertQuery
	 * @return
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	public void remoteSelectInsert(String selectQuery, String insertQuery)
			throws SQLException, InstantiationException,
			IllegalAccessException, ClassNotFoundException {
		Connection remoteConnection = sourceDb.getConnection();
		Connection localConnection = targetDb.getConnection();
		remoteSelectInsert(selectQuery, insertQuery, remoteConnection,
				localConnection);
	}

	/**
	 * Fetch data from source database and insert into target database
	 * 
	 * @param selectQuery
	 * @param insertQuery
	 * @param sourceConnection
	 * @param targetConnection
	 * @throws SQLException
	 */
	public void remoteSelectInsert(String selectQuery, String insertQuery,
			Connection sourceConnection, Connection targetConnection)
			throws SQLException {
		PreparedStatement source = sourceConnection
				.prepareStatement(selectQuery);
		PreparedStatement target = targetConnection
				.prepareStatement(insertQuery);
		ResultSet data = source.executeQuery();
		ResultSetMetaData metaData = data.getMetaData();
		while (data.next()) {
			for (int i = 1; i <= metaData.getColumnCount(); i++) {
				String value = data.getString(i);
				target.setString(i, value);
			}
			target.executeUpdate();
		}
	}
}
