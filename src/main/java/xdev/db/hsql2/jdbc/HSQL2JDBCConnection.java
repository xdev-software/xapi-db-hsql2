/*
 * SqlEngine Database Adapter HSQL2 - XAPI SqlEngine Database Adapter for HSQL2
 * Copyright © 2003 XDEV Software (https://xdev.software)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package xdev.db.hsql2.jdbc;

import java.sql.Connection;
import java.sql.Statement;
import java.text.ParseException;
import java.util.Date;
import java.util.Map;

import xdev.db.DBException;
import xdev.db.jdbc.JDBCConnection;


public class HSQL2JDBCConnection extends JDBCConnection<HSQL2JDBCDataSource, HSQL2Dbms>
{
	public HSQL2JDBCConnection(HSQL2JDBCDataSource dataSource)
	{
		super(dataSource);
	}
	
	@Override
	public void createTable(
		String tableName, String primaryKey, Map<String, String> columnMap,
		boolean isAutoIncrement, Map<String, String> foreignKeys) throws Exception
	{
		
		if(!columnMap.containsKey(primaryKey))
		{
			columnMap.put(primaryKey, "INTEGER"); //$NON-NLS-1$
		}
		
		StringBuffer createStatement = null;
		
		if(isAutoIncrement)
		{
			createStatement =
				new StringBuffer("CREATE TABLE IF NOT EXISTS \"" + tableName + "\"(" //$NON-NLS-1$ //$NON-NLS-2$
					+ primaryKey + " " + columnMap.get(primaryKey) + " IDENTITY NOT NULL,"); //$NON-NLS-1$
			// $NON-NLS-2$
		}
		else
		{
			createStatement = new StringBuffer("CREATE IF NOT EXISTS \"" + tableName + "\"(" //$NON-NLS-1$
				// $NON-NLS-2$
				+ primaryKey + " " + columnMap.get(primaryKey) + ","); //$NON-NLS-1$ //$NON-NLS-2$
		}
		
		for(String keySet : columnMap.keySet())
		{
			if(!keySet.equals(primaryKey))
			{
				createStatement.append(keySet + " " + columnMap.get(keySet) + ","); //$NON-NLS-1$ //$NON-NLS-2$
			}
		}
		
		createStatement.append(")"); //$NON-NLS-1$
		
		if(log.isDebugEnabled())
		{
			log.debug("SQL Statement to create a table: " + createStatement); //$NON-NLS-1$
		}
		
		Connection connection = super.getConnection();
		Statement statement = connection.createStatement();
		try
		{
			statement.execute(createStatement.toString());
		}
		catch(Exception e)
		{
			throw e;
		}
		finally
		{
			statement.close();
			connection.close();
		}
	}
	
	@Override
	public Date getServerTime() throws DBException, ParseException
	{
		String selectTime = "CALL current_timestamp "; //$NON-NLS-1$
		return super.getServerTime(selectTime);
	}
}
