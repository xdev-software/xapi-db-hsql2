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

/*-
 * #%L
 * HSQL2
 * %%
 * Copyright (C) 2003 - 2023 XDEV Software
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Lesser Public License for more details.
 * 
 * You should have received a copy of the GNU General Lesser Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/lgpl-3.0.html>.
 * #L%
 */


import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hsqldb.jdbc.JDBCStatement;
import xdev.db.ColumnMetaData;
import xdev.db.DBException;
import xdev.db.DataType;
import xdev.db.Index;
import xdev.db.Index.IndexType;
import xdev.db.Result;
import xdev.db.StoredProcedure;
import xdev.db.StoredProcedure.Param;
import xdev.db.StoredProcedure.ParamType;
import xdev.db.StoredProcedure.ReturnTypeFlavor;
import xdev.db.jdbc.JDBCConnection;
import xdev.db.jdbc.JDBCMetaData;
import xdev.db.sql.Functions;
import xdev.db.sql.SELECT;
import xdev.db.sql.Table;
import xdev.util.ProgressMonitor;
import xdev.vt.Cardinality;
import xdev.vt.EntityRelationship;
import xdev.vt.EntityRelationship.Entity;
import xdev.vt.EntityRelationshipModel;

import com.xdev.jadoth.sqlengine.interfaces.ConnectionProvider;


public class HSQL2JDBCMetaData extends JDBCMetaData
{
	private static final long		serialVersionUID	= 2862594319338582561L;
	private final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	private final DateFormat timeFormat = new SimpleDateFormat("HH:mm:ss");
	private final DateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	
	public HSQL2JDBCMetaData(HSQL2JDBCDataSource dataSource) throws DBException
	{
		super(dataSource);
	}
	
	@Override
	public TableInfo[] getTableInfos(ProgressMonitor monitor, EnumSet<TableType> types) throws DBException
	{
		monitor.beginTask("",ProgressMonitor.UNKNOWN);
		
		List<TableInfo> list = new ArrayList<>();
		
		try(JDBCConnection jdbcConnection = (JDBCConnection)dataSource.openConnection())
		{
			String tableTypeStatement = getTableTypeStatement(types);
			Result result = jdbcConnection.query("SELECT TABLE_SCHEM, TABLE_NAME, TABLE_TYPE "
				+ "FROM INFORMATION_SCHEMA.SYSTEM_TABLES " + "WHERE TABLE_TYPE in "
				+ tableTypeStatement);
			
			while(result.next() && !monitor.isCanceled())
			{
				String tableType = result.getString("TABLE_TYPE");
				
				TableType type = null;
				if(tableType.equals("TABLE"))
				{
					type = TableType.TABLE;
				}
				else if(tableType.equals("VIEW"))
				{
					type = TableType.VIEW;
				}
				
				if(type != null && types.contains(type))
				{
					list.add(new TableInfo(type, result.getString("TABLE_SCHEM"), result
						.getString("TABLE_NAME")));
				}
			}
			
			result.close();
		}
		
		monitor.done();
		
		TableInfo[] tables = list.toArray(new TableInfo[list.size()]);
		Arrays.sort(tables);
		return tables;
	}
	
	
	@Override
	protected TableMetaData getTableMetaData(JDBCConnection jdbcConnection, DatabaseMetaData meta,
			int flags, TableInfo table) throws DBException, SQLException
	{
		String               tableName        = table.getName();
		Table                tableIdentity    = new Table(tableName);
		Map<String, Boolean> autoIncrementMap = new HashMap<>();
		SELECT               select           = new SELECT().FROM(tableIdentity).WHERE("1 = 0");
		Result               result           = jdbcConnection.query(select);
		int                  cc               = result.getColumnCount();
		
		for(int i = 0; i < cc; i++)
		{
			ColumnMetaData cm = result.getMetadata(i);
			autoIncrementMap.put(cm.getName(),cm.isAutoIncrement());
		}
		result.close();
		
		result = jdbcConnection.query("SELECT * FROM INFORMATION_SCHEMA.SYSTEM_COLUMNS "
				+ "WHERE TABLE_NAME = ?",table.getName());
		
		List<ColumnMetaData> columns = new ArrayList<>();
		while(result.next())
		{
			addMetaDataToColumns(table, autoIncrementMap, result, columns);
		}
		result.close();
		
		Map<IndexInfo, Set<String>> indexMap          = new HashMap<>();
		Set<String>                 primaryKeyColumns = new HashSet<>();
		String                      primaryKeyName    = "PRIMARY_KEY";
		
		result = jdbcConnection.query("SELECT * FROM INFORMATION_SCHEMA.SYSTEM_PRIMARYKEYS "
				+ "WHERE TABLE_NAME = ?",table.getName());
		
		while(result.next())
		{
			primaryKeyColumns.add(result.getString("COLUMN_NAME"));
			primaryKeyName = result.getString("PK_NAME");
		}
		result.close();
		
		if((flags & INDICES) != 0)
		{
			if(!primaryKeyColumns.isEmpty())
			{
				indexMap.put(new IndexInfo(primaryKeyName,IndexType.PRIMARY_KEY),primaryKeyColumns);
			}
			
			result = jdbcConnection.query("SELECT * FROM INFORMATION_SCHEMA.SYSTEM_INDEXINFO "
					+ "WHERE TABLE_NAME = ?",table.getName());
			
			while(result.next())
			{
				String indexName = result.getString("INDEX_NAME");
				String columnName = result.getString("COLUMN_NAME");
				if(indexName != null
					&& columnName != null
						&& !primaryKeyColumns.contains(columnName))
				{
					boolean unique = !result.getBoolean("NON_UNIQUE");
					IndexInfo info = new IndexInfo(indexName,unique ? IndexType.UNIQUE : IndexType.NORMAL);
					Set<String> columnNames = indexMap.get(info);
					if(columnNames == null)
					{
						columnNames = new HashSet<>();
						indexMap.put(info,columnNames);
					}
					columnNames.add(columnName);
				}
			}
			result.close();
		}
		
		Index[] indices = new Index[indexMap.size()];
		int i = 0;
		for(IndexInfo indexInfo : indexMap.keySet())
		{
			Set<String> columnList = indexMap.get(indexInfo);
			String[] indexColumns = columnList.toArray(new String[columnList.size()]);
			indices[i++] = new Index(indexInfo.name,indexInfo.type,indexColumns);
		}
		
		int count = UNKNOWN_ROW_COUNT;
		
		if((flags & ROW_COUNT) != 0)
		{
			result = jdbcConnection.query(new SELECT().columns(Functions.COUNT()).FROM(
					tableIdentity));
			if(result.next())
			{
				count = result.getInt(0);
			}
			result.close();
		}
		
		return new TableMetaData(
			table,
			columns.toArray(new ColumnMetaData[columns.size()]),
			indices,
			count
		);
	}
	
	private static void addMetaDataToColumns(
		TableInfo table,
		Map<String, Boolean> autoIncrementMap,
		Result result,
		List<ColumnMetaData> columns) throws DBException
	{
		String columnName = result.getString("COLUMN_NAME");
		Object defaultValue = result.getObject("COLUMN_DEF");
		
		if("NULL".equals(defaultValue))
		{
			defaultValue = null;
		}
		
		columns.add(new ColumnMetaData(
			table.getName(),                                        //tableName
			columnName,                                             //columnName
			"",                                                     //caption
			DataType.get(result.getInt("DATA_TYPE")),       //type
			result.getInt("COLUMN_SIZE"),                   //length
			result.getInt("DECIMAL_DIGITS"),                //scale
			defaultValue,                                           //defaultValue
			"YES".equals(result.getString("IS_NULLABLE")),  //nullable
			Boolean.TRUE.equals(autoIncrementMap.get(columnName))   //autoIncrement
		));
	}
	
	@Override
	public StoredProcedure[] getStoredProcedures(ProgressMonitor monitor) throws DBException
	{
		monitor.beginTask("",ProgressMonitor.UNKNOWN);
		
		List<StoredProcedure> list = new ArrayList<>();
		
		try
		{
			ConnectionProvider<?> connectionProvider = dataSource.getConnectionProvider();
			
			try(Connection connection = connectionProvider.getConnection())
			{
				DatabaseMetaData meta = connection.getMetaData();
				
				ResultSet rs = getProcedures(connection);
				
				while(rs.next() && !monitor.isCanceled())
				{
					ReturnTypeFlavor returnTypeFlavor;
					DataType         returnType    = null;
					String           name          = rs.getString("PROCEDURE_NAME");
					String           description   = rs.getString("REMARKS");
					int              procedureType = rs.getInt("PROCEDURE_TYPE");
					
					switch(procedureType)
					{
						case DatabaseMetaData.procedureNoResult:
							returnTypeFlavor = ReturnTypeFlavor.VOID;
							break;
							
						case DatabaseMetaData.procedureReturnsResult:
							String dataType = rs.getString("DATA_TYPE");
							String[] split = dataType.split(",");
							if(split.length > 1)
							{
								returnTypeFlavor = ReturnTypeFlavor.RESULT_SET;
							}
							else
							{
								dataType = convDataTypeString(dataType);
								
								returnType = DataType.valueOf(dataType);
								returnTypeFlavor = ReturnTypeFlavor.TYPE;
							}
							
							break;
						default:
							returnTypeFlavor = ReturnTypeFlavor.UNKNOWN;
					}
					
					List<Param> params = new ArrayList<>();
					ResultSet rsp = meta.getProcedureColumns(null, "PUBLIC", null, name);
					
					while(rsp.next())
					{
						DataType dataType = DataType.get(rsp.getInt("DATA_TYPE"));
						String columnName = rsp.getString("COLUMN_NAME");
						switch(rsp.getInt("COLUMN_TYPE"))
						{
							case DatabaseMetaData.procedureColumnReturn:
								returnTypeFlavor = ReturnTypeFlavor.TYPE;
								returnType = dataType;
								break;
							
							case DatabaseMetaData.procedureColumnResult:
								returnTypeFlavor = ReturnTypeFlavor.RESULT_SET;
								break;
							
							case DatabaseMetaData.procedureColumnIn:
								params.add(new Param(ParamType.IN, columnName, dataType));
								break;
							
							case DatabaseMetaData.procedureColumnOut:
								params.add(new Param(ParamType.OUT, columnName, dataType));
								break;
							
							case DatabaseMetaData.procedureColumnInOut:
								params.add(new Param(ParamType.IN_OUT, columnName, dataType));
								break;
								
							default:
								break;
						}
					}
					rsp.close();
					
					list.add(new StoredProcedure(returnTypeFlavor, returnType, name, description,
						params.toArray(new Param[params.size()])));
				}
				rs.close();
			}
		}
		catch(SQLException e)
		{
			throw new DBException(dataSource,e);
		}
		
		monitor.done();
		
		return list.toArray(new StoredProcedure[list.size()]);
	}
	
	
	private String convDataTypeString(String dataType)
	{
		if(dataType.equalsIgnoreCase("character"))
		{
			dataType = "CHAR";
		}
		else if(dataType.equalsIgnoreCase("bit"))
		{
			dataType = "BOOLEAN";
		}
		else if(dataType.equalsIgnoreCase("other"))
		{
			dataType = "OBJECT";
		}
		else if(dataType.equalsIgnoreCase("datetime"))
		{
			dataType = "TIMESTAMP";
		}
		else if(dataType.equalsIgnoreCase("int"))
		{
			dataType = "INTEGER";
		}
		return dataType;
	}
	
	
	private ResultSet getProcedures(Connection connection) throws SQLException
	{
		String sql = "SELECT PROCEDURE_NAME,REMARKS, DATA_TYPE, PROCEDURE_TYPE FROM INFORMATION_SCHEMA.SYSTEM_PROCEDURES, INFORMATION_SCHEMA.ROUTINES WHERE INFORMATION_SCHEMA.SYSTEM_PROCEDURES.SPECIFIC_NAME LIKE INFORMATION_SCHEMA.ROUTINES.SPECIFIC_NAME AND PROCEDURE_SCHEM LIKE 'PUBLIC'";
		
		JDBCStatement localJDBCStatement;
		ResultSet executeQuery = null;
		try
		{
			localJDBCStatement = (JDBCStatement)connection.createStatement(1004,1007);
			executeQuery = localJDBCStatement.executeQuery(sql);
		}
		catch(SQLException e)
		{
			throw new SQLException("SqlStatement: " + sql);
		}
		return executeQuery;
	}
	
	
	@Override
	public EntityRelationshipModel getEntityRelationshipModel(ProgressMonitor monitor,
			TableInfo... tableInfos) throws DBException
	{
		monitor.beginTask("",tableInfos.length);
		
		EntityRelationshipModel model = new EntityRelationshipModel();
		
		try
		{
			List<String> tables = new ArrayList<>();
			for(TableInfo table : tableInfos)
			{
				if(table.getType() == TableType.TABLE)
				{
					tables.add(table.getName());
				}
			}
			Collections.sort(tables);
			
			try(Connection connection = dataSource.getConnectionProvider().getConnection())
			{
				DatabaseMetaData meta = connection.getMetaData();
				int done = 0;
				
				for(String table : tables)
				{
					if(monitor.isCanceled())
					{
						break;
					}
					
					monitor.setTaskName(table);
					
					ResultSet rs = meta.getExportedKeys(null, null, table);
					try
					{
						String pkTable = null;
						String fkTable = null;
						List<String> pkColumns = new ArrayList();
						List<String> fkColumns = new ArrayList();
						
						while(rs.next())
						{
							short keySeq = rs.getShort("KEY_SEQ");
							
							if(keySeq == 1 && pkColumns.size() > 0)
							{
								addNewEntityRelationshipToModel(model, tables, pkTable, fkTable, pkColumns, fkColumns);
							}
							
							pkTable = rs.getString("PKTABLE_NAME");
							fkTable = rs.getString("FKTABLE_NAME");
							
							pkColumns.add(rs.getString("PKCOLUMN_NAME"));
							fkColumns.add(rs.getString("FKCOLUMN_NAME"));
						}
						
						if(pkColumns.size() > 0)
						{
							addNewEntityRelationshipToModel(model, tables, pkTable, fkTable, pkColumns, fkColumns);
						}
					}
					finally
					{
						rs.close();
					}
					
					monitor.worked(++done);
				}
			}
		}
		catch(SQLException e)
		{
			throw new DBException(dataSource,e);
		}
		
		monitor.done();
		
		return model;
	}
	
	/**
	 * Checks if the table contains PK and FK.
	 * If true, than it adds a new One-To-Many-Relationship to the model
	 */
	private void addNewEntityRelationshipToModel(
		EntityRelationshipModel model,
		List<String> tables,
		String pkTable,
		String fkTable,
		List<String> pkColumns,
		List<String> fkColumns)
	{
		if(tables.contains(pkTable) && tables.contains(fkTable))
		{
			model.add(new EntityRelationship(
				new Entity(
					pkTable,
					pkColumns.toArray(new String[pkColumns.size()]),
					Cardinality.ONE),
				new Entity(
					fkTable,
					fkColumns.toArray(new String[fkColumns.size()]),
					Cardinality.MANY)
			));
			pkColumns.clear();
			fkColumns.clear();
		}
	}
	
	@SuppressWarnings("incomplete-switch")
	@Override
	public boolean equalsType(ColumnMetaData clientColumn, ColumnMetaData dbColumn)
	{
		DataType clientType = clientColumn.getType();
		DataType dbType = dbColumn.getType();
		
		if(clientType == dbType)
		{
			switch(clientType)
			{
				case TINYINT:
				case SMALLINT:
				case INTEGER:
				case BIGINT:
				case REAL:
				case FLOAT:
				case DOUBLE:
				case DATE:
				case TIME:
				case TIMESTAMP:
				case BOOLEAN:
				case BINARY:
				case VARBINARY:
				case LONGVARCHAR:
				case LONGVARBINARY:
				case CLOB:
				case BLOB:
				{
					return true;
				}
				
				case NUMERIC:
				case DECIMAL:
				{
					return clientColumn.getLength() == dbColumn.getLength()
							&& clientColumn.getScale() == dbColumn.getScale();
				}
				
				case CHAR:
				case VARCHAR:
				{
					return clientColumn.getLength() == dbColumn.getLength();
				}
			}
		}
		
		Boolean match = getTypeMatch(clientColumn,dbColumn);
		if(match != null)
		{
			return match;
		}
		
		match = getTypeMatch(dbColumn,clientColumn);
		if(match != null)
		{
			return match;
		}
		
		return false;
	}
	

	@SuppressWarnings("incomplete-switch")
	private Boolean getTypeMatch(ColumnMetaData thisColumn, ColumnMetaData thatColumn)
	{
		DataType thisType = thisColumn.getType();
		DataType thatType = thatColumn.getType();
		
		switch(thisType)
		{
			case CLOB:
			{
				return thatType == DataType.LONGVARCHAR;
			}
			
			case LONGVARCHAR:
			{
				return thatType == DataType.CLOB;
			}
			
			case BLOB:
			{
				return thatType == DataType.BINARY;
			}
			
			case BINARY:
			{
				return thatType == DataType.BLOB;
			}
			
			case BOOLEAN:
			{
				return thatType == DataType.TINYINT && thatColumn.getLength() == 1;
			}
			
			case TINYINT:
			{
				return thatType == DataType.BOOLEAN && thisColumn.getLength() == 1;
			}
			
			default:
				return null;
		}
	}
	
	
	@Override
	protected void createTable(JDBCConnection jdbcConnection, TableMetaData table)
			throws DBException, SQLException
	{
		StringBuilder sb = new StringBuilder();
		sb.append("CREATE CACHED TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		sb.append(" (");
		
		ColumnMetaData[] columns = table.getColumns();
		for(int i = 0; i < columns.length; i++)
		{
			if(i > 0)
			{
				sb.append(", ");
			}
			
			ColumnMetaData column = columns[i];
			appendEscapedName(column.getName(),sb);
			sb.append(" ");
			appendColumnDefinition(column,sb);
		}
		
		for(Index index : table.getIndices())
		{
			if(isSupported(index))
			{
				sb.append(", ");
				appendIndexDefinition(index,sb);
			}
		}
		
		sb.append(")");
		
		jdbcConnection.write(
			sb.toString(),
			false,
			new ArrayList<>().toArray()
		);
	}
	
	
	@Override
	protected void addColumn(JDBCConnection jdbcConnection, TableMetaData table,
			ColumnMetaData column, ColumnMetaData columnBefore, ColumnMetaData columnAfter)
			throws DBException, SQLException
	{
		StringBuilder sb = new StringBuilder();
		sb.append("ALTER TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		sb.append(" ADD COLUMN ");
		appendEscapedName(column.getName(),sb);
		sb.append(" ");
		appendColumnDefinition(column,sb);
		if(columnAfter != null)
		{
			sb.append(" BEFORE ");
			appendEscapedName(columnBefore.getName(),sb);
		}
		
		jdbcConnection.write(
			sb.toString(),
			false,
			new ArrayList<>().toArray()
		);
	}
	
	
	@Override
	protected void alterColumn(JDBCConnection jdbcConnection, TableMetaData table,
			ColumnMetaData column, ColumnMetaData existing) throws DBException, SQLException
	{
		StringBuilder sb = new StringBuilder();
		sb.append("ALTER TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		sb.append(" ALTER COLUMN ");
		appendEscapedName(existing.getName(),sb);
		sb.append(" ");
		appendColumnDefinition(column,sb);
		
		jdbcConnection.write(
			sb.toString(),
			false,
			new ArrayList<>().toArray()
		);
	}
	

	@SuppressWarnings("incomplete-switch")
	private void appendColumnDefinition(ColumnMetaData column, StringBuilder sb)
	{
		DataType type = column.getType();
		appendCorrectDataType(column, sb, type);
		
		if(column.isAutoIncrement())
		{
			sb.append(" GENERATED BY DEFAULT AS IDENTITY");
		}
		else
		{
			Object defaultValue = column.getDefaultValue();
			if(!(defaultValue == null && !column.isNullable()))
			{
				sb.append(" DEFAULT ");
				if(defaultValue == null)
				{
					sb.append("NULL");
				}
				else if(defaultValue instanceof String)
				{
					appendStringWithSurroundingBackslashes(sb, defaultValue);
				}
				else if(defaultValue instanceof Date)
				{
					appendCorrectDateFormat(sb, type, (Date)defaultValue);
				}
				else
				{
					sb.append(defaultValue.toString());
				}
			}
		}
		
		if(column.isNullable())
		{
			sb.append(" NULL");
		}
		else
		{
			sb.append(" NOT NULL");
		}
	}
	
	/**
	 * Switches on {@link DataType}, to append the correct values to the {@link StringBuilder}
	 */
	private static void appendCorrectDataType(ColumnMetaData column, StringBuilder sb, DataType type)
	{
		switch(type)
		{
			case TINYINT:
			case SMALLINT:
			case INTEGER:
			case BIGINT:
			case REAL:
			case FLOAT:
			case DOUBLE:
			case DATE:
			case TIME:
			case TIMESTAMP:
			case BOOLEAN:
			case BINARY:
			case VARBINARY:
			case LONGVARCHAR:
			case LONGVARBINARY:
			{
				sb.append(type.name());
			}
			break;
			
			case NUMERIC:
			case DECIMAL:
			{
				sb.append(type.name());
				sb.append("(");
				sb.append(column.getLength());
				sb.append(",");
				sb.append(column.getScale());
				sb.append(")");
			}
			break;
			
			case CHAR:
			case VARCHAR:
			{
				sb.append(type.name());
				sb.append("(");
				sb.append(column.getLength());
				sb.append(")");
			}
			break;
			
			case CLOB:
			{
				sb.append("LONGVARCHAR");
			}
			break;
			
			case BLOB:
			{
				sb.append("BINARY");
			}
			break;
			
			default:
				break;
		}
	}
	
	private static void appendStringWithSurroundingBackslashes(StringBuilder sb, Object defaultValue)
	{
		sb.append('\'');
		for(char ch : defaultValue.toString().toCharArray())
		{
			if(ch == '\'')
			{
				sb.append('\'');
			}
			sb.append(ch);
		}
		sb.append('\'');
	}
	
	/**
	 * Switches on {@link DataType} to append the corret {@link DateFormat} to the {@link StringBuilder}
	 */
	private void appendCorrectDateFormat(StringBuilder sb, DataType type, Date defaultValue)
	{
		DateFormat format = null;
		switch(type)
		{
			case DATE:
				format = dateFormat;
			break;
			case TIME:
				format = timeFormat;
			break;
			case TIMESTAMP:
				format = timestampFormat;
			break;
			default:
				break;
		}
		
		if(format != null)
		{
			sb.append('\'');
			sb.append(format.format(defaultValue));
			sb.append('\'');
		}
	}
	
	@Override
	protected void dropColumn(JDBCConnection jdbcConnection, TableMetaData table,
			ColumnMetaData column) throws DBException, SQLException
	{
		StringBuilder sb = new StringBuilder();
		sb.append("ALTER TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		sb.append(" DROP COLUMN ");
		appendEscapedName(column.getName(),sb);
		
		jdbcConnection.write(sb.toString());
	}
	
	
	@Override
	protected void createIndex(JDBCConnection jdbcConnection, TableMetaData table, Index index)
			throws DBException, SQLException
	{
		if(!isSupported(index))
		{
			return;
		}
		
		StringBuilder sb = new StringBuilder();
		sb.append("ALTER TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		sb.append(" ADD ");
		appendIndexDefinition(index,sb);
		
		jdbcConnection.write(sb.toString());
	}
	
	
	private boolean isSupported(Index index)
	{
		return index.getType() != IndexType.NORMAL;
	}
	
	
	private void appendIndexDefinition(Index index, StringBuilder sb) throws DBException
	{
		switch(index.getType())
		{
			case PRIMARY_KEY:
			{
				sb.append("PRIMARY KEY");
			}
			break;
			
			case UNIQUE:
			{
				sb.append("UNIQUE");
			}
			break;
			
			default:
			{
				throw new DBException(dataSource,
						"Only primary keys and unique indices are supported.");
			}
		}
		
		sb.append(" (");
		String[] columns = index.getColumns();
		for(int i = 0; i < columns.length; i++)
		{
			if(i > 0)
			{
				sb.append(", ");
			}
			appendEscapedName(columns[i],sb);
		}
		sb.append(")");
	}
	
	
	@Override
	protected void dropIndex(JDBCConnection jdbcConnection, TableMetaData table, Index index)
			throws DBException, SQLException
	{
		StringBuilder sb = new StringBuilder();
		sb.append("ALTER TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		
		sb.append(" DROP CONSTRAINT ");
		appendEscapedName(getValidIndexName(index),sb);
		
		jdbcConnection.write(sb.toString());
	}
	
	
	private String getValidIndexName(Index index)
	{
		String name = index.getName();
		if(name.equals("PRIMARY_KEY"))
		{
			name = "PK";
		}
		return name;
	}
	
	
	private String getTableTypeStatement(EnumSet<TableType> types)
	{
		if(types == null || types.isEmpty())
		{
			return "";
		}
		
		String tableStatement = "(";
		
		if(types.contains(TableType.TABLE))
		{
			tableStatement += "'TABLE'";
		}
		
		if(types.contains(TableType.TABLE) && types.contains(TableType.VIEW))
		{
			tableStatement += " , ";
		}
		
		if(types.contains(TableType.VIEW))
		{
			tableStatement += "'VIEW'";
		}
		
		tableStatement += ")";
		
		return tableStatement;
	}
}
