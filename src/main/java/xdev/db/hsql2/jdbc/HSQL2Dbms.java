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

import com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor;
import com.xdev.jadoth.sqlengine.dbms.SQLExceptionParser;
import com.xdev.jadoth.sqlengine.internal.DatabaseGateway;
import com.xdev.jadoth.sqlengine.internal.tables.SqlTableIdentity;


public class HSQL2Dbms
	extends
	DbmsAdaptor.Implementation<HSQL2Dbms, HSQL2DMLAssembler, HSQL2DDLMapper, HSQL2RetrospectionAccessor, HSQL2Syntax>
{
	// /////////////////////////////////////////////////////////////////////////
	// constants //
	// ///////////////////
	
	public static final HSQL2Syntax SYNTAX = new HSQL2Syntax();
	/**
	 * The Constant MAX_VARCHAR_LENGTH.
	 */
	protected static final int MAX_VARCHAR_LENGTH = Integer.MAX_VALUE;
	protected static final char IDENTIFIER_DELIMITER = '"';
	
	// /////////////////////////////////////////////////////////////////////////
	// constructors //
	// ///////////////////
	
	/**
	 * Instantiates a new hsql20 dbms.
	 */
	public HSQL2Dbms()
	{
		this(new SQLExceptionParser.Body());
	}
	
	/**
	 * Instantiates a new hsql20 dbms.
	 *
	 * @param sqlExceptionParser the sql exception parser
	 */
	public HSQL2Dbms(final SQLExceptionParser sqlExceptionParser)
	{
		super(sqlExceptionParser, false);
		this.setRetrospectionAccessor(new HSQL2RetrospectionAccessor(this));
		this.setDMLAssembler(new HSQL2DMLAssembler(this));
		this.setSyntax(SYNTAX);
	}
	
	/**
	 * @see DbmsAdaptor#createConnectionInformation(String, int, String, String, String, String)
	 */
	@Override
	public HSQL2ConnectionInformation createConnectionInformation(
		final String host,
		final int port, final String user, final String password, final String catalog, final String properties)
	{
		return new HSQL2ConnectionInformation(host, port, user, password, catalog, properties, this);
	}
	
	/**
	 * HSQL does not support any means of calculating table columns selectivity as far as it is known.
	 */
	@Override
	public Object updateSelectivity(final SqlTableIdentity table)
	{
		return null;
	}
	
	/**
	 * @see DbmsAdaptor#assembleTransformBytes(byte[], java.lang.StringBuilder)
	 */
	@Override
	public StringBuilder assembleTransformBytes(final byte[] bytes, final StringBuilder sb)
	{
		return null;
	}
	
	/**
	 * @see DbmsAdaptor.Implementation#getRetrospectionAccessor()
	 */
	@Override
	public HSQL2RetrospectionAccessor getRetrospectionAccessor()
	{
		throw new RuntimeException("HSQL Retrospection not implemented yet!");
	}
	
	/**
	 * @see DbmsAdaptor#initialize(DatabaseGateway)
	 */
	@Override
	public void initialize(final DatabaseGateway<HSQL2Dbms> dbc)
	{
	}
	
	/**
	 * @see DbmsAdaptor#rebuildAllIndices(String)
	 */
	@Override
	public Object rebuildAllIndices(final String fullQualifiedTableName)
	{
		return null;
	}
	
	@Override
	public boolean supportsOFFSET_ROWS()
	{
		return true;
	}
	
	/**
	 * @see DbmsAdaptor#getMaxVARCHARlength()
	 */
	@Override
	public int getMaxVARCHARlength()
	{
		return MAX_VARCHAR_LENGTH;
	}
	
	@Override
	public char getIdentifierDelimiter()
	{
		return IDENTIFIER_DELIMITER;
	}
}
