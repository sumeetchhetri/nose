/*
        Copyright 2011, Sumeet Chhetri 
  
    Licensed under the Apache License, Version 2.0 (the "License"); 
    you may not use this file except in compliance with the License. 
    You may obtain a copy of the License at 
  
        http://www.apache.org/licenses/LICENSE-2.0 
  
    Unless required by applicable law or agreed to in writing, software 
    distributed under the License is distributed on an "AS IS" BASIS, 
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
    See the License for the specific language governing permissions and 
    limitations under the License.  
*/
package com.jdb;

public class JdbQuery
{
	private String query;
	private String tablename;
	private String dbname;
	private String[] columns;
	private String[] whr;
	private String[] whrVals;
	private String[] values;
	private String[] clauses;
	public String[] getClauses()
	{
		return clauses;
	}
	public void setClauses(String[] clauses)
	{
		this.clauses = clauses;
	}
	public String[] getColumns()
	{
		return columns;
	}
	public void setColumns(String[] columns)
	{
		this.columns = columns;
	}
	public String getDbname()
	{
		return dbname;
	}
	public void setDbname(String dbname)
	{
		this.dbname = dbname;
	}
	public String getQuery()
	{
		return query;
	}
	public void setQuery(String query)
	{
		this.query = query;
	}
	public String getTablename()
	{
		return tablename;
	}
	public void setTablename(String tablename)
	{
		this.tablename = tablename;
	}
	public String[] getValues()
	{
		return values;
	}
	public void setValues(String[] values)
	{
		this.values = values;
	}
	public String[] getWhr()
	{
		return whr;
	}
	public void setWhr(String[] whr)
	{
		this.whr = whr;
	}
	public String[] getWhrVals()
	{
		return whrVals;
	}
	public void setWhrVals(String[] whrVals)
	{
		this.whrVals = whrVals;
	}
	public JdbQuery(String query, String tablename, String dbname, 
			String[] columns, String[] whr, String[] whrVals, String[] values, String[] clauses)
	{
		super();
		this.query = query;
		this.tablename = tablename;
		this.dbname = dbname;
		this.columns = columns;
		this.whr = whr;
		this.whrVals = whrVals;
		this.values = values;
		this.clauses = clauses;
	}
}
