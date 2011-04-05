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

import java.io.Serializable;
import java.util.HashMap;

public final class Database implements Serializable
{
	private String users;
	protected Database(String name,String user)
	{
		users = user;
		this.name = name;
		tables = new HashMap<String, Table>();
	}
	private String name;
	protected HashMap<String,Table> tables;
	
	protected void newTable(String tableName,Table table)
	{
		tables.put(tableName, table);
	}
	
	protected void alterTable(String tableName,String[] colTypesNames,String[] misc)
	{
		tables.get(tableName).modify(colTypesNames, misc);
	}
	
	protected Table getTable(String tableName)
	{
		return tables.get(tableName);
	}
}
