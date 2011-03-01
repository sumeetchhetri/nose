package com.jdb;

import java.io.Serializable;
import java.util.HashMap;

public class Database implements Serializable
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
