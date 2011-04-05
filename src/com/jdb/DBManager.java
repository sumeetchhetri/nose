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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("unchecked")
public final class DBManager
{
	private volatile static DBManager dbManager;
	protected HashMap<String,Database> databases;
	
	private DBManager()
	{
		loadDatabases();
		loadUserPrivileges();
		loadConnections();
		loadCache();
	}
	 
	private void loadCache()
	{
		CacheManager.getCacheManager();
	}

	private void loadConnections()
	{
		ConnectionManager.getConnectionManager();
	}

	private void loadUserPrivileges()
	{
		UserManager.getUserManager();
	}
	
	static
	{
		getDBManager();
	}
	private void loadDatabases()
	{
		try
		{
			ObjectInputStream jdbin = new ObjectInputStream(new FileInputStream(new File("jdb_databases.dat")));
			databases = (HashMap<String,Database>)jdbin.readObject();
			jdbin.close();
			
		}
		catch (FileNotFoundException e)
		{
			databases = new HashMap<String,Database>();
			try
			{
				ObjectOutputStream jdbout = new ObjectOutputStream(new FileOutputStream(new File("jdb_databases.dat")));
				jdbout.writeObject(databases);
				jdbout.close();
			}
			catch (FileNotFoundException e1)
			{
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			catch (IOException e1)
			{
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		catch (IOException e)
		{
			databases = new HashMap<String,Database>();
			try
			{
				ObjectOutputStream jdbout = new ObjectOutputStream(new FileOutputStream(new File("jdb_databases.dat")));
				jdbout.writeObject(databases);
				jdbout.close();
			}
			catch (FileNotFoundException e1)
			{
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			catch (IOException e1)
			{
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		catch (ClassNotFoundException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static DBManager getDBManager()
	{
		if(dbManager==null) 
		{
			synchronized(DBManager.class)
			{
				if(dbManager==null)
				{
					dbManager= new DBManager();
				}
			}
		}
		return dbManager;
	}
	public static void persist()
	{
		try
		{
			synchronized (DBManager.class)
			{	
				ObjectOutputStream jdbout = new ObjectOutputStream(new FileOutputStream(new File("jdb_databases.dat")));
				jdbout.writeObject(getDBManager().databases);
				jdbout.close();
			}
		}
		catch (FileNotFoundException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	protected static void shutdown()
	{
		try
		{
			Thread.sleep(10000);
		}
		catch (InterruptedException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		persist();
		ConnectionManager.getConnectionManager().shutdown();
	}
	
	public static boolean createDatabase(String name,String user,String password)
	{
		boolean flag = false;
		Database db =  new Database(name,user);
		getDBManager().databases.put(name, db);
		persist();
		return flag;
	}
	
	public static boolean createTable(String dbname,String tableName,String user,
			String password,String[] colTypesNames,String[] misc,Map<String,String> mapping,
			int algo)
	{
		boolean flag = false;
		Table table =  new Table(dbname,tableName,colTypesNames,misc,mapping,algo);
		getDBManager().databases.get(dbname).newTable(tableName,table);
		persist();
		return flag;
	}
	
	public static boolean alterTable(String dbname,String tableName,String user,
			String password,String[] colTypesNames,String[] misc)
	{
		boolean flag = false;
		getDBManager().databases.get(dbname).alterTable(tableName, colTypesNames, misc);
		persist();
		return flag;
	}
	
	public static Table getTable(String dbname,String tableName)
	{
		return getDBManager().databases.get(dbname).getTable(tableName);
	}
}
