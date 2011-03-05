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
import java.io.InputStream;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import com.amef.AMEFEncodeException;
import com.amef.JDBObject;
import com.server.JdbFlusher;

public class Connection
{
	public Connection()
	{
		
	}
	private FileOutputStream bos;
	
	private boolean status;
	
	private boolean trxStatus;
	
	private Map<String,Object> trxOpers;
	
	private <T> List<T> selectColumnsWhere(String dbname,String tableName,String[] cols,String where)
	{
		String filter = "SOME";	
		boolean whrcls = true;
		List<T> allObjects = DBManager.getTable(dbname, tableName).getObjects();
		if(cols==null || cols.length==0)
			filter = "ALL";
		if(where==null || "".equals(where))
			whrcls = false;
		return allObjects;
	}
	
	public byte[] selectColumnsServer(String dbname,String tableName,String[] cols,String where)
	{
		String filter = "SOME";	
		boolean whrcls = true;
		byte[] data = DBManager.getTable(dbname, tableName).getObjectsStream();
		if(cols==null || cols.length==0)
			filter = "ALL";
		if(where==null || "".equals(where))
			whrcls = false;
		return data;
	}
	
	public <T> List<T> selectAll(String dbname,String table,String where)
	{
		return selectColumnsWhere(dbname,table,null,where);
	}
	
	public List<JDBObject> selectAMEFObject(String dbname,String tableName,String where)
	{
		return DBManager.getTable(dbname, tableName).getAMEFObjects();
	}
	/*public void selectAMEFObjectsb(final String dbname,final String tableName,
			String where,final Queue<Object> q, final String subq)
	{
		new Thread(new Runnable()
		{
			public void run()
			{
				InputStream jdbin = null;				
				try
				{
					Table table = DBManager.getTable(dbname, tableName);
					JDBObject objtab = new JDBObject();
					for (int i = 0; i < table.getColumnNames().length; i++)
					{
						objtab.addPacket(table.getColumnTypes()[i],table.getColumnNames()[i]);
					}
					q.add(JdbResources.getEncoder().encode(objtab, false));
					JDBObject objtab1 = new JDBObject();
					if(table.getMapping()!=null && table.getMapping().size()>0)
					{						
						for (Iterator<Map.Entry<String,String>> iter = table.getMapping().entrySet().iterator(); iter.hasNext();)
						{
							Map.Entry<String,String> entry = iter.next();
							objtab1.addPacket(entry.getValue(),entry.getKey());
						}						
					}	
					q.add(JdbResources.getEncoder().encode(objtab1, false));
					if(table.getRecords()==0)
					{
						q.add("DONE");
						return;
					}
					jdbin = new FileInputStream(new File(table.getFileName()));
					table.getAMEFObjectsb(q,jdbin,subq,objtab);
					q.add("DONE");
				}
				catch (FileNotFoundException e)
				{
					e.printStackTrace();
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
				finally
				{
					try
					{
						if(jdbin!=null)
							jdbin.close();
					}
					catch (IOException e)
					{
						e.printStackTrace();
					}
				}
			}
		}).start();
	}
	
	
	public <T> List<T> select(String dbname,String table,String[] cols,String where)
	{
		return selectColumnsWhere(dbname,table,cols,where);
	}
	
	public boolean insert(String dbname,String tableName,JDBObject row)
	{
		Table table = DBManager.getTable(dbname, tableName);
		table.incRecords();
		try
		{
			bos = new FileOutputStream(new File(table.getFileName()),true);
			bos.write(JdbResources.getEncoder().encode(row, false));
			bos.flush();
			DBManager.persist();
		}
		catch (FileNotFoundException e)
		{
			try
			{
				new File(table.getFileName()).createNewFile();
				return false;
			}
			catch (IOException e1)
			{
				e1.printStackTrace();return false;
			}				
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();return false;
		}
		catch (AMEFEncodeException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();return false;
		}
		return true;
		//DBManager.getTable(dbname, tableName).storeObject(row,flusher);
	}
	
	public boolean insert(String dbname,String tableName,String row)
	{
		Table table = DBManager.getTable(dbname, tableName);
		table.incRecords();
		try
		{
			bos = new FileOutputStream(new File(table.getFileName()),true);
			bos.write(row.getBytes());
			bos.flush();
			DBManager.persist();
		}
		catch (FileNotFoundException e)
		{
			try
			{
				new File(table.getFileName()).createNewFile();
				return false;
			}
			catch (IOException e1)
			{
				e1.printStackTrace();return false;
			}				
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();return false;
		}
		return true;
		//DBManager.getTable(dbname, tableName).storeObject(row,flusher);
	}*/
		
	
	
	public void update(String dbname,String table,String[] cols,String where)
	{
		
	}
	
	public void delete(String dbname,String table,String where)
	{
		
	}
	
	public void truncate(String dbname,String table)
	{
		
	}	
	
	public void startTransaction()
	{
		trxOpers = new LinkedHashMap<String, Object>();
	}
	
	public void rollback()
	{
		
	}
}
