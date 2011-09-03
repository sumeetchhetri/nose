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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.amef.AMEFObject;

public final class JdbMemoryStore
{
	static class TableData
	{
		private Table table;
		private Queue<Object> data;
		boolean done;
		public TableData(Table table, Queue<Object> data)
		{
			super();
			this.table = table;
			this.data = new ConcurrentLinkedQueue<Object>();
			if(data!=null)
				this.data.addAll(data);
			done = false;
		}		
	}
	private JdbMemoryStore() throws Exception
	{
		store = new HashMap<String, TableData>();
		/*for (Map.Entry<String,Table> entry : DBManager.getDBManager().databases.get("temp").tables.entrySet())
		{
			Queue<Object> q = new ConcurrentLinkedQueue<Object>();
			AMEFObject objtab = new AMEFObject();
			objtab.setName("TABLE_COLUMN_DET");
			for (int j = 0; j < entry.getValue().getColumnNames().length; j++)
			{
				objtab.addPacket(entry.getValue().getColumnTypes()[j],entry.getValue().getColumnNames()[j]);
			}
			for (int i = 0; i < entry.getValue().getAlgo(); i++)
			{
				InputStream jdbin = new FileInputStream(new File(entry.getValue().getFileName(i)));								
				entry.getValue().getAMEFObjectsb(q,jdbin,"",objtab);
			}
			AMEFObject objtab1 = new AMEFObject();
			objtab1.setName("TABLE_CLASS_MAP");
			if(entry.getValue().getMapping()!=null && entry.getValue().getMapping().size()>0)
			{						
				for (Iterator<Map.Entry<String,String>> iter = entry.getValue().getMapping().entrySet().iterator(); iter.hasNext();)
				{
					Map.Entry<String,String> eentry = iter.next();
					objtab1.addPacket(eentry.getValue(),eentry.getKey());
				}						
			}
			TableData tabData = new TableData(entry.getValue(),q);
			store.put(entry.getKey(), tabData);
		}*/
	}
	
	public void selectFromStore(Queue<Object> q, Table tab, int index) throws Exception
	{
		if(store.get(tab.getName())!=null && store.get(tab.getName()).done)
		{
			if(index==0)
				q.addAll(store.get(tab.getName()).data);
			return;
		}
		AMEFObject objtab = new AMEFObject();
		objtab.setName("TABLE_COLUMN_DET");
		for (int j = 0; j < tab.getColumnNames().length; j++)
		{
			objtab.addPacket(tab.getColumnTypes()[j],tab.getColumnNames()[j]);
		}
		synchronized(store)
		{
			if(store.get(tab.getName())==null)
				store.put(tab.getName(), new TableData(tab,null));
		}
		TableData tabData = store.get(tab.getName());
		InputStream jdbin = new FileInputStream(new File(tab.getFileName(index)));
		tab.getAMEFObjectsb(tabData.data,jdbin,"",objtab,null);
		if(index==1)
		{
			tabData.done = true;
			q.addAll(store.get(tab.getName()).data);
		}
	}
	
	private volatile static JdbMemoryStore memoryStore;
	
	private Map<String,TableData> store;
	
	public static JdbMemoryStore getJdbMemoryStore() throws Exception
	{
		if(memoryStore==null) 
		{
			synchronized(DBManager.class)
			{
				if(memoryStore==null)
				{
					memoryStore= new JdbMemoryStore();
				}
			}
		}
		return memoryStore;
	}	
}
