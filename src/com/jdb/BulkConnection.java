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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;

import com.amef.AMEFEncodeException;
import com.amef.JDBObject;
import com.jdb.Table.AggInfo;
import com.jdb.Table.Opval;
import com.server.JdbFlusher;

@SuppressWarnings("unchecked")
public class BulkConnection
{
	ScheduledExecutorService workerThreadPool = null;
	
	FutureTask[] futures = null;
	
	String tableName = null;
	
	Thread thr = null;
	
	public BulkConnection()
	{
		futures = new FutureTask[4];		
		flusher = new JdbFlusher();
		thr = new Thread(flusher,"Flusher Thread");
		thr.start();
	}
	
	private JdbFlusher flusher;
	
	public JdbFlusher getFlusher()
	{
		return flusher;
	}
	
	
	public void setFlusher(JdbFlusher flusher)
	{
		this.flusher = flusher;
	}

	/*private <T> List<T> selectColumnsWhere(String dbname,String tableName,String[] cols,String where)
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
	}*/
	private Map<String,Integer> getPositions(JDBObject objtab)
	{
		Map<String,Integer> whrandcls = new HashMap<String, Integer>();
		for (int i = 0; i < objtab.getPackets().size(); i++)
		{
			whrandcls.put(objtab.getPackets().get(i).getNameStr(),i);
		}
		return whrandcls;
	}
	
	private Map<String,Integer> getPositions(JDBObject objtab,String table)
	{
		Map<String,Integer> whrandcls = new HashMap<String, Integer>();
		for (int i = 0; i < objtab.getPackets().size(); i++)
		{
			whrandcls.put(table+"."+objtab.getPackets().get(i).getNameStr(),i);
		}
		return whrandcls;
	}
	
	private String getOper(String ter) throws Exception
	{
		String oper = "";
		if(ter.indexOf("!=")!=-1)
		{
			oper = "!=";
		}
		else if(ter.indexOf(">=")!=-1)
		{
			oper = ">=";
		}
		else if(ter.indexOf("<=")!=-1)
		{
			oper = "<=";
		}
		else if(ter.indexOf("==")!=-1)
		{
			oper = "==";
		}
		else if(ter.indexOf("=")!=-1)
		{
			oper = "=";
		}
		else if(ter.indexOf("<>")!=-1)
		{
			oper = "<>";
		}
		else if(ter.indexOf(">")!=-1)
		{
			oper = ">";
		}
		else if(ter.indexOf("<")!=-1)
		{
			oper = "<";
		}
		else if(ter.toLowerCase().indexOf(" like ")!=-1)
		{
			oper = " like ";
		}
		else if(ter.toLowerCase().indexOf(" in ")!=-1)
		{
			oper = " in ";
		}
		else 
			throw new Exception("Invalid operator");
		return oper;
	}
	
	private String[] getConditionsf(String subq,Map<String,Integer> map,String[] alias,String[] table) throws Exception
	{
		String[] condition = new String[]{"",""};
		if(subq!=null && !subq.equals(""))
		{
			while(subq.indexOf(" and ")!=-1 || subq.indexOf(" or ")!=-1)
			{
				int anop = subq.indexOf(" and ");
				int orop = subq.indexOf(" or ");
				if((anop<orop && anop!=-1) || (anop!=-1 && orop==-1))
				{
					String ter = subq.split(" and ")[0].trim();
					String oper = getOper(ter);
					String key = ter.split(oper)[0];
					if(key.indexOf(".")!=-1 && key.split("\\.")[0].equals(alias))
						key = key.split("\\.")[1];
					if(map.containsKey(key) && (isAConstant(ter.split(oper)[1])) || map.containsKey(ter.split(oper)[1]))
					{
						
					}
					else
					{
						if(condition[1].equals(""))
							condition[1] += ter.split(oper)[0] + oper + ter.split(oper)[1];
						else
							condition[1] += " and " + ter.split(oper)[0] + oper + ter.split(oper)[1];
					}
					subq = subq.split(" and ")[1];
				}
				else if(orop!=-1)
				{
					String ter = subq.split(" or ")[0].trim();
					String oper = getOper(ter);
					String key = ter.split(oper)[0];
					if(key.indexOf(".")!=-1 && key.split("\\.")[0].equals(alias))
						key = key.split("\\.")[1];
					if(map.containsKey(key) && (isAConstant(ter.split(oper)[1])) || map.containsKey(ter.split("=")[1]))
					{
						
					}
					else
					{
						if(condition[1].equals(""))
							condition[1] += ter.split(oper)[0] + oper + ter.split(oper)[1];
						else
							condition[1] += " or " + ter.split(oper)[0] + oper + ter.split(oper)[1];
					}
					subq = subq.split(" or ")[1];
				}
			}
			String oper = getOper(subq);
			String key = subq.split(oper)[0];
			if(key.indexOf(".")!=-1 && key.split("\\.")[0].equals(alias))
				key = key.split("\\.")[1];
			if(map.containsKey(key) && (isAConstant(subq.split(oper)[1]) 
					|| map.containsKey(subq.split(oper)[1]) 
					|| (subq.split(oper)[1].indexOf(".")!=-1 && 
							map.containsKey(subq.split(oper)[1].split("\\.")[1]) 
							&& subq.split(oper)[1].split("\\.")[1].equals(alias))))
			{	
				
			}
			else
			{
				String key1=subq.split(oper)[0],key2=subq.split(oper)[1];
				if(subq.split(oper)[0].indexOf(".")!=-1)
				{
					for (int i = 0; i < alias.length; i++)
					{
						if(subq.split(oper)[0].split("\\.")[0].equals(alias[i]))
						{
							key1 = table[i] + "." +  subq.split(oper)[0].split("\\.")[1];
							break;
						}
					}						
				}
				if(subq.split(oper)[1].indexOf(".")!=-1)
				{
					for (int i = 0; i < alias.length; i++)
					{
						if(subq.split(oper)[1].split("\\.")[0].equals(alias[i]))
						{
							key2 = table[i] + "." +  subq.split(oper)[1].split("\\.")[1];
							break;
						}
					}							
				}
				if(condition[1].equals(""))
				{				
					condition[1] += key1 + oper + key2;
				}
				else
				{
					condition[1] += " and " + key1 + oper + key2;
				}
			}
		}
		return condition;
	}
	
	private String[] getConditions(String subq,Map<String,Integer> map,String alias,String table) throws Exception
	{
		String[] condition = new String[]{"",""};
		if(subq!=null && !subq.equals(""))
		{
			while(subq.indexOf(" and ")!=-1 || subq.indexOf(" or ")!=-1)
			{
				int anop = subq.indexOf(" and ");
				int orop = subq.indexOf(" or ");
				if((anop<orop && anop!=-1) || (anop!=-1 && orop==-1))
				{
					String ter = subq.split(" and ")[0].trim();
					String oper = getOper(ter);
					String key = ter.split(oper)[0];
					if(key.indexOf(".")!=-1 && key.split("\\.")[0].equals(alias))
						key = key.split("\\.")[1];
					if(map.containsKey(key) && (isAConstant(ter.split(oper)[1])) || map.containsKey(ter.split(oper)[1]))
					{
						if(condition[0].equals(""))
							condition[0] += key + oper + ter.split(oper)[1];
						else
							condition[0] += " and " + key + oper + ter.split(oper)[1];
					}
					else
					{
						if(condition[1].equals(""))
							condition[1] += ter.split(oper)[0] + oper + ter.split(oper)[1];
						else
							condition[1] += " and " + ter.split(oper)[0] + oper + ter.split(oper)[1];
					}
					subq = subq.split(" and ")[1];
				}
				else if(orop!=-1)
				{
					String ter = subq.split(" or ")[0].trim();
					String oper = getOper(ter);
					String key = ter.split(oper)[0];
					if(key.indexOf(".")!=-1 && key.split("\\.")[0].equals(alias))
						key = key.split("\\.")[1];
					if(map.containsKey(key) && (isAConstant(ter.split(oper)[1])) || map.containsKey(ter.split("=")[1]))
					{
						if(condition[0].equals(""))
							condition[0] += key + oper + ter.split("=")[1];
						else
							condition[0] += " or " + key + oper + ter.split(oper)[1];
					}
					else
					{
						if(condition[1].equals(""))
							condition[1] += ter.split(oper)[0] + oper + ter.split(oper)[1];
						else
							condition[1] += " or " + ter.split(oper)[0] + oper + ter.split(oper)[1];
					}
					subq = subq.split(" or ")[1];
				}
			}
			String oper = getOper(subq);
			String key = subq.split(oper)[0];
			if(key.indexOf(".")!=-1 && key.split("\\.")[0].equals(alias))
				key = key.split("\\.")[1];
			if(map.containsKey(key) && (isAConstant(subq.split(oper)[1]) 
					|| map.containsKey(subq.split(oper)[1]) 
					|| (subq.split(oper)[1].indexOf(".")!=-1 && 
							map.containsKey(subq.split(oper)[1].split("\\.")[1]) 
							&& subq.split(oper)[1].split("\\.")[1].equals(alias))))
			{	
				if(subq.split(oper)[1].indexOf(".")!=-1 && 
							map.containsKey(subq.split(oper)[1].split("\\.")[1]) 
							&& subq.split(oper)[1].split("\\.")[0].equals(alias))
				{
					if(condition[0].equals(""))
						condition[0] += key + oper + subq.split(oper)[1].split("\\.")[1];
					else
						condition[0] += " and " + key + oper + subq.split(oper)[1].split("\\.")[1];
				}
				else 
				{	if(condition[0].equals(""))
						condition[0] += key + oper + subq.split(oper)[1];
					else
						condition[0] += " and " + key + oper + subq.split(oper)[1];
				}
			}
			else
			{
				if(condition[1].equals(""))
				{
					String key1=subq.split(oper)[0],key2=subq.split(oper)[1];
					if(subq.split(oper)[0].indexOf(".")!=-1 && subq.split(oper)[0].split("\\.")[0].equals(alias))
						key1 = table + "." +  subq.split(oper)[0].split("\\.")[1];
					else if(subq.split(oper)[1].indexOf(".")!=-1 && subq.split(oper)[1].split("\\.")[0].equals(alias))
						key2 = table + "." +  subq.split(oper)[0].split("\\.")[1];
					condition[1] += key1 + oper + key2;
				}
				else
				{
					String key1=subq.split(oper)[0],key2=subq.split(oper)[1];
					if(subq.split(oper)[0].indexOf(".")!=-1 && subq.split(oper)[0].split("\\.")[0].equals(alias))
						key1 = table + "." +  subq.split(oper)[0].split("\\.")[1];
					else if(subq.split(oper)[1].indexOf(".")!=-1 && subq.split(oper)[1].split("\\.")[0].equals(alias))
						key2 = table + "." +  subq.split(oper)[0].split("\\.")[1];
					condition[1] += " and " + key1 + oper + key2;
				}
			}
		}
		return condition;
	}
	
	
	private boolean isAConstant(String string) 
	{
		if(string.charAt(0)=='"' || string.charAt(0)=='\'')
			return true;
		else if(string.indexOf(".")==-1)
		{
			try
			{			
				Long.parseLong(string);
				return true;
			}
			catch(Exception e)
			{}
		}
		else
		{
			try
			{			
				Double.parseDouble(string);
				return true;
			}
			catch(Exception e)
			{}
		}
		return false;
	}
	
	
	private static boolean isAConstantS(String string) 
	{
		if(string.charAt(0)=='"' || string.charAt(0)=='\'')
			return true;
		else if(string.indexOf(".")==-1)
		{
			try
			{			
				Long.parseLong(string);
				return true;
			}
			catch(Exception e)
			{}
		}
		else
		{
			try
			{			
				Double.parseDouble(string);
				return true;
			}
			catch(Exception e)
			{}
		}
		return false;
	}
	
	static class Opval
	{
		public String value,operator;
		public Opval(String value, String operator)
		{
			super();
			this.value = value;
			this.operator = operator;
		}
		
	}
	
	private static void getLogicalValue(String ter,List<Boolean> whrandcls,
			Map<String,Integer> map1, Map<String,Integer> map2,
			String alias1, String alias2,JDBObject objec1,JDBObject objec2) throws Exception
	{
		String oper = "";
		if(ter.indexOf("!=")!=-1)
		{
			oper = "!=";
		}
		else if(ter.indexOf(">=")!=-1)
		{
			oper = ">=";
		}
		else if(ter.indexOf("<=")!=-1)
		{
			oper = "<=";
		}
		else if(ter.indexOf("==")!=-1)
		{
			oper = "==";
		}
		else if(ter.indexOf("=")!=-1)
		{
			oper = "=";
		}
		else if(ter.indexOf("<>")!=-1)
		{
			oper = "<>";
		}
		else if(ter.indexOf(">")!=-1)
		{
			oper = ">";
		}
		else if(ter.indexOf("<")!=-1)
		{
			oper = "<";
		}
		else if(ter.toLowerCase().indexOf(" like ")!=-1)
		{
			oper = " like ";
		}
		else if(ter.toLowerCase().indexOf(" in ")!=-1)
		{
			oper = " in ";
		}
		else 
			throw new Exception("Invalid operator");
		String key1 = ter.split(oper)[0];		
		String key2 = ter.split(oper)[1];
		char type1='!',type2='!';
		byte[] val1 = null,val2 = null;
		if(map1.containsKey(key1))
		{
			val1 = objec1.getPackets().get(map1.get(key1)).getValue();
			type1 = objec1.getPackets().get(map1.get(key1)).getType();
		}
		/*else if(map2.containsKey(key1))
		{
			val1 = objec2.getPackets().get(map2.get(key1)).getValue();
			type1 = objec2.getPackets().get(map2.get(key1)).getType();
		}
		else if(key1.indexOf(".")!=-1)
		{
			if(key1.split("\\.")[0].equals(alias1) && map1.containsKey(key1.split("\\.")[1]))
			{
				val1 = objec1.getPackets().get(map1.get(key1.split("\\.")[1])).getValue();
				type1 = objec1.getPackets().get(map1.get(key1.split("\\.")[1])).getType();
			}
			else if(key1.split("\\.")[0].equals(alias2) && map2.containsKey(key1.split("\\.")[1]))
			{
				val1 = objec2.getPackets().get(map2.get(key1.split("\\.")[1])).getValue();
				type1 = objec2.getPackets().get(map2.get(key1.split("\\.")[1])).getType();
			}
			else throw new Exception("Invalid column name");
		}*/
		if(map1.containsKey(key2))
		{
			val2 = objec1.getPackets().get(map1.get(key2)).getValue();
			type2 = objec1.getPackets().get(map1.get(key2)).getType();
		}
		/*else if(map2.containsKey(key2))
		{
			val2 = objec2.getPackets().get(map2.get(key2)).getValue();
			type2 = objec2.getPackets().get(map2.get(key2)).getType();
		}
		else if(key2.indexOf(".")!=-1)
		{
			if(key2.split("\\.")[0].equals(alias1) && map1.containsKey(key2.split("\\.")[1]))
			{
				val2 = objec1.getPackets().get(map1.get(key2.split("\\.")[1])).getValue();
				type2 = objec1.getPackets().get(map1.get(key2.split("\\.")[1])).getType();
			}
			else if(key2.split("\\.")[0].equals(alias2) && map2.containsKey(key2.split("\\.")[1]))
			{
				val2 = objec2.getPackets().get(map2.get(key2.split("\\.")[1])).getValue();
				type2 = objec2.getPackets().get(map2.get(key2.split("\\.")[1])).getType();
			}
			else throw new Exception("Invalid column name");
		}*/
		whrandcls.add(eval(val1, val2, type1, type2, oper));
	}
	
	
	private static void getLogicalValue2(String ter,List<Boolean> whrandcls,
			Map<String,Integer> map1, Map<String,Integer> map2,
			String alias1, String alias2,JDBObject objec1,JDBObject objec2) throws Exception
	{
		String oper = "";
		if(ter.indexOf("!=")!=-1)
		{
			oper = "!=";
		}
		else if(ter.indexOf(">=")!=-1)
		{
			oper = ">=";
		}
		else if(ter.indexOf("<=")!=-1)
		{
			oper = "<=";
		}
		else if(ter.indexOf("==")!=-1)
		{
			oper = "==";
		}
		else if(ter.indexOf("=")!=-1)
		{
			oper = "=";
		}
		else if(ter.indexOf("<>")!=-1)
		{
			oper = "<>";
		}
		else if(ter.indexOf(">")!=-1)
		{
			oper = ">";
		}
		else if(ter.indexOf("<")!=-1)
		{
			oper = "<";
		}
		else if(ter.toLowerCase().indexOf(" like ")!=-1)
		{
			oper = " like ";
		}
		else if(ter.toLowerCase().indexOf(" in ")!=-1)
		{
			oper = " in ";
		}
		else 
			throw new Exception("Invalid operator");
		String key1 = ter.split(oper)[0];		
		String key2 = ter.split(oper)[1];
		char type1='!',type2='!';
		byte[] val1 = null,val2 = null;
		if(map1.containsKey(key1))
		{
			val1 = objec1.getPackets().get(map1.get(key1)).getValue();
			type1 = objec1.getPackets().get(map1.get(key1)).getType();
		}
		else if(map2.containsKey(key1))
		{
			val1 = objec2.getPackets().get(map2.get(key1)).getValue();
			type1 = objec2.getPackets().get(map2.get(key1)).getType();
		}
		else if(key1.indexOf(".")!=-1)
		{
			if(key1.split("\\.")[0].equals(alias1) && map1.containsKey(key1.split("\\.")[1]))
			{
				val1 = objec1.getPackets().get(map1.get(key1.split("\\.")[1])).getValue();
				type1 = objec1.getPackets().get(map1.get(key1.split("\\.")[1])).getType();
			}
			else if(key1.split("\\.")[0].equals(alias2) && map2.containsKey(key1.split("\\.")[1]))
			{
				val1 = objec2.getPackets().get(map2.get(key1.split("\\.")[1])).getValue();
				type1 = objec2.getPackets().get(map2.get(key1.split("\\.")[1])).getType();
			}
			else throw new Exception("Invalid column name");
		}
		if(map1.containsKey(key2))
		{
			val2 = objec1.getPackets().get(map1.get(key2)).getValue();
			type2 = objec1.getPackets().get(map1.get(key2)).getType();
		}
		else if(map2.containsKey(key2))
		{
			val2 = objec2.getPackets().get(map2.get(key2)).getValue();
			type2 = objec2.getPackets().get(map2.get(key2)).getType();
		}
		else if(key2.indexOf(".")!=-1)
		{
			if(key2.split("\\.")[0].equals(alias1) && map1.containsKey(key2.split("\\.")[1]))
			{
				val2 = objec1.getPackets().get(map1.get(key2.split("\\.")[1])).getValue();
				type2 = objec1.getPackets().get(map1.get(key2.split("\\.")[1])).getType();
			}
			else if(key2.split("\\.")[0].equals(alias2) && map2.containsKey(key2.split("\\.")[1]))
			{
				val2 = objec2.getPackets().get(map2.get(key2.split("\\.")[1])).getValue();
				type2 = objec2.getPackets().get(map2.get(key2.split("\\.")[1])).getType();
			}
			else throw new Exception("Invalid column name");
		}
		whrandcls.add(eval(val1, val2, type1, type2, oper));
	}
	
	private static void getLogicalValueHv(String ter,List<Boolean> whrandcls,
			Map<String,Object> map1,JDBObject objec1, 
			LinkedHashMap<Object, com.jdb.BulkConnection.AggInfo> aggVals, Map<String, Character> mpst) throws Exception
	{
		String oper = "";
		if(ter.indexOf("!=")!=-1)
		{
			oper = "!=";
		}
		else if(ter.indexOf(">=")!=-1)
		{
			oper = ">=";
		}
		else if(ter.indexOf("<=")!=-1)
		{
			oper = "<=";
		}
		else if(ter.indexOf("==")!=-1)
		{
			oper = "==";
		}
		else if(ter.indexOf("=")!=-1)
		{
			oper = "=";
		}
		else if(ter.indexOf("<>")!=-1)
		{
			oper = "<>";
		}
		else if(ter.indexOf(">")!=-1)
		{
			oper = ">";
		}
		else if(ter.indexOf("<")!=-1)
		{
			oper = "<";
		}
		else if(ter.toLowerCase().indexOf(" like ")!=-1)
		{
			oper = " like ";
		}
		else if(ter.toLowerCase().indexOf(" in ")!=-1)
		{
			oper = " in ";
		}
		else 
			throw new Exception("Invalid operator");
		String key1 = ter.split(oper)[0];		
		String key2 = ter.split(oper)[1];
		
		Object val1 = null,val2 = null;
		if(aggVals.get(key1)==null && aggVals.get(key2)==null)
			throw new Exception("");
		if(map1.containsKey(key1))
		{
			val1 = map1.get(key1);
		}
		if(map1.containsKey(key2))
		{
			val2 = map1.get(key2);
		}
		if(val1!=null && val2!=null)
		{
			if(val1 instanceof byte[])
				whrandcls.add(Arrays.equals((byte[])val1, (byte[])val2));
			else
				whrandcls.add(val1.equals(val2));
		}
		else if(val1!=null && val2==null && isAConstantS(key2))
		{
			if(JDBObject.isNumber(mpst.get(key1)))
			{
				if(val1 instanceof byte[])
				{
					whrandcls.add(eval((byte[])val1, 
							JdbResources.longToByteArray(Long.parseLong(key2), 8), 
							mpst.get(key1), mpst.get(key1), oper));
				}
				else if(val1 instanceof Long)
				{
					whrandcls.add(val1==Long.valueOf(key2));
				}
			}
			else if(JDBObject.isFloatingPoint(mpst.get(key1)))
			{
				if(val1 instanceof byte[])
				{
					whrandcls.add(eval((byte[])val1, 
							key2.getBytes(), 
							mpst.get(key1), mpst.get(key1), oper));
				}
			}
			else if(JDBObject.isString(mpst.get(key1)))
			{
				if(val1 instanceof byte[])
				{
					if(key2.startsWith("'") || key2.startsWith("\""))
						key2 = key2.substring(1,key2.length()-1);
					whrandcls.add(eval((byte[])val1, 
							key2.getBytes(), 
							mpst.get(key1), mpst.get(key1), oper));
				}
			}
		}
		else if(val1==null && val2!=null && isAConstantS(key1))
		{
			if(JDBObject.isNumber(mpst.get(key2)))
			{
				if(val2 instanceof byte[])
				{
					whrandcls.add(eval((byte[])val2, 
							JdbResources.longToByteArray(Long.parseLong(key1), 8), 
							mpst.get(key2), mpst.get(key2), oper));
				}
				else if(val2 instanceof Long)
				{
					whrandcls.add(val2==Long.valueOf(key1));
				}
			}
			else if(JDBObject.isFloatingPoint(mpst.get(key2)))
			{
				if(val2 instanceof byte[])
				{
					whrandcls.add(eval((byte[])val2, 
							key1.getBytes(), 
							mpst.get(key2), mpst.get(key2), oper));
				}
			}
			else if(JDBObject.isString(mpst.get(key2)) || JDBObject.isDate(mpst.get(key2)))
			{
				if(val2 instanceof byte[])
				{
					if(key1.startsWith("'") || key1.startsWith("\""))
						key1 = key1.substring(1,key1.length()-1);
					whrandcls.add(eval((byte[])val2, 
							key1.getBytes(), 
							mpst.get(key2), mpst.get(key2), oper));
				}
			}
		}
		else
		{
			whrandcls.add(eval(key1.getBytes(), 
					key2.getBytes(), 
					't', 't', oper));
		}
	}
	
	
	private static boolean eval(byte[] left,byte[] right,char type1, char type2, String operator) throws Exception
	{
		if(JDBObject.isString(type1) && JDBObject.isString(type2))
		{
			String righ = new String(right);
			if(righ.charAt(0)=='\'' || righ.charAt(0)=='"')
				righ = righ.substring(1,righ.length()-1);
			if(operator.equals("=") || operator.equals("=="))
				return new String(left).equals(righ);
			else if(operator.equals("!=") || operator.equals("<>"))
				return !new String(left).equals(righ);
			else if(operator.equals(">"))
				return new String(left).compareTo(righ)<0;
			else if(operator.equals("<"))
				return new String(left).compareTo(righ)>0;
			else if(operator.equals(">="))
				return new String(left).compareTo(righ)<0 || new String(left).equals(righ);
			else if(operator.equals("<="))
				return new String(left).compareTo(righ)>0 || new String(left).equals(righ);
			else if(operator.equals(" like "))
				return new String(left).indexOf(righ)!=-1;
		}
		else if(JDBObject.isInteger(type1) && JDBObject.isInteger(type2))
		{
			if(operator.equals("=") || operator.equals("=="))
				return JdbResources.byteArrayToInt(left) == JdbResources.byteArrayToInt(right);
			else if(operator.equals("!=") || operator.equals("<>"))
				return JdbResources.byteArrayToInt(left) != JdbResources.byteArrayToInt(right);
			else if(operator.equals(">"))
				return JdbResources.byteArrayToInt(left) > JdbResources.byteArrayToInt(right);
			else if(operator.equals("<"))
				return JdbResources.byteArrayToInt(left) < JdbResources.byteArrayToInt(right);
			else if(operator.equals(">="))
				return JdbResources.byteArrayToInt(left) >= JdbResources.byteArrayToInt(right);
			else if(operator.equals("<="))
				return JdbResources.byteArrayToInt(left) <= JdbResources.byteArrayToInt(right);
			else if(operator.equals(" like "))
				return String.valueOf(JdbResources.byteArrayToInt(left)).indexOf(new String(""+JdbResources.byteArrayToInt(right)))!=-1;
		}
		else if(JDBObject.isNumber(type1) && JDBObject.isNumber(type2))
		{
			if(operator.equals("=") || operator.equals("=="))
				return JdbResources.byteArrayToLong(left) == JdbResources.byteArrayToLong(right);
			else if(operator.equals("!=") || operator.equals("<>"))
				return JdbResources.byteArrayToLong(left) != JdbResources.byteArrayToLong(right);
			else if(operator.equals(">"))
				return JdbResources.byteArrayToLong(left) > JdbResources.byteArrayToLong(right);
			else if(operator.equals("<"))
				return JdbResources.byteArrayToLong(left) < JdbResources.byteArrayToLong(right);
			else if(operator.equals(">="))
				return JdbResources.byteArrayToLong(left) >= JdbResources.byteArrayToLong(right);
			else if(operator.equals("<="))
				return JdbResources.byteArrayToLong(left) <= JdbResources.byteArrayToLong(right);
			else if(operator.equals(" like "))
				return String.valueOf(JdbResources.byteArrayToInt(left)).indexOf(new String(""+JdbResources.byteArrayToLong(right)))!=-1;
		}
		else if(JDBObject.isFloatingPoint(type1) && JDBObject.isFloatingPoint(type2))
		{
			String righ = new String(right);
			if(operator.equals("=") || operator.equals("=="))
				return new String(left).equals(righ);
			else if(operator.equals("!=") || operator.equals("<>"))
				return !new String(left).equals(righ);
			else if(operator.equals(">"))
				return new String(left).compareTo(righ)<0;
			else if(operator.equals("<"))
				return new String(left).compareTo(righ)>0;
			else if(operator.equals(">="))
				return new String(left).compareTo(righ)<0 || new String(left).equals(righ);
			else if(operator.equals("<="))
				return new String(left).compareTo(righ)>0 || new String(left).equals(righ);
			else if(operator.equals(" like "))
				return new String(left).indexOf(righ)!=-1;
		}
		else if(JDBObject.isDate(type1) && JDBObject.isDate(type2))
		{
			String righ = new String(right);
			if(operator.equals("=") || operator.equals("=="))
				return new String(left).equals(righ);
			else if(operator.equals("!=") || operator.equals("<>"))
				return !new String(left).equals(righ);
			else if(operator.equals(">"))
				return new String(left).compareTo(righ)<0;
			else if(operator.equals("<"))
				return new String(left).compareTo(righ)>0;
			else if(operator.equals(">="))
				return new String(left).compareTo(righ)<0 || new String(left).equals(righ);
			else if(operator.equals("<="))
				return new String(left).compareTo(righ)>0 || new String(left).equals(righ);
			else if(operator.equals(" like "))
				return new String(left).indexOf(righ)!=-1;
		}
		else if(JDBObject.isBoolean(type1) && JDBObject.isBoolean(type2))
		{
			if(operator.equals("=") || operator.equals("=="))
				return left[0]==right[0];
			else if(operator.equals("!=") || operator.equals("<>"))
				return left[0]!=right[0];
			else if(operator.equals(" like "))
				return left[0]==right[0];
		}
		else if(JDBObject.isChar(type1) && JDBObject.isChar(type2))
		{
			if(operator.equals("=") || operator.equals("=="))
				return left[0]==right[0];
			else if(operator.equals("!=") || operator.equals("<>"))
				return left[0]!=right[0];
			else if(operator.equals(">"))
				return left[0]>right[0];
			else if(operator.equals("<"))
				return left[0]<right[0];
			else if(operator.equals(">="))
				return left[0]>=right[0];
			else if(operator.equals("<="))
				return left[0]<=right[0];
			else if(operator.equals("  like  "))
				return left[0]==right[0];
		}
		else
			throw new Exception("Unmatched LHS and RHS types");
		return false;
	}

	private static boolean evaluate(JDBObject objec,JDBObject objec1,String subq,
			Map<String,Integer> map1, Map<String,Integer> map2,String alias1, String alias2) throws Exception
	{
		List<Boolean> whrandcls = new ArrayList<Boolean>();
		List<Boolean> whrorcls = new ArrayList<Boolean>();
		if(subq!=null && !subq.equals(""))
		{
			while(subq.indexOf(" and ")!=-1 || subq.indexOf(" or ")!=-1)
			{
				int anop = subq.indexOf(" and ");
				int orop = subq.indexOf(" or ");
				if((anop<orop && anop!=-1) || (anop!=-1 && orop==-1))
				{
					String ter = subq.split(" and ")[0].trim();
					getLogicalValue(ter, whrandcls, map1, map2, alias1, alias2, objec, objec1);
					subq = subq.substring(anop+5);
				}
				else if(orop!=-1)
				{
					String ter = subq.split(" or ")[0].trim();
					getLogicalValue(ter, whrorcls, map1, map2, alias1, alias2, objec, objec1);
					subq = subq.substring(orop+5);
				}
			}	
			getLogicalValue(subq, whrandcls, map1, map2, alias1, alias2, objec, objec1);
		}
		if(whrandcls.size()==0)
			return true;
		boolean flag = true;
				
		for (Boolean lval : whrandcls)
		{
			flag &= lval;
		}
		for (Boolean lval : whrorcls)
		{
			flag |= lval;
		}
		return flag;
	}
	
	
	private static boolean evaluate2(JDBObject objec,JDBObject objec1,String subq,
			Map<String,Integer> map1, Map<String,Integer> map2,String alias1, String alias2) throws Exception
	{
		List<Boolean> whrandcls = new ArrayList<Boolean>();
		List<Boolean> whrorcls = new ArrayList<Boolean>();
		if(subq!=null && !subq.equals(""))
		{
			while(subq.indexOf(" and ")!=-1 || subq.indexOf(" or ")!=-1)
			{
				int anop = subq.indexOf(" and ");
				int orop = subq.indexOf(" or ");
				if((anop<orop && anop!=-1) || (anop!=-1 && orop==-1))
				{
					String ter = subq.split(" and ")[0].trim();
					getLogicalValue2(ter, whrandcls, map1, map2, alias1, alias2, objec, objec1);
					subq = subq.substring(anop+5);
				}
				else if(orop!=-1)
				{
					String ter = subq.split(" or ")[0].trim();
					getLogicalValue2(ter, whrorcls, map1, map2, alias1, alias2, objec, objec1);
					subq = subq.substring(orop+5);
				}
			}	
			getLogicalValue2(subq, whrandcls, map1, map2, alias1, alias2, objec, objec1);
		}
		if(whrandcls.size()==0)
			return true;
		boolean flag = true;
				
		for (Boolean lval : whrandcls)
		{
			flag &= lval;
		}
		for (Boolean lval : whrorcls)
		{
			flag |= lval;
		}
		return flag;
	}
	
	
	private static boolean evaluateHv(JDBObject objec,String subq,
			Map<String,Object> map1, Map<String,Character> mpst, LinkedHashMap<Object, com.jdb.BulkConnection.AggInfo> aggVals) throws Exception
	{
		List<Boolean> whrandcls = new ArrayList<Boolean>();
		List<Boolean> whrorcls = new ArrayList<Boolean>();
		if(subq!=null && !subq.equals(""))
		{
			while(subq.indexOf(" and ")!=-1 || subq.indexOf(" or ")!=-1)
			{
				int anop = subq.indexOf(" and ");
				int orop = subq.indexOf(" or ");
				if((anop<orop && anop!=-1) || (anop!=-1 && orop==-1))
				{
					String ter = subq.split(" and ")[0].trim();
					getLogicalValueHv(ter, whrandcls, map1, objec, aggVals, mpst);
					subq = subq.substring(anop+5);
				}
				else if(orop!=-1)
				{
					String ter = subq.split(" or ")[0].trim();
					getLogicalValueHv(ter, whrorcls, map1, objec, aggVals, mpst);
					subq = subq.substring(orop+5);
				}
			}	
			getLogicalValueHv(subq, whrandcls, map1, objec, aggVals, mpst);
		}
		if(whrandcls.size()==0)
			return true;
		boolean flag = true;				
		for (Boolean lval : whrandcls)
		{
			flag &= lval;
		}
		for (Boolean lval : whrorcls)
		{
			flag |= lval;
		}
		return flag;
	}
	
	private int getCharCount(String str,char chr)
	{
		int j = 0;
		for (int i = 0; i < str.length(); i++)
		{
			if(str.charAt(i)==chr)
				j++;
		}
		return j;
	}
	
	private Map<String,String> getTypes(JDBObject objtab)
	{
		Map<String,String> whrandcls = new HashMap<String, String>();
		for (int i = 0; i < objtab.getPackets().size(); i++)
		{
			whrandcls.put(objtab.getPackets().get(i).getNameStr(),objtab.getPackets().get(i).getValueStr());
		}
		return whrandcls;
	}
	
	private Map<String,String> getTypes(JDBObject objtab,String table)
	{
		Map<String,String> whrandcls = new HashMap<String, String>();
		for (int i = 0; i < objtab.getPackets().size(); i++)
		{
			whrandcls.put(table+"."+objtab.getPackets().get(i).getNameStr(),objtab.getPackets().get(i).getValueStr());
		}
		return whrandcls;
	}
	
	class JI
	{
		public JI(int len, int hor)
		{
			reflist = new JDBObject[len][hor];
			this.currpv = this.currph = 0;
			this.hor = hor;
		}
		private int currpv,currph,hor;
		private Map<String, Integer> mps;
		private JDBObject[][] reflist;
		public JDBObject getObj(int mi)
		{
			if(hor==1)
				return reflist[mi][0];
			JDBObject jdbo = new JDBObject();
			for (int i = 0; i < reflist[mi].length; i++) 
			{
				JDBObject jdb = reflist[mi][i];
				for (int l = 0; l < jdb.getPackets().size(); l++)
				{							
					jdbo.addPacket(jdb.getPackets().get(l).getValue(),
							jdb.getPackets().get(l).getType());
				}
			}
			return jdbo;
		}
		public void addObjh(JDBObject ref)
		{
			reflist[currpv][currph] = ref;
			currph++;
		}
		public void addObjhJI(JI ref,int mi)
		{
			for (int i = 0; i < ref.reflist[mi].length; i++) 
			{
				reflist[currpv][currph++] = ref.reflist[mi][i];
			}
		}
		public void addObjhJINull(JI ref,int mi)
		{
			for (int i = 0; i < ref.reflist[mi].length; i++) 
			{				
				JDBObject jdbo = new JDBObject();									
				for (int l = 0; l < ref.reflist[mi][i].getPackets().size(); l++)
				{							
					jdbo.addNullPacket(JDBObject.
							getEqvNullType(ref.reflist[mi][i].getPackets().get(l).getType()));
				}
				reflist[currpv][currph++] = jdbo;
			}
		}
		public void increment()
		{
			currpv++;
			currph = 0;
		}
		public int getLength()
		{
			return currpv;
		}
		public int getInitLength()
		{
			return reflist.length;
		}
	}
	
	class JdbAscComparator implements Comparator<JDBObject>
	{
		int pos = -1;
		JdbAscComparator(String coln,Map<String, Integer> mps)
		{
			pos = mps.get(coln);
		}
		public int compare(JDBObject _1, JDBObject _2)
		{
			return _1.getPackets().get(pos).compare(_2.getPackets().get(pos));
		}
	}
	
	class JdbDescComparator implements Comparator<JDBObject>
	{
		int pos = -1;
		JdbDescComparator(String coln,Map<String, Integer> mps)
		{
			pos = mps.get(coln);
		}
		public int compare(JDBObject _1, JDBObject _2)
		{
			return -_1.getPackets().get(pos).compare(_2.getPackets().get(pos));
		}
	}
	
	class JoinedIndex
	{
		public boolean[][] nnindexes; 
		public List<JDBObject> reflist1,reflist2,nullreflist;
		public JDBObject getJoinedData(int mi,int i,int j)
		{
			JDBObject jdbo = new JDBObject();
			if(nnindexes[mi][0] && reflist1.get(i)!=null)
			{
				for (int l = 0; l < reflist1.get(i).getPackets().size(); l++)
				{							
					jdbo.addPacket(reflist1.get(i).getPackets().get(l).getValue(),
							reflist1.get(i).getPackets().get(l).getType());
				}	
			}
			if(nnindexes[mi][1] && reflist2.get(j)!=null)
			{
				for (int l = 0; l < reflist2.get(j).getPackets().size(); l++)
				{							
					jdbo.addPacket(reflist2.get(j).getPackets().get(l).getValue(),
							reflist2.get(j).getPackets().get(l).getType());
				}	
			}
			if(!nnindexes[mi][0] && nullreflist.get(i)!=null)
			{
				for (int l = 0; l < nullreflist.get(i).getPackets().size(); l++)
				{							
					jdbo.addPacket(nullreflist.get(i).getPackets().get(l).getValue(),
							nullreflist.get(i).getPackets().get(l).getType());
				}	
			}
			if(!nnindexes[mi][1] && nullreflist.get(j)!=null)
			{
				for (int l = 0; l < nullreflist.get(j).getPackets().size(); l++)
				{							
					jdbo.addPacket(nullreflist.get(j).getPackets().get(l).getValue(),
							nullreflist.get(j).getPackets().get(l).getType());
				}	
			}
			return jdbo;
		}
		public void addLeftData(int mi)
		{
			nnindexes[mi][0] = true;
		}
		public void addRightData(int mi)
		{
			nnindexes[mi][1] = true;
		}
		public void addData(int mi)
		{
			nnindexes[mi][0] = nnindexes[mi][1] =true;
		}
	}
	
	
	@SuppressWarnings("rawtypes")
	private void handleJoins1(String[] tables,String[] qparts,String subq,Queue q, String type, 
			boolean distinct, String grpbycol, String havcls, long limit, 
			String ordbycol, String ordbytyp) throws Exception
	{
		String[] finjndcond = new String[tables.length];
		String[] types = new String[tables.length-1];
		int tyc = 0;
		if(!type.equals(""))
		{
			for (int i = 0; i < tables.length; i++) 
			{
				if(tables[i].indexOf(" on ")!=-1)
				{
					String tc = tables[i].split(" on ")[1].trim();
					if(tc.indexOf(" left")!=-1)
						types[tyc++] = "left";
					else if(tc.indexOf(" inner")!=-1)
						types[tyc++] = "inner";
					else if(tc.indexOf(" right")!=-1)
						types[tyc++] = "right";
					else if(tc.indexOf(" full")!=-1)
						types[tyc++] = "full";
					else
					{
						if(i<tables.length-1)
							throw new Exception("Illegal join type");
					}
					finjndcond[i] = (tables[i].split(" on ")[1].replaceFirst(types[tyc-1], "").trim());
					tables[i] = tables[i].split(" on ")[0].trim();
				}
				else
				{
					if(i<tables.length-1)
						types[tyc++] = tables[i].split(" ")[tables[i].split(" ").length-1].trim();
					if(types[tyc-1]!=null)
						tables[i] = tables[i].replaceFirst(types[tyc-1], "").trim();
					else
						tables[i] = tables[i].trim();
					finjndcond[i] = "";
				}
			}
			if(type.equals("inner"))
				type = "";
		}
		boolean imaginaryaggr = false;
		if(!grpbycol.equals("") && qparts[0].equals("*"))
			imaginaryaggr = true;
		if(qparts[0].equals("*") && !imaginaryaggr)
		{
			JI[] jis = new JI[2];
			List<JDBObject> qs[] = new List[tables.length];
			Map<String,Integer> mps[] = new HashMap[tables.length+1];
			Map<String,Integer> mpf = new HashMap<String,Integer>();
			mps[tables.length] = new HashMap<String, Integer>();
			String[] aliases = new String[tables.length],tabns = new String[tables.length];;
			String[] cond = null;
			int refs[][] = new int[tables.length][];
			for (int i = 0; i < tables.length; i++)
			{
				String tablen = tables[i].trim().split(" ")[0];
				if(tables[i].trim().split(" ").length==2)
					aliases[i] = tables[i].trim().split(" ")[1];
				Table table = DBManager.getTable("temp", tablen);
				JDBObject objtab = new JDBObject();		
				tabns[i] = table.getName();
				for (int ii = 0; ii < table.getColumnNames().length; ii++)
				{
					objtab.addPacket(table.getColumnTypes()[ii],table.getColumnNames()[ii]);
				}
				mps[i] = getPositions(objtab,table.getName());	
				mpf.putAll(mps[i]);
				if(tables.length==1)
					cond = new String[]{subq};
				else
					cond = getConditions(subq, mps[i], aliases[i], table.getName());
				if(distinct)
					qs[i] = new ArrayList<JDBObject>((Set<JDBObject>)selectAMEFObjectsDo("temp", table, qparts, null , cond[0], objtab, tables.length==1,false, grpbycol));
				else
					qs[i] = (ArrayList<JDBObject>)selectAMEFObjectso("temp", table, qparts, null , cond[0], objtab, tables.length==1,false, grpbycol);
				refs[i] = new int[qs[i].size()];
			}
			if(!ordbycol.equals(""))
			{
				String[] grpcs = ordbycol.split(","); 
				for(int v=0;v<grpcs.length;v++)
				{
					boolean fl = false,ex = false;
					int tabind = -1;
					String ordbyt = "";
					for(int u=0;u<tables.length;u++)
					{
						if(mps[u].containsKey(tabns[u]+"."+grpcs[v]))
						{
							ordbyt = tabns[u]+"."+grpcs[v];
							if(fl)
							{
								ex = false;
								break;
							}
							else
							{
								fl = true;
								ex = true;
								tabind = u;
							}
						}
						else if(grpcs[v].indexOf(".")!=-1)
						{
							ordbyt = grpcs[v];
							if(mps[u].containsKey(grpcs[v]))
							{
								if(fl)
								{
									ex = false;
									break;
								}
								else
								{
									fl = true;
									ex = true;
									tabind = u;
								}
							}
						}
					}
					if(!ex || !fl)
						throw new Exception("Ambigious Order By clause");
					else
					{
						if(ordbytyp.equals("asc"))
							Collections.sort(qs[tabind],new JdbAscComparator(ordbyt, mps[tabind]));
						else
							Collections.sort(qs[tabind],new JdbDescComparator(ordbyt, mps[tabind]));
					}
				}				
			}
			if(tables.length==1)
			{
				if(limit==-1)
					limit = qs[0].size();
				for (int i = 0; i < limit; i++)
				{
					q.add(JdbResources.getEncoder().encodeWL(qs[0].get(i), true));
				}
				return;
			}
			for (int i = 0; i < aliases.length; i++) 
			{
				for (int j = 0; j < finjndcond.length; j++) 
				{
					if(aliases[i]!=null && !aliases[i].equals("") && finjndcond[j]!=null && finjndcond[j].indexOf(aliases[i]+".")!=-1)
					{
						finjndcond[j] = finjndcond[j].replaceFirst(aliases[i]+".", tabns[i]+".");
					}
				}
			}
			
			
			boolean[][] flags = new boolean[tables.length][]; 
			int j=1;
			jis[0] = new JI(qs[0].size(),1);
			for (int i = 0; i < qs[0].size(); i++) 
			{
				jis[0].addObjh(qs[0].get(i));
				jis[0].increment();
			}
			while (j < tables.length)
			{
				long size = jis[0].getLength() * qs[j].size();
				jis[1] = new JI((int)size,j+1);
				for(int i=0;i< jis[0].getLength();i++)
				{					
					JDBObject objm = jis[0].getObj(i);						
					if(flags[0]==null)
						flags[0] = new boolean[jis[1].getInitLength()];
					
					for(int k=0;k<qs[j].size();k++)
					{
						JDBObject obje = ((List<JDBObject>)qs[j]).get(k);
						if(flags[j]==null)
							flags[j] = new boolean[qs[j].size()];
						boolean condi = (finjndcond.length>j)?evaluate2(objm, obje, finjndcond[j], mps[0], mps[j], aliases[0], aliases[j])
								:evaluate2(objm, obje, cond[1], mps[0], mps[j], aliases[0], aliases[j]);
						if(condi || type.equals("left") || type.equals("right") || type.equals("full"))
						{							
							if(type.equals("") || types[j-1].equals("left"))
							{
								if(condi)
								{
									flags[0][i] = true;
									flags[j][k] = true;
									jis[1].addObjhJI(jis[0],i);
									jis[1].addObjh(obje);
									jis[1].increment();
								}
								else if(!flags[0][i])
								{
									flags[0][i] = true;
									flags[j][k] = true;
									jis[1].addObjhJI(jis[0],i);
									JDBObject jdbo = new JDBObject();
									for (int l = 0; l < obje.getPackets().size(); l++)
									{							
										jdbo.addNullPacket(JDBObject.
												getEqvNullType(obje.getPackets().get(l).getType()));
									}
									jis[1].addObjh(jdbo);
									jis[1].increment();
								}
							}
							else if(types[j-1].equals("right"))
							{
								if(condi)
								{
									flags[0][i] = true;
									flags[j][k] = true;
									jis[1].addObjhJI(jis[0],i);
									jis[1].addObjh(obje);
									jis[1].increment();
								}
								else if(!flags[j][k])
								{
									flags[0][i] = true;
									flags[j][k] = true;
									jis[1].addObjhJINull(jis[0],i);
									jis[1].addObjh(obje);
									jis[1].increment();
								}
							}
							else if(types[j-1].equals("full"))
							{
								if(condi)
								{
									flags[0][i] = true;
									flags[j][k] = true;
									jis[1].addObjhJI(jis[0],i);
									jis[1].addObjh(obje);
									jis[1].increment();
								}
								else
								{
									if(!flags[0][i] && k==qs[j].size()-1)
									{
										flags[0][i] = true;		
										jis[1].addObjhJI(jis[0],i);
										JDBObject jdbo = new JDBObject();	
										for (int l = 0; l < obje.getPackets().size(); l++)
										{							
											jdbo.addNullPacket(JDBObject.
													getEqvNullType(obje.getPackets().get(l).getType()));
										}
										jis[1].addObjh(jdbo);
										jis[1].increment();
									}
									if(!flags[j][k] && i==qs[0].size()-1)
									{
										flags[j][k] = true;
										jis[1].addObjhJINull(jis[0],i);
										JDBObject jdbo = new JDBObject();	
										for (int l = 0; l < obje.getPackets().size(); l++)
										{							
											jdbo.addNullPacket(JDBObject.
													getEqvNullType(obje.getPackets().get(l).getType()));
										}										
										jis[1].addObjh(jdbo);
										jis[1].increment();
									}
								}
							}
						}
					}
				}
				mps[tables.length].putAll(mps[0]);
				for (Iterator iter = mps[j].entrySet().iterator(); iter.hasNext();)
				{
					Map.Entry<String,Integer> entry = (Map.Entry<String,Integer>)iter.next();
					mps[tables.length].put(entry.getKey(), entry.getValue()+mps[0].size());
				}
				mps[0] = mps[tables.length];
				mps[tables.length] =  new HashMap<String, Integer>();
				j++;
				jis[0] = jis[1];
				flags[0] = new boolean[jis[0].getLength()];
			}
			if(limit==-1)
				limit = jis[0].getLength();
			for (int o=0;o<(int)limit;o++)
			{
				q.add(JdbResources.getEncoder().encodeWL(jis[0].getObj(o), true));
			}
		}
		else
		{			
			int countss = 0;
			List<JDBObject> qs[] = new List[tables.length];
			JI[] jis = new JI[2];
			Map<String,Integer> mps[] = new HashMap[tables.length+1];
			Map<String,Integer> mpf = new HashMap<String, Integer>();
			Map<String,String> mpt[] = new HashMap[tables.length+1];
			String[] aliases = new String[tables.length],tabns = new String[tables.length];
			String[][] qpartz = new String[tables.length][];
			String[] cond = null;
			LinkedHashMap<Object, AggInfo> aggVals = null;
			LinkedHashMap<Object, AggInfo> rowVals = null;
			JDBObject[] objtbs = new JDBObject[tables.length];
			boolean aggr = false;
			for (int i = 0; i < tables.length; i++)
			{
				String tablen = tables[i].trim().split(" ")[0];
				Table table = DBManager.getTable("temp", tablen);
				tabns[i] = table.getName();
				JDBObject objtab = new JDBObject();		
				for (int ii = 0; ii < table.getColumnNames().length; ii++)
				{
					objtab.addPacket(table.getColumnTypes()[ii],table.getColumnNames()[ii]);
				}
				mps[i] = getPositions(objtab,table.getName());	
				mpt[i] = getTypes(objtab,table.getName());
				mpf.putAll(mps[i]);
				objtbs[i] = objtab;
				if(tables[i].trim().split(" ").length==2)
					aliases[i] = tables[i].trim().split(" ")[1];
				if(!imaginaryaggr)
				{
				    List<String> qprts = new ArrayList<String>();
					for (int j = 0; j < qparts.length; j++) 
					{
						String[] column = qparts[j].split("\\.");
						if(column.length==2)
						{
							if((column[0].indexOf("count(")!=-1))
							{
								String accol = column[1].replaceFirst("\\)", "");
								accol = accol.replaceFirst("\\)", "");							
								if(column[0].replaceFirst("count\\(", "").equals(aliases[i]) 
										|| column[0].replaceFirst("count\\(", "").equals(table.getName()))
								{	if(mps[i].containsKey(table.getName()+"."+accol))
									qprts.add("count("+table.getName()+"."+accol+")");
									else throw new Exception("Invalid column name");
									aggr = true;
									countss ++;
								}
							}
							else if(column[0].indexOf("sum(")!=-1)
							{
								String accol = column[1].replaceFirst("\\)", "");
								accol = accol.replaceFirst("\\)", "");							
								if(column[0].replaceFirst("sum\\(", "").equals(aliases[i]) 
										|| column[0].replaceFirst("sum\\(", "").equals(table.getName()))
								{	if(mps[i].containsKey(table.getName()+"."+accol))
									qprts.add("sum("+table.getName()+"."+accol+")");
									else throw new Exception("Invalid column name");
									aggr = true;
								}
							}
							else if(column[0].indexOf("avg(")!=-1)
							{
								String accol = column[1].replaceFirst("\\)", "");
								accol = accol.replaceFirst("\\)", "");							
								if(column[0].replaceFirst("avg\\(", "").equals(aliases[i]) 
										|| column[0].replaceFirst("avg\\(", "").equals(table.getName()))
								{	if(mps[i].containsKey(table.getName()+"."+accol))
									qprts.add("avg("+table.getName()+"."+accol+")");
									else throw new Exception("Invalid column name");
									aggr = true;
								}
							}
							else if(column[0].indexOf("min(")!=-1)
							{
								String accol = column[1].replaceFirst("\\)", "");
								accol = accol.replaceFirst("\\)", "");							
								if(column[0].replaceFirst("min\\(", "").equals(aliases[i]) 
										|| column[0].replaceFirst("min\\(", "").equals(table.getName()))
								{	if(mps[i].containsKey(table.getName()+"."+accol))
									qprts.add("min("+table.getName()+"."+accol+")");
									else throw new Exception("Invalid column name");
									aggr = true;
								}
							}
							else if(column[0].indexOf("max(")!=-1)
							{
								String accol = column[1].replaceFirst("\\)", "");
								accol = accol.replaceFirst("\\)", "");							
								if(column[0].replaceFirst("max\\(", "").equals(aliases[i]) 
										|| column[0].replaceFirst("max\\(", "").equals(table.getName()))
								{	if(mps[i].containsKey(table.getName()+"."+accol))
									qprts.add("max("+table.getName()+"."+accol+")");
									else throw new Exception("Invalid column name");
									aggr = true;
								}
							}
							else if(column[0].indexOf("first(")!=-1)
							{
								String accol = column[1].replaceFirst("\\)", "");
								accol = accol.replaceFirst("\\)", "");							
								if(column[0].replaceFirst("first\\(", "").equals(aliases[i]) 
										|| column[0].replaceFirst("first\\(", "").equals(table.getName()))
								{	if(mps[i].containsKey(table.getName()+"."+accol))
									qprts.add("first("+table.getName()+"."+accol+")");
									else throw new Exception("Invalid column name");
									aggr = true;
								}
							}
							else if(column[0].indexOf("last(")!=-1)
							{
								String accol = column[1].replaceFirst("\\)", "");
								accol = accol.replaceFirst("\\)", "");							
								if(column[0].replaceFirst("last\\(", "").equals(aliases[i]) 
										|| column[0].replaceFirst("last\\(", "").equals(table.getName()))
								{	if(mps[i].containsKey(table.getName()+"."+accol))
									qprts.add("last("+table.getName()+"."+accol+")");
									else throw new Exception("Invalid column name");
									aggr = true;
								}
							}
							else
							{
								if(column[0].equals(table.getName()) || column[0].equals(aliases[i]))
								{
									if(mps[i].containsKey(table.getName()+"."+column[1]))
										qprts.add(table.getName()+"."+column[1]);
									else throw new Exception("Invalid column name");
								}
							}
						}
						else if(column.length==1)
						{						
							if((column[0].indexOf("count(")!=-1))
							{
								String accol = column[0].replaceFirst("count\\(", "");
								accol = accol.replaceFirst("\\)", "");
								boolean fl = false,ex = false;
								for(int u=0;u<tables.length;u++)
								{
									if(mps[u].containsKey(table.getName()+"."+accol))
									{
										if(fl)
										{
											ex = false;
											break;
										}
										else
										{
											fl = true;
											ex = true;
										}
									}
								}
								if(!ex || !fl)
									throw new Exception("Ambigious column name");
								if(mps[i].containsKey(table.getName()+"."+accol))
									qprts.add(column[0]);
								aggr = true;
								countss ++;
							}
							else if(column[0].indexOf("sum(")!=-1)
							{
								String accol = column[0].replaceFirst("sum\\(", "");
								accol = accol.replaceFirst("\\)", "");
								boolean fl = false,ex = false;
								for(int u=0;u<tables.length;u++)
								{
									if(mps[u].containsKey(table.getName()+"."+accol))
									{
										if(fl)
										{
											ex = false;
											break;
										}
										else
										{
											fl = true;
											ex = true;
										}
									}
								}
								if(!ex || !fl)
									throw new Exception("Ambigious column name");
								if(mps[i].containsKey(table.getName()+"."+accol))
									qprts.add(column[0]);
								aggr = true;
							}
							else if(column[0].indexOf("avg(")!=-1)
							{
								String accol = column[0].replaceFirst("avg\\(", "");
								accol = accol.replaceFirst("\\)", "");
								boolean fl = false,ex = false;
								for(int u=0;u<tables.length;u++)
								{
									if(mps[u].containsKey(table.getName()+"."+accol))
									{
										if(fl)
										{
											ex = false;
											break;
										}
										else
										{
											fl = true;
											ex = true;
										}
									}
								}
								if(!ex || !fl)
									throw new Exception("Ambigious column name");
								if(mps[i].containsKey(table.getName()+"."+accol))
									qprts.add(column[0]);
								aggr = true;
							}
							else if(column[0].indexOf("min(")!=-1)
							{
								String accol = column[0].replaceFirst("min\\(", "");
								accol = accol.replaceFirst("\\)", "");
								boolean fl = false,ex = false;
								for(int u=0;u<tables.length;u++)
								{
									if(mps[u].containsKey(table.getName()+"."+accol))
									{
										if(fl)
										{
											ex = false;
											break;
										}
										else
										{
											fl = true;
											ex = true;
										}
									}
								}
								if(!ex || !fl)
									throw new Exception("Ambigious column name");
								if(mps[i].containsKey(table.getName()+"."+accol))
									qprts.add(column[0]);
								aggr = true;
							}
							else if(column[0].indexOf("max(")!=-1)
							{
								String accol = column[0].replaceFirst("max\\(", "");
								accol = accol.replaceFirst("\\)", "");
								boolean fl = false,ex = false;
								for(int u=0;u<tables.length;u++)
								{
									if(mps[u].containsKey(table.getName()+"."+accol))
									{
										if(fl)
										{
											ex = false;
											break;
										}
										else
										{
											fl = true;
											ex = true;
										}
									}
								}
								if(!ex || !fl)
									throw new Exception("Ambigious column name");
								if(mps[i].containsKey(table.getName()+"."+accol))
									qprts.add(column[0]);
								aggr = true;
							}
							else if(column[0].indexOf("first(")!=-1)
							{
								String accol = column[0].replaceFirst("first\\(", "");
								accol = accol.replaceFirst("\\)", "");
								boolean fl = false,ex = false;
								for(int u=0;u<tables.length;u++)
								{
									if(mps[u].containsKey(table.getName()+"."+accol))
									{
										if(fl)
										{
											ex = false;
											break;
										}
										else
										{
											fl = true;
											ex = true;
										}
									}
								}
								if(!ex || !fl)
									throw new Exception("Ambigious column name");
								if(mps[i].containsKey(table.getName()+"."+accol))
									qprts.add(column[0]);
								aggr = true;
							}
							else if(column[0].indexOf("last(")!=-1)
							{
								String accol = column[0].replaceFirst("last\\(", "");
								accol = accol.replaceFirst("\\)", "");
								boolean fl = false,ex = false;
								for(int u=0;u<tables.length;u++)
								{
									if(mps[u].containsKey(table.getName()+"."+accol))
									{
										if(fl)
										{
											ex = false;
											break;
										}
										else
										{
											fl = true;
											ex = true;
										}
									}
								}
								if(!ex || !fl)
									throw new Exception("Ambigious column name");
								if(mps[i].containsKey(table.getName()+"."+accol))
									qprts.add(column[0]);
								aggr = true;
							}
							else
								qprts.add(column[0]);
						}					
					}	
					if(tables.length==1)
						cond = new String[]{subq};
					else
						cond = getConditions(subq, mps[i], aliases[i],table.getName());
					qpartz[i] = qprts.toArray(new String[qprts.size()]);
					qprts.clear();
					String con = "";
					for (int k = 0; k < qpartz[i].length; k++)
					{
						int i1 = getCharCount(qpartz[i][k], '(');
						int i2 = getCharCount(qpartz[i][k], ')');
						if(i1==i2)
						{
							con = qpartz[i][k];
						}
						else
						{
							while(i1!=i2)
							{
								con += qpartz[i][k] + ",";
								i1 = getCharCount(con, '(');
								i2 = getCharCount(con, ')');
								if(i1!=i2)k = k+1;
								
							}
							con = con.substring(0,con.length()-1);
						}
						if(checkColumnExists(con, objtab, table.getName()))
						{	
							qprts.add(con);
							con = "";
						}
					}
					qpartz[i] = qprts.toArray(new String[qprts.size()]);
					if(distinct)
						qs[i] = new ArrayList<JDBObject>((HashSet<JDBObject>)selectAMEFObjectsDo("temp", table, (aggr && tables.length>1)?new String[]{}:qpartz[i], null , cond[0], objtab, aggr?tables.length>1:tables.length==1, aggr, grpbycol));
					else
						qs[i] = (ArrayList<JDBObject>)selectAMEFObjectso("temp", table, (aggr && tables.length>1)?new String[]{}:qpartz[i], null , cond[0], objtab, aggr?tables.length>1:tables.length==1, aggr, grpbycol);
				}
				else
				{
					if(tables.length==1)
						cond = new String[]{subq};
					else
						cond = getConditions(subq, mps[i], aliases[i],table.getName());
					qpartz[i] = new String[]{"count(*)"};
					if(distinct)
						qs[i] = new ArrayList<JDBObject>((HashSet<JDBObject>)selectAMEFObjectsDo("temp", table, tables.length==1?new String[]{"count(*)"}:new String[]{}, null , cond[0], objtab, true, false, grpbycol));
					else
						qs[i] = (ArrayList<JDBObject>)selectAMEFObjectso("temp", table, tables.length==1?new String[]{"count(*)"}:new String[]{}, null , cond[0], objtab, true, false, grpbycol);
				}
			}
			mps[tables.length] = new HashMap<String, Integer>();
			mpt[tables.length] = new HashMap<String, String>();
			if(aggr || imaginaryaggr)
			{
				aggVals = new LinkedHashMap<Object, AggInfo>();
				rowVals = new LinkedHashMap<Object, AggInfo>();
			}
			if(tables.length==1)
			{
				if(imaginaryaggr)
				{
					int k = 0;
					JDBObject obje = new JDBObject();
					for (Iterator<JDBObject> iter1 = qs[0].iterator();iter1.hasNext();k++)//(int k = 0; k < qs[j].size(); k++)
					{
						JDBObject objm = iter1.next();
						createRow("count(*)", objm, obje, aggVals, mps[0],mpt[0],mps[0],mpt[0],grpbycol,rowVals,tables[0],tables[0]);
					}
					qs[0].clear();
					for (Iterator iter = rowVals.entrySet().iterator(); iter.hasNext();)
					{
						Map.Entry<Object,AggInfo> entry = (Map.Entry<Object,AggInfo>)iter.next();					
						qs[0].add((JDBObject)(entry.getValue().count));
					}
				}
				if(limit==-1)
					limit = qs[0].size();
				for (int i = 0; i < limit; i++)
				{
					q.add(JdbResources.getEncoder().encodeWL(qs[0].get(i), true));
				}
				return;
			}
			boolean[][] flags = new boolean[tables.length][]; 
			cond = getConditionsf(subq, mpf, aliases, tabns);
			
			if(!grpbycol.equals(""))
			{
				String[] grpcs = grpbycol.split(","); 
				for(int v=0;v<grpcs.length;v++)
				{
					boolean fl = false,ex = false;
					for(int u=0;u<tables.length;u++)
					{
						if(mps[u].containsKey(tabns[u]+"."+grpcs[v]))
						{
							if(fl)
							{
								ex = false;
								break;
							}
							else
							{
								fl = true;
								ex = true;
							}
						}
						else if(grpcs[v].indexOf(".")!=-1)
						{
							if(mps[u].containsKey(grpcs[v]))
							{
								if(fl)
								{
									ex = false;
									break;
								}
								else
								{
									fl = true;
									ex = true;
								}
							}
						}
					}
					if(!ex || !fl)
						throw new Exception("Ambigious Group By clause");
				}				
			}
			if(!ordbycol.equals(""))
			{
				String[] grpcs = ordbycol.split(","); 
				for(int v=0;v<grpcs.length;v++)
				{
					boolean fl = false,ex = false;
					int tabind = -1;
					String ordbyt = "";
					for(int u=0;u<tables.length;u++)
					{
						if(mps[u].containsKey(tabns[u]+"."+grpcs[v]))
						{
							ordbyt = tabns[u]+"."+grpcs[v];
							if(fl)
							{
								ex = false;
								break;
							}
							else
							{
								fl = true;
								ex = true;
								tabind = u;
							}
						}
						else if(grpcs[v].indexOf(".")!=-1)
						{
							ordbyt = grpcs[v];
							if(mps[u].containsKey(grpcs[v]))
							{
								if(fl)
								{
									ex = false;
									break;
								}
								else
								{
									fl = true;
									ex = true;
									tabind = u;
								}
							}
						}
					}
					if(!ex || !fl)
						throw new Exception("Ambigious Order By clause");
					else
					{
						if(ordbytyp.equals("asc"))
							Collections.sort(qs[tabind],new JdbAscComparator(ordbyt, mps[tabind]));
						else
							Collections.sort(qs[tabind],new JdbDescComparator(ordbyt, mps[tabind]));
					}
				}				
			}
			
			for (int ii = 0; ii < aliases.length; ii++) 
			{
				for (int j = 0; j < finjndcond.length; j++) 
				{
					if(aliases[ii]!=null && !aliases[ii].equals("") && finjndcond[j]!=null && finjndcond[j].indexOf(aliases[ii]+".")!=-1)
					{
						finjndcond[j] = finjndcond[j].replaceFirst(aliases[ii]+".", tabns[ii]+".");
					}
				}
			}
			int j=1;
			jis[0] = new JI(qs[0].size(),1);
			for (int i = 0; i < qs[0].size(); i++) 
			{
				jis[0].addObjh(qs[0].get(i));
				jis[0].increment();
			}
			while (j < tables.length)
			{
				long size = jis[0].getLength() * qs[j].size();
				jis[1] = new JI((int)size,j+1);
				for (int i = 0; i < jis[0].getLength(); i++)
				{					
					JDBObject objm = jis[0].getObj(i);						
					if(flags[0]==null)
						flags[0] = new boolean[jis[1].getInitLength()];
					if (j < tables.length)
					{
						for (int k = 0; k < qs[j].size(); k++)
						{
							JDBObject obje = ((List<JDBObject>)qs[j]).get(k);
							if(flags[j]==null)
								flags[j] = new boolean[qs[j].size()];
							boolean condi = (finjndcond.length>j)?evaluate2(objm, obje, finjndcond[j], mps[0], mps[j], aliases[0], aliases[j])
									:evaluate2(objm, obje, cond[1], mps[0], mps[j], aliases[0], aliases[j]);
							if(condi || type.equals("left") || type.equals("right") || type.equals("full"))
							{							
								if(type.equals("") || types[j-1].equals("left"))
								{
									if(condi)
									{
										flags[0][i] = true;
										flags[j][k] = true;
										jis[1].addObjhJI(jis[0],i);
										jis[1].addObjh(obje);
										jis[1].increment();
									}
									else if(!flags[0][i])
									{
										flags[0][i] = true;
										flags[j][k] = true;
										jis[1].addObjhJI(jis[0],i);
										JDBObject jdbo = new JDBObject();
										for (int l = 0; l < obje.getPackets().size(); l++)
										{							
											jdbo.addNullPacket(JDBObject.
													getEqvNullType(obje.getPackets().get(l).getType()));
										}
										jis[1].addObjh(jdbo);
										jis[1].increment();
									}
								}
								else if(types[j-1].equals("right"))
								{
									if(condi)
									{
										flags[0][i] = true;
										flags[j][k] = true;
										jis[1].addObjhJI(jis[0],i);
										jis[1].addObjh(obje);
										jis[1].increment();
									}
									else if(!flags[j][k])
									{
										flags[0][i] = true;
										flags[j][k] = true;
										jis[1].addObjhJINull(jis[0],i);
										jis[1].addObjh(obje);
										jis[1].increment();
									}
								}
								else if(types[j-1].equals("full"))
								{
									if(condi)
									{
										flags[0][i] = true;
										flags[j][k] = true;
										jis[1].addObjhJI(jis[0],i);
										jis[1].addObjh(obje);
										jis[1].increment();
									}
									else
									{
										if(!flags[0][i] && k==qs[j].size()-1)
										{
											flags[0][i] = true;		
											jis[1].addObjhJI(jis[0],i);
											JDBObject jdbo = new JDBObject();	
											for (int l = 0; l < obje.getPackets().size(); l++)
											{							
												jdbo.addNullPacket(JDBObject.
														getEqvNullType(obje.getPackets().get(l).getType()));
											}
											jis[1].addObjh(jdbo);
											jis[1].increment();
										}
										if(!flags[j][k] && i==qs[0].size()-1)
										{
											flags[j][k] = true;
											jis[1].addObjhJINull(jis[0],i);
											JDBObject jdbo = new JDBObject();	
											for (int l = 0; l < obje.getPackets().size(); l++)
											{							
												jdbo.addNullPacket(JDBObject.
														getEqvNullType(obje.getPackets().get(l).getType()));
											}										
											jis[1].addObjh(jdbo);
											jis[1].increment();
										}
									}
								}
							}
						}
					}
				}
				mps[tables.length].putAll(mps[0]);
				for (Iterator iter = mps[j].entrySet().iterator(); iter.hasNext();)
				{
					Map.Entry<String,Integer> entry = (Map.Entry<String,Integer>)iter.next();
					mps[tables.length].put(entry.getKey(), entry.getValue()+mps[0].size());
				}
				mpt[tables.length].putAll(mpt[0]);
				for (Iterator iter = mpt[j].entrySet().iterator(); iter.hasNext();)
				{
					Map.Entry<String,String> entry = (Map.Entry<String,String>)iter.next();
					mpt[tables.length].put(entry.getKey(), entry.getValue());
				}
				mps[0] = mps[tables.length];
				mpt[0] = mpt[tables.length];
				mps[tables.length] =  new HashMap<String, Integer>();
				mpt[tables.length] = new HashMap<String, String>();
				jis[0] = jis[1];
				flags[0] = new boolean[jis[0].getLength()];
				j++;
			}
			if(aggr || imaginaryaggr)
			{	
				JDBObject objkm = new JDBObject();
				for (int k = 0; k < jis[0].getLength(); k++)
				{
					JDBObject objm = jis[0].getObj(k);
					for (int tt1 = 0; tt1 < qpartz.length; tt1++)
					{
						for (int tt = 0; tt < qpartz[tt1].length; tt++)
						{	
							if(isAggrFuncF(qpartz[tt1][tt]))
							{
								createRow(qpartz[tt1][tt], objm, objkm, aggVals, mps[0],mpt[0],mps[0],mpt[0],grpbycol,rowVals,tables[0],tables[0]);								
							}
							else
							{
								if(grpbycol.indexOf(qpartz[tt1][tt])==-1)
									throw new Exception("Invalid group by clause");
								aggVals.put(qpartz[tt1][tt], new AggInfo<Long>());
							}
						}
					}
				}
				long dl = 0;
				if(limit==-1)
					limit = rowVals.size();
				if(imaginaryaggr)
				{
					for (Iterator iter = rowVals.entrySet().iterator(); iter.hasNext();)
					{
						if(dl++<limit)
						{
							Map.Entry<Object,AggInfo> entry = (Map.Entry<Object,AggInfo>)iter.next();					
							q.add(JdbResources.getEncoder().encodeWL((JDBObject)(entry.getValue().count),true));
						}
						else break;
					}
					return;
				}
				for (Iterator iter = rowVals.entrySet().iterator(); iter.hasNext();)
				{
					JDBObject obh = new JDBObject();
					Map.Entry<Object,AggInfo> entry = (Map.Entry<Object,AggInfo>)iter.next();
					Map mpss = new HashMap<String,Object>();
					Map mpst = new HashMap<String,Character>();
					for (Iterator iter1 = aggVals.entrySet().iterator(); iter1.hasNext();)
					{
						Map.Entry<Object,AggInfo> entry1 = (Map.Entry<Object,AggInfo>)iter1.next();
						if(mps[0].get((String)entry1.getKey())!=null)
						{
							JDBObject objt = (JDBObject)entry.getKey();
							for (int jj = 0; jj < objt.getPackets().size(); jj++)
							{
								if(objt.getPackets().get(jj).getNameStr().equals((String)entry1.getKey()))
								{
									obh.addPacket(objt.getPackets().get(jj).getValue(),
										objt.getPackets().get(jj).getType());
									mpss.put((String)entry1.getKey(),
											objt.getPackets().get(jj).getValue());
									mpst.put((String)entry1.getKey(), objt.getPackets().get(jj).getType());
									break;
								}
							}
							
						}
						else if(((String)entry1.getKey()).startsWith("count"))
						{
							if(grpbycol.equals(""))
							{
								obh.addPacket((entry.getValue().cnt+1)/countss);
								mpss.put((String)entry1.getKey(),
										JdbResources.longToByteArray((long)(entry.getValue().cnt+1)/countss,8));
								mpst.put((String)entry1.getKey(), 'l');
							}
							else
							{
								obh.addPacket(entry.getValue().cnt+1);
								mpss.put((String)entry1.getKey(),
										JdbResources.longToByteArray(entry.getValue().cnt+1,8));
								mpst.put((String)entry1.getKey(), 'l');
							}
						}
						else if(isScalarFuncF((String)entry1.getKey()))
						{
							String con = createRow((String)entry1.getKey(), obh, (JDBObject)entry.getKey(), mps[0], false);
							mpss.put((String)entry1.getKey(),
									obh.getPackets().get(obh.getPackets().size()-1).getValue());
							mpst.put((String)entry1.getKey(),obh.getPackets().get(obh.getPackets().size()-1).getType());
						}					
						else
						{
							if(entry1.getValue().count instanceof JDBObject)
							{
								obh.addPacket(((JDBObject)entry1.getValue().count).getPackets().get(0).getTValue());
								mpss.put((String)entry1.getKey(),
										((JDBObject)entry1.getValue().count).getPackets().get(0).getTValue());
							}
							else
							{	
								obh.addPacket(entry1.getValue().count);
								mpss.put((String)entry1.getKey(),
										entry1.getValue().getFinalValue());
							}
						}								
					}
					boolean condi = evaluateHv(obh, havcls, mpss, mpst, aggVals);
					if(condi && dl++<limit)
						q.add(JdbResources.getEncoder().encodeWL(obh,true));					
				}
				return;
			}
			if(limit==-1)
				limit = jis[0].getLength();
			for (int o=0;o<(int)limit;o++)
			{
				q.add(JdbResources.getEncoder().encodeWL(jis[0].getObj(o), true));
			}			
		}
	}
	
	
	
	private void handleJoins(String[] tables,String[] qparts,String subq,Queue q, String type, 
			boolean distinct, String grpbycol, String havcls, long limit) throws Exception
	{
		String[] finjndcond = new String[tables.length];
		String[] types = new String[tables.length-1];
		int tyc = 0;
		if(!type.equals(""))
		{
			for (int i = 0; i < tables.length; i++) 
			{
				if(tables[i].indexOf(" on ")!=-1)
				{
					String tc = tables[i].split(" on ")[1].trim();
					if(tc.indexOf(" left")!=-1)
						types[tyc++] = "left";
					else if(tc.indexOf(" inner")!=-1)
						types[tyc++] = "inner";
					else if(tc.indexOf(" right")!=-1)
						types[tyc++] = "right";
					else if(tc.indexOf(" full")!=-1)
						types[tyc++] = "full";
					else
					{
						if(i<tables.length-1)
							throw new Exception("Illegal join type");
					}
					finjndcond[i] = (tables[i].split(" on ")[1].replaceFirst(types[tyc-1], "").trim());
					tables[i] = tables[i].split(" on ")[0].trim();
				}
				else
				{
					if(i<tables.length-1)
						types[tyc++] = tables[i].split(" ")[tables[i].split(" ").length-1].trim();
					if(types[tyc-1]!=null)
						tables[i] = tables[i].replaceFirst(types[tyc-1], "").trim();
					else
						tables[i] = tables[i].trim();
					finjndcond[i] = "";
				}
			}
			if(type.equals("inner"))
				type = "";
		}
		boolean imaginaryaggr = false;
		if(!grpbycol.equals("") && qparts[0].equals("*"))
			imaginaryaggr = true;
		if(qparts[0].equals("*") && !imaginaryaggr)
		{
			Collection<JDBObject> qs[] = new Collection[tables.length+1];
			Map<String,Integer> mps[] = new HashMap[tables.length+1];
			mps[tables.length] = new HashMap<String, Integer>();
			String[] aliases = new String[tables.length],tabns = new String[tables.length];;
			String[] cond = null;
			for (int i = 0; i < tables.length; i++)
			{
				String tablen = tables[i].trim().split(" ")[0];
				if(tables[i].trim().split(" ").length==2)
					aliases[i] = tables[i].trim().split(" ")[1];
				Table table = DBManager.getTable("temp", tablen);
				JDBObject objtab = new JDBObject();		
				tabns[i] = table.getName();
				for (int ii = 0; ii < table.getColumnNames().length; ii++)
				{
					objtab.addPacket(table.getColumnTypes()[ii],table.getColumnNames()[ii]);
				}
				mps[i] = getPositions(objtab,table.getName());	
				if(tables.length==1)
					cond = new String[]{subq};
				else
					cond = getConditions(subq, mps[i], aliases[i], table.getName());
				if(distinct)
					qs[i] = (HashSet<JDBObject>)selectAMEFObjectsDo("temp", table, qparts, null , cond[0], objtab, tables.length==1,false, grpbycol);
				else
					qs[i] = (ArrayList<JDBObject>)selectAMEFObjectso("temp", table, qparts, null , cond[0], objtab, tables.length==1,false, grpbycol);
			}
			if(tables.length==1)
			{
				long dl = 0;
				if(limit==-1)
					limit = qs[0].size();
				for (Iterator<JDBObject> iter = qs[0].iterator();iter.hasNext();)//(int i = 0; i < qs[tables.length].size(); i++)
				{
					if(dl++<limit)
						q.add(JdbResources.getEncoder().encodeWL(iter.next(), true));
					else break;
				}
				return;
			}
			//if(distinct)
				qs[tables.length] = new HashSet<JDBObject>();
			//else
			//	qs[tables.length] = new ArrayList<JDBObject>();
			for (int i = 0; i < aliases.length; i++) 
			{
				for (int j = 0; j < finjndcond.length; j++) 
				{
					if(aliases[i]!=null && !aliases[i].equals("") && finjndcond[j]!=null && finjndcond[j].indexOf(aliases[i]+".")!=-1)
					{
						finjndcond[j] = finjndcond[j].replaceFirst(aliases[i]+".", tabns[i]+".");
					}
				}
			}
			boolean[][] flags = new boolean[tables.length][]; 
			int j=1;
			while (j < tables.length)
			{
				int i = 0;
				for (Iterator<JDBObject> iter = qs[0].iterator();iter.hasNext();i++)//(int i = 0; i < qs[0].size(); i++)
				{					
					JDBObject objm = iter.next();						
					if(flags[0]==null)
						flags[0] = new boolean[qs[0].size()];
					
					int k = 0;
					for (Iterator<JDBObject> iter1 = qs[j].iterator();iter1.hasNext();k++)//(int k = 0; k < qs[j].size(); k++)
					{
						JDBObject obje = iter1.next();
						if(flags[j]==null)
							flags[j] = new boolean[qs[j].size()];
						boolean added = false;
						boolean condi = (finjndcond.length>j)?evaluate2(objm, obje, finjndcond[j], mps[0], mps[j], aliases[0], aliases[j])
								:evaluate2(objm, obje, cond[1], mps[0], mps[j], aliases[0], aliases[j]);
						if(condi || type.equals("left") || type.equals("right") || type.equals("full"))
						{
							JDBObject jdbo = new JDBObject();
							if(type.equals("") || types[j-1].equals("left"))
							{
								if(condi)
								{
									flags[0][i] = true;
									flags[j][k] = true;
									for (int l = 0; l < objm.getPackets().size(); l++)
									{							
										jdbo.addPacket(objm.getPackets().get(l).getValue(),
												objm.getPackets().get(l).getType());
									}	
									for (int l = 0; l < obje.getPackets().size(); l++)
									{							
										jdbo.addPacket(obje.getPackets().get(l).getValue(),
												obje.getPackets().get(l).getType());
									}	
								}
								else if(!flags[0][i])
								{
									flags[0][i] = true;
									flags[j][k] = true;
									for (int l = 0; l < objm.getPackets().size(); l++)
									{							
										jdbo.addPacket(objm.getPackets().get(l).getValue(),
												objm.getPackets().get(l).getType());
									}	
									for (int l = 0; l < obje.getPackets().size(); l++)
									{							
										jdbo.addNullPacket(JDBObject.
												getEqvNullType(obje.getPackets().get(l).getType()));
									}
								}
							}
							else if(types[j-1].equals("right"))
							{
								if(condi)
								{
									flags[0][i] = true;
									flags[j][k] = true;
									for (int l = 0; l < objm.getPackets().size(); l++)
									{							
										jdbo.addPacket(objm.getPackets().get(l).getValue(),
												objm.getPackets().get(l).getType());
									}	
									for (int l = 0; l < obje.getPackets().size(); l++)
									{							
										jdbo.addPacket(obje.getPackets().get(l).getValue(),
												obje.getPackets().get(l).getType());
									}	
								}
								else if(!flags[j][k])
								{
									flags[0][i] = true;
									flags[j][k] = true;
									for (int l = 0; l < objm.getPackets().size(); l++)
									{							
										jdbo.addNullPacket(JDBObject.
												getEqvNullType(objm.getPackets().get(l).getType()));
									}	
									for (int l = 0; l < obje.getPackets().size(); l++)
									{							
										jdbo.addNullPacket(JDBObject.
												getEqvNullType(obje.getPackets().get(l).getType()));
									}
								}
							}
							else if(types[j-1].equals("full"))
							{
								if(condi)
								{
									flags[0][i] = true;
									flags[j][k] = true;
									for (int l = 0; l < objm.getPackets().size(); l++)
									{							
										jdbo.addPacket(objm.getPackets().get(l).getValue(),
												objm.getPackets().get(l).getType());
									}	
									for (int l = 0; l < obje.getPackets().size(); l++)
									{							
										jdbo.addPacket(obje.getPackets().get(l).getValue(),
												obje.getPackets().get(l).getType());
									}	
								}
								else
								{
									if(!flags[0][i] && k==qs[j].size()-1)
									{
										flags[0][i] = true;
										if(added)
											jdbo = new JDBObject();
										for (int l = 0; l < objm.getPackets().size(); l++)
										{							
											jdbo.addPacket(objm.getPackets().get(l).getValue(),
													objm.getPackets().get(l).getType());
										}	
										for (int l = 0; l < obje.getPackets().size(); l++)
										{							
											jdbo.addNullPacket(JDBObject.
													getEqvNullType(obje.getPackets().get(l).getType()));
										}
										if(jdbo.getPackets().size()>0)
										{
											qs[tables.length].add(jdbo);
											added = true;
										}
										
									}
									if(!flags[j][k] && i==qs[0].size()-1)
									{
										flags[j][k] = true;
										if(added)
											jdbo = new JDBObject();
										for (int l = 0; l < objm.getPackets().size(); l++)
										{							
											jdbo.addNullPacket(JDBObject.
													getEqvNullType(objm.getPackets().get(l).getType()));
										}	
										for (int l = 0; l < obje.getPackets().size(); l++)
										{							
											jdbo.addNullPacket(JDBObject.
													getEqvNullType(obje.getPackets().get(l).getType()));
										}
										if(jdbo.getPackets().size()>0)
										{
											qs[tables.length].add(jdbo);
											added = true;
										}
									}
								}
							}
							if(jdbo.getPackets().size()>0 && !added)
								qs[tables.length].add(jdbo);
						}
					}
				}
				mps[tables.length].putAll(mps[0]);
				for (Iterator iter = mps[j].entrySet().iterator(); iter.hasNext();)
				{
					Map.Entry<String,Integer> entry = (Map.Entry<String,Integer>)iter.next();
					mps[tables.length].put(entry.getKey(), entry.getValue()+mps[0].size());
				}
				mps[0] = mps[tables.length];
				mps[tables.length] =  new HashMap<String, Integer>();
				qs[0] = qs[tables.length];
				flags[0] = new boolean[qs[0].size()];
				//if(distinct)
					qs[tables.length] = new HashSet<JDBObject>();
				//else
				//	qs[tables.length] = new ArrayList<JDBObject>();
				j++;
			}
			long dl = 0;
			if(limit==-1)
				limit = qs[0].size();
			for (Iterator<JDBObject> iter = qs[0].iterator();iter.hasNext();)//(int i = 0; i < qs[tables.length].size(); i++)
			{
				if(dl++<limit)
					q.add(JdbResources.getEncoder().encodeWL(iter.next(), true));
				else break;
			}
			//System.out.println(qs[tables.length]);
		}
		else
		{			
			int countss = 0;
			Collection<JDBObject> qs[] = null;
			if(distinct)
				qs = new HashSet[tables.length+1];
			else
				qs = new ArrayList[tables.length+1];
			Map<String,Integer> mps[] = new HashMap[tables.length+1];
			Map<String,Integer> mpf = new HashMap<String, Integer>();
			Map<String,String> mpt[] = new HashMap[tables.length+1];
			String[] aliases = new String[tables.length],tabns = new String[tables.length];
			String[][] qpartz = new String[tables.length][];
			String[] cond = null;
			LinkedHashMap<Object, com.jdb.BulkConnection.AggInfo> aggVals = null;
			LinkedHashMap<Object, com.jdb.BulkConnection.AggInfo> rowVals = null;
			JDBObject[] objtbs = new JDBObject[tables.length];
			boolean aggr = false;
			for (int i = 0; i < tables.length; i++)
			{
				String tablen = tables[i].trim().split(" ")[0];
				Table table = DBManager.getTable("temp", tablen);
				tabns[i] = table.getName();
				JDBObject objtab = new JDBObject();		
				for (int ii = 0; ii < table.getColumnNames().length; ii++)
				{
					objtab.addPacket(table.getColumnTypes()[ii],table.getColumnNames()[ii]);
				}
				mps[i] = getPositions(objtab,table.getName());	
				mpt[i] = getTypes(objtab,table.getName());
				mpf.putAll(mps[i]);
				objtbs[i] = objtab;
				if(tables[i].trim().split(" ").length==2)
					aliases[i] = tables[i].trim().split(" ")[1];
				if(!imaginaryaggr)
				{
				    List<String> qprts = new ArrayList<String>();
					for (int j = 0; j < qparts.length; j++) 
					{
						String[] column = qparts[j].split("\\.");
						if(column.length==2)
						{
							if((column[0].indexOf("count(")!=-1))
							{
								String accol = column[1].replaceFirst("\\)", "");
								accol = accol.replaceFirst("\\)", "");							
								if(column[0].replaceFirst("count\\(", "").equals(aliases[i]) 
										|| column[0].replaceFirst("count\\(", "").equals(table.getName()))
								{	if(mps[i].containsKey(table.getName()+"."+accol))
									qprts.add("count("+table.getName()+"."+accol+")");
									else throw new Exception("Invalid column name");
									aggr = true;
									countss ++;
								}
							}
							else if(column[0].indexOf("sum(")!=-1)
							{
								String accol = column[1].replaceFirst("\\)", "");
								accol = accol.replaceFirst("\\)", "");							
								if(column[0].replaceFirst("sum\\(", "").equals(aliases[i]) 
										|| column[0].replaceFirst("sum\\(", "").equals(table.getName()))
								{	if(mps[i].containsKey(table.getName()+"."+accol))
									qprts.add("sum("+table.getName()+"."+accol+")");
									else throw new Exception("Invalid column name");
									aggr = true;
								}
							}
							else if(column[0].indexOf("avg(")!=-1)
							{
								String accol = column[1].replaceFirst("\\)", "");
								accol = accol.replaceFirst("\\)", "");							
								if(column[0].replaceFirst("avg\\(", "").equals(aliases[i]) 
										|| column[0].replaceFirst("avg\\(", "").equals(table.getName()))
								{	if(mps[i].containsKey(table.getName()+"."+accol))
									qprts.add("avg("+table.getName()+"."+accol+")");
									else throw new Exception("Invalid column name");
									aggr = true;
								}
							}
							else if(column[0].indexOf("min(")!=-1)
							{
								String accol = column[1].replaceFirst("\\)", "");
								accol = accol.replaceFirst("\\)", "");							
								if(column[0].replaceFirst("min\\(", "").equals(aliases[i]) 
										|| column[0].replaceFirst("min\\(", "").equals(table.getName()))
								{	if(mps[i].containsKey(table.getName()+"."+accol))
									qprts.add("min("+table.getName()+"."+accol+")");
									else throw new Exception("Invalid column name");
									aggr = true;
								}
							}
							else if(column[1].indexOf("max(")!=-1)
							{
								String accol = column[1].replaceFirst("\\)", "");
								accol = accol.replaceFirst("\\)", "");							
								if(column[0].replaceFirst("max\\(", "").equals(aliases[i]) 
										|| column[0].replaceFirst("max\\(", "").equals(table.getName()))
								{	if(mps[i].containsKey(table.getName()+"."+accol))
									qprts.add("max("+table.getName()+"."+accol+")");
									else throw new Exception("Invalid column name");
									aggr = true;
								}
							}
							else if(column[1].indexOf("first(")!=-1)
							{
								String accol = column[1].replaceFirst("\\)", "");
								accol = accol.replaceFirst("\\)", "");							
								if(column[0].replaceFirst("first\\(", "").equals(aliases[i]) 
										|| column[0].replaceFirst("first\\(", "").equals(table.getName()))
								{	if(mps[i].containsKey(table.getName()+"."+accol))
									qprts.add("first("+table.getName()+"."+accol+")");
									else throw new Exception("Invalid column name");
									aggr = true;
								}
							}
							else if(column[1].indexOf("last(")!=-1)
							{
								String accol = column[1].replaceFirst("\\)", "");
								accol = accol.replaceFirst("\\)", "");							
								if(column[0].replaceFirst("last\\(", "").equals(aliases[i]) 
										|| column[0].replaceFirst("last\\(", "").equals(table.getName()))
								{	if(mps[i].containsKey(table.getName()+"."+accol))
									qprts.add("last("+table.getName()+"."+accol+")");
									else throw new Exception("Invalid column name");
									aggr = true;
								}
							}
							else
							{
								if(column[0].equals(table.getName()) || column[0].equals(aliases[i]))
								{
									if(mps[i].containsKey(table.getName()+"."+column[1]))
										qprts.add(table.getName()+"."+column[1]);
									else throw new Exception("Invalid column name");
								}
							}
						}
						else if(column.length==1)
						{						
							if((column[0].indexOf("count(")!=-1))
							{
								String accol = column[0].replaceFirst("count\\(", "");
								accol = accol.replaceFirst("\\)", "");
								boolean fl = false,ex = false;
								for(int u=0;u<tables.length;u++)
								{
									if(mps[u].containsKey(table.getName()+"."+accol))
									{
										if(fl)
										{
											ex = false;
											break;
										}
										else
										{
											fl = true;
											ex = true;
										}
									}
								}
								if(!ex || !fl)
									throw new Exception("Ambigious column name");
								if(mps[i].containsKey(table.getName()+"."+accol))
									qprts.add(column[0]);
								aggr = true;
								countss ++;
							}
							else if(column[0].indexOf("sum(")!=-1)
							{
								String accol = column[0].replaceFirst("sum\\(", "");
								accol = accol.replaceFirst("\\)", "");
								boolean fl = false,ex = false;
								for(int u=0;u<tables.length;u++)
								{
									if(mps[u].containsKey(table.getName()+"."+accol))
									{
										if(fl)
										{
											ex = false;
											break;
										}
										else
										{
											fl = true;
											ex = true;
										}
									}
								}
								if(!ex || !fl)
									throw new Exception("Ambigious column name");
								if(mps[i].containsKey(table.getName()+"."+accol))
									qprts.add(column[0]);
								aggr = true;
							}
							else if(column[0].indexOf("avg(")!=-1)
							{
								String accol = column[0].replaceFirst("avg\\(", "");
								accol = accol.replaceFirst("\\)", "");
								boolean fl = false,ex = false;
								for(int u=0;u<tables.length;u++)
								{
									if(mps[u].containsKey(table.getName()+"."+accol))
									{
										if(fl)
										{
											ex = false;
											break;
										}
										else
										{
											fl = true;
											ex = true;
										}
									}
								}
								if(!ex || !fl)
									throw new Exception("Ambigious column name");
								if(mps[i].containsKey(table.getName()+"."+accol))
									qprts.add(column[0]);
								aggr = true;
							}
							else if(column[0].indexOf("min(")!=-1)
							{
								String accol = column[0].replaceFirst("min\\(", "");
								accol = accol.replaceFirst("\\)", "");
								boolean fl = false,ex = false;
								for(int u=0;u<tables.length;u++)
								{
									if(mps[u].containsKey(table.getName()+"."+accol))
									{
										if(fl)
										{
											ex = false;
											break;
										}
										else
										{
											fl = true;
											ex = true;
										}
									}
								}
								if(!ex || !fl)
									throw new Exception("Ambigious column name");
								if(mps[i].containsKey(table.getName()+"."+accol))
									qprts.add(column[0]);
								aggr = true;
							}
							else if(column[0].indexOf("max(")!=-1)
							{
								String accol = column[0].replaceFirst("max\\(", "");
								accol = accol.replaceFirst("\\)", "");
								boolean fl = false,ex = false;
								for(int u=0;u<tables.length;u++)
								{
									if(mps[u].containsKey(table.getName()+"."+accol))
									{
										if(fl)
										{
											ex = false;
											break;
										}
										else
										{
											fl = true;
											ex = true;
										}
									}
								}
								if(!ex || !fl)
									throw new Exception("Ambigious column name");
								if(mps[i].containsKey(table.getName()+"."+accol))
									qprts.add(column[0]);
								aggr = true;
							}
							else if(column[0].indexOf("first(")!=-1)
							{
								String accol = column[0].replaceFirst("first\\(", "");
								accol = accol.replaceFirst("\\)", "");
								boolean fl = false,ex = false;
								for(int u=0;u<tables.length;u++)
								{
									if(mps[u].containsKey(table.getName()+"."+accol))
									{
										if(fl)
										{
											ex = false;
											break;
										}
										else
										{
											fl = true;
											ex = true;
										}
									}
								}
								if(!ex || !fl)
									throw new Exception("Ambigious column name");
								if(mps[i].containsKey(table.getName()+"."+accol))
									qprts.add(column[0]);
								aggr = true;
							}
							else if(column[0].indexOf("last(")!=-1)
							{
								String accol = column[0].replaceFirst("last\\(", "");
								accol = accol.replaceFirst("\\)", "");
								boolean fl = false,ex = false;
								for(int u=0;u<tables.length;u++)
								{
									if(mps[u].containsKey(table.getName()+"."+accol))
									{
										if(fl)
										{
											ex = false;
											break;
										}
										else
										{
											fl = true;
											ex = true;
										}
									}
								}
								if(!ex || !fl)
									throw new Exception("Ambigious column name");
								if(mps[i].containsKey(table.getName()+"."+accol))
									qprts.add(column[0]);
								aggr = true;
							}
							else
								qprts.add(column[0]);
						}					
					}	
					if(tables.length==1)
						cond = new String[]{subq};
					else
						cond = getConditions(subq, mps[i], aliases[i],table.getName());
					qpartz[i] = qprts.toArray(new String[qprts.size()]);
					qprts.clear();
					String con = "";
					for (int k = 0; k < qpartz[i].length; k++)
					{
						int i1 = getCharCount(qpartz[i][k], '(');
						int i2 = getCharCount(qpartz[i][k], ')');
						if(i1==i2)
						{
							con = qpartz[i][k];
						}
						else
						{
							while(i1!=i2)
							{
								con += qpartz[i][k] + ",";
								i1 = getCharCount(con, '(');
								i2 = getCharCount(con, ')');
								if(i1!=i2)k = k+1;
								
							}
							con = con.substring(0,con.length()-1);
						}
						if(checkColumnExists(con, objtab, table.getName()))
						{	
							qprts.add(con);
							con = "";
						}
					}
					qpartz[i] = qprts.toArray(new String[qprts.size()]);
					if(distinct)
						qs[i] = (HashSet<JDBObject>)selectAMEFObjectsDo("temp", table, (aggr && tables.length>1)?new String[]{}:qpartz[i], null , cond[0], objtab, aggr?tables.length>1:tables.length==1, aggr, grpbycol);
					else
						qs[i] = (ArrayList<JDBObject>)selectAMEFObjectso("temp", table, (aggr && tables.length>1)?new String[]{}:qpartz[i], null , cond[0], objtab, aggr?tables.length>1:tables.length==1, aggr, grpbycol);
				}
				else
				{
					if(tables.length==1)
						cond = new String[]{subq};
					else
						cond = getConditions(subq, mps[i], aliases[i],table.getName());
					qpartz[i] = new String[]{"count(*)"};
					if(distinct)
						qs[i] = (HashSet<JDBObject>)selectAMEFObjectsDo("temp", table, tables.length==1?new String[]{"count(*)"}:new String[]{}, null , cond[0], objtab, true, false, grpbycol);
					else
						qs[i] = (ArrayList<JDBObject>)selectAMEFObjectso("temp", table, tables.length==1?new String[]{"count(*)"}:new String[]{}, null , cond[0], objtab, true, false, grpbycol);
				}
			}
			mps[tables.length] = new HashMap<String, Integer>();
			mpt[tables.length] = new HashMap<String, String>();
			if(aggr || imaginaryaggr)
			{
				aggVals = new LinkedHashMap<Object, AggInfo>();
				rowVals = new LinkedHashMap<Object, AggInfo>();
			}
			if(tables.length==1)
			{
				if(imaginaryaggr)
				{
					int k = 0;
					JDBObject obje = new JDBObject();
					for (Iterator<JDBObject> iter1 = qs[0].iterator();iter1.hasNext();k++)//(int k = 0; k < qs[j].size(); k++)
					{
						JDBObject objm = iter1.next();
						createRow("count(*)", objm, obje, aggVals, mps[0],mpt[0],mps[0],mpt[0],grpbycol,rowVals,tables[0],tables[0]);
					}
					qs[0].clear();
					for (Iterator iter = rowVals.entrySet().iterator(); iter.hasNext();)
					{
						Map.Entry<Object,AggInfo> entry = (Map.Entry<Object,AggInfo>)iter.next();					
						qs[0].add((JDBObject)(entry.getValue().count));
					}
				}
				long dl = 0;
				if(limit==-1)
					limit = qs[0].size();
				for (Iterator<JDBObject> iter = qs[0].iterator();iter.hasNext();)//(int i = 0; i < qs[tables.length].size(); i++)
				{
					if(dl++<limit)
						q.add(JdbResources.getEncoder().encodeWL(iter.next(), true));
					else break;
				}
				return;
			}
			if(distinct)
				qs[tables.length] = new HashSet<JDBObject>();
			else
				qs[tables.length] = new ArrayList<JDBObject>();
			boolean[][] flags = new boolean[tables.length][]; 
			int i = 0;
			cond = getConditionsf(subq, mpf, aliases, tabns);
			
			if(!grpbycol.equals(""))
			{
				String[] grpcs = grpbycol.split(","); 
				for(int v=0;v<grpcs.length;v++)
				{
					boolean fl = false,ex = false;
					for(int u=0;u<tables.length;u++)
					{
						if(mps[u].containsKey(tabns[i]+"."+grpcs[v]))
						{
							if(fl)
							{
								ex = false;
								break;
							}
							else
							{
								fl = true;
								ex = true;
							}
						}
						else if(grpcs[v].indexOf(".")!=-1)
						{
							//if(mps[u].containsKey(grpcs[v].split("\\.")[1])
							//		&& grpcs[v].split("\\.")[0].equals(tabns[u]))
							if(mps[u].containsKey(grpcs[v]))
							{
								if(fl)
								{
									ex = false;
									break;
								}
								else
								{
									fl = true;
									ex = true;
								}
							}
						}
					}
					if(!ex || !fl)
						throw new Exception("Ambigious Group By clause");
				}
				
			}
			/*boolean origaggr = aggr;
			if(tables.length>2 && aggr)
				aggr = false;*/
			for (int ii = 0; ii < aliases.length; ii++) 
			{
				for (int j = 0; j < finjndcond.length; j++) 
				{
					if(aliases[ii]!=null && !aliases[ii].equals("") && finjndcond[j]!=null && finjndcond[j].indexOf(aliases[ii]+".")!=-1)
					{
						finjndcond[j] = finjndcond[j].replaceFirst(aliases[ii]+".", tabns[ii]+".");
					}
				}
			}
			int j=1;
			while (j < tables.length)
			{
				i = 0;				
				for (Iterator<JDBObject> iter = qs[0].iterator();iter.hasNext();i++)//(int i = 0; i < qs[0].size(); i++)
				{					
					JDBObject objm = iter.next();						
					if(flags[0]==null)
						flags[0] = new boolean[qs[0].size()];
					if (j < tables.length)
					{
						int k = 0;
						for (Iterator<JDBObject> iter1 = qs[j].iterator();iter1.hasNext();k++)//(int k = 0; k < qs[j].size(); k++)
						{

							JDBObject obje = iter1.next();
							if(flags[j]==null)
								flags[j] = new boolean[qs[j].size()];
							boolean added = false;
							boolean condi = (finjndcond.length>j)?evaluate2(objm, obje, finjndcond[j], mps[0], mps[j], aliases[0], aliases[j])
									:evaluate2(objm, obje, cond[1], mps[0], mps[j], aliases[0], aliases[j]);
							//boolean condi = evaluate2(objm, obje, cond[1], mps[0], mps[j], aliases[0], aliases[j]);//(tables.length==2)?evaluate2(objm, obje, cond[1], mps[0], mps[j], aliases[0], aliases[j]):true;
							if(condi || type.equals("left") || type.equals("right") || type.equals("full"))
							{
								JDBObject jdbo = new JDBObject();
								if(type.equals("") || types[j-1].equals("left"))
								{
									if(condi)
									{
										flags[0][i] = true;
										flags[j][k] = true;
										for (int l = 0; l < objm.getPackets().size(); l++)
										{							
											jdbo.addPacket(objm.getPackets().get(l).getValue(),
													objm.getPackets().get(l).getType());
										}	
										for (int l = 0; l < obje.getPackets().size(); l++)
										{							
											jdbo.addPacket(obje.getPackets().get(l).getValue(),
													obje.getPackets().get(l).getType());
										}	
									}
									else if(!flags[0][i])
									{
										flags[0][i] = true;
										flags[j][k] = true;
										for (int l = 0; l < objm.getPackets().size(); l++)
										{							
											jdbo.addPacket(objm.getPackets().get(l).getValue(),
													objm.getPackets().get(l).getType());
										}	
										for (int l = 0; l < obje.getPackets().size(); l++)
										{							
											jdbo.addNullPacket(JDBObject.
													getEqvNullType(obje.getPackets().get(l).getType()));
										}
									}
								}
								else if(types[j-1].equals("right"))
								{
									if(condi)
									{
										flags[0][i] = true;
										flags[j][k] = true;
										for (int l = 0; l < objm.getPackets().size(); l++)
										{							
											jdbo.addPacket(objm.getPackets().get(l).getValue(),
													objm.getPackets().get(l).getType());
										}	
										for (int l = 0; l < obje.getPackets().size(); l++)
										{							
											jdbo.addPacket(obje.getPackets().get(l).getValue(),
													obje.getPackets().get(l).getType());
										}	
									}
									else if(!flags[j][k])
									{
										flags[0][i] = true;
										flags[j][k] = true;
										for (int l = 0; l < objm.getPackets().size(); l++)
										{							
											jdbo.addNullPacket(JDBObject.
													getEqvNullType(objm.getPackets().get(l).getType()));
										}	
										for (int l = 0; l < obje.getPackets().size(); l++)
										{							
											jdbo.addNullPacket(JDBObject.
													getEqvNullType(obje.getPackets().get(l).getType()));
										}
									}
								}
								else if(types[j-1].equals("full"))
								{
									if(condi)
									{
										flags[0][i] = true;
										flags[j][k] = true;
										for (int l = 0; l < objm.getPackets().size(); l++)
										{							
											jdbo.addPacket(objm.getPackets().get(l).getValue(),
													objm.getPackets().get(l).getType());
										}	
										for (int l = 0; l < obje.getPackets().size(); l++)
										{							
											jdbo.addPacket(obje.getPackets().get(l).getValue(),
													obje.getPackets().get(l).getType());
										}	
									}
									else
									{
										if(!flags[0][i] && k==qs[j].size()-1)
										{
											flags[0][i] = true;
											if(added)
												jdbo = new JDBObject();
											for (int l = 0; l < objm.getPackets().size(); l++)
											{							
												jdbo.addPacket(objm.getPackets().get(l).getValue(),
														objm.getPackets().get(l).getType());
											}	
											for (int l = 0; l < obje.getPackets().size(); l++)
											{							
												jdbo.addNullPacket(JDBObject.
														getEqvNullType(obje.getPackets().get(l).getType()));
											}
											if(jdbo.getPackets().size()>0)
											{
												qs[tables.length].add(jdbo);
												added = true;
											}
											
										}
										if(!flags[j][k] && i==qs[0].size()-1)
										{
											flags[j][k] = true;
											if(added)
												jdbo = new JDBObject();
											for (int l = 0; l < objm.getPackets().size(); l++)
											{							
												jdbo.addNullPacket(JDBObject.
														getEqvNullType(objm.getPackets().get(l).getType()));
											}	
											for (int l = 0; l < obje.getPackets().size(); l++)
											{							
												jdbo.addNullPacket(JDBObject.
														getEqvNullType(obje.getPackets().get(l).getType()));
											}
											if(jdbo.getPackets().size()>0)
											{
												qs[tables.length].add(jdbo);
												added = true;
											}
										}
									}
								}
								if(jdbo.getPackets().size()>0 && !added)
									qs[tables.length].add(jdbo);
							}
						
							/*JDBObject obje = iter1.next();
							if(flags[j]==null)
								flags[j] = new boolean[qs[j].size()];
							boolean added = false;
							boolean condi = evaluate(objm, obje, cond[1], mps[0], mps[j], tabns[0], tabns[j]);
							if(condi || type.equals("left") || type.equals("right") || type.equals("full"))
							{							
								JDBObject jdbo = new JDBObject();
								if(type.equals("left") || type.equals(""))
								{
									if(condi)
									{
										flags[0][i] = true;
										flags[j][k] = true;
										for (int tt = 0; tt < qpartz[0].length; tt++)
										{	
											if(isScalarFunc(qpartz[0][tt], objtbs[0], tabns[0]))
											{
												if(aggr)
													aggVals.put(qpartz[0][tt], null);
												else
													createRow(qpartz[0][tt], jdbo, objm, mps[0], true);
											}
											else
											{
												boolean agg = createRow(qpartz[0][tt], objm, obje, aggVals, mps[0],mpt[0],mps[j],mpt[j],grpbycol,rowVals,tables[0],tables[j]);
												if((!agg && grpbycol.indexOf(qpartz[0][tt])==-1))
													throw new Exception("Invalid group by clause");
											}
										}
										for (int tt = 0; tt < qpartz[j].length; tt++)
										{	
											if(isScalarFunc(qpartz[j][tt], objtbs[j], tabns[j]))
											{
												if(aggr)
												aggVals.put(qpartz[j][tt], null);
												else
													createRow(qpartz[j][tt], jdbo, objm, mps[j], true);
											}
											else
											{
												boolean agg = createRow(qpartz[j][tt], obje, objm, aggVals, mps[j],mpt[j],mps[0],mpt[0],grpbycol,rowVals,tables[j],tables[0]);
												if((!agg && grpbycol.indexOf(qpartz[j][tt])==-1))
													throw new Exception("Invalid group by clause");
											}
										}
									}
									else if(!flags[0][i])
									{
										flags[0][i] = true;
										flags[j][k] = true;
										for (int tt = 0; tt < qpartz[0].length; tt++)
										{	
											if(isScalarFunc(qpartz[0][tt], objtbs[0], tabns[0]))
											{
												if(aggr)
												aggVals.put(qpartz[0][tt], null);
												else
													createRow(qpartz[0][tt], jdbo, objm, mps[0], true);
											}
											else
											{
												boolean agg = createRow(qpartz[0][tt], objm, obje, aggVals, mps[0],mpt[0],mps[j],mpt[j],grpbycol,rowVals,tables[0],tables[j]);
												if((!agg && grpbycol.indexOf(qpartz[0][tt])==-1))
													throw new Exception("");
											}
										}
										for (int tt = 0; tt < qpartz[j].length; tt++)
										{
											String cond1 = qpartz[j][tt];
											//if(cond1.indexOf(".")!=-1)
											//	cond1 = cond1.split("\\.")[1];
											jdbo.addNullPacket(JDBObject.
														getEqvNullType(obje.getPackets().get(mps[j].get(cond1)).getType()));
										}
									}
								}
								else if(type.equals("right"))
								{
									if(condi)
									{
										flags[0][i] = true;
										flags[j][k] = true;
										for (int tt = 0; tt < qpartz[0].length; tt++)
										{	
											if(isScalarFunc(qpartz[0][tt], objtbs[0], tabns[0]))
											{
												if(aggr)
												aggVals.put(qpartz[0][tt], null);
												else
													createRow(qpartz[0][tt], jdbo, objm, mps[0], true);
											}
											else
											{
												boolean agg = createRow(qpartz[0][tt], objm, obje, aggVals, mps[0],mpt[0],mps[j],mpt[j],grpbycol,rowVals,tables[0],tables[j]);
												if((!agg && grpbycol.indexOf(qpartz[0][tt])==-1))
													throw new Exception("Invalid group by clause");
											}
										}
										for (int tt = 0; tt < qpartz[j].length; tt++)
										{	
											if(isScalarFunc(qpartz[j][tt], objtbs[j], tabns[j]))
											{
												if(aggr)
												aggVals.put(qpartz[j][tt], null);
												else
													createRow(qpartz[j][tt], jdbo, objm, mps[j], true);
											}
											else
											{
												boolean agg = createRow(qpartz[j][tt], obje, objm, aggVals, mps[j],mpt[j],mps[0],mpt[0],grpbycol,rowVals,tables[j],tables[0]);
												if((!agg && grpbycol.indexOf(qpartz[j][tt])==-1))
													throw new Exception("Invalid group by clause");
											}
										}
									}
									else if(!flags[j][k])
									{
										flags[0][i] = true;
										flags[j][k] = true;
										for (int tt = 0; tt < qpartz[0].length; tt++)
										{
											String cond1 = qpartz[0][tt];
											//if(cond1.indexOf(".")!=-1)
											//	cond1 = cond1.split("\\.")[1];
											jdbo.addNullPacket(JDBObject.
														getEqvNullType(objm.getPackets().get(mps[0].get(cond1)).getType()));
										}
										for (int tt = 0; tt < qpartz[j].length; tt++)
										{	
											if(isScalarFunc(qpartz[j][tt], objtbs[j], tabns[j]))
											{
												if(aggr)
												aggVals.put(qpartz[j][tt], null);
												else
													createRow(qpartz[j][tt], jdbo, objm, mps[j], true);
											}
											else
											{
												boolean agg = createRow(qpartz[j][tt], obje, objm, aggVals, mps[j],mpt[j],mps[0],mpt[0],grpbycol,rowVals,tables[j],tables[0]);
												if((!agg && grpbycol.indexOf(qpartz[j][tt])==-1))
													throw new Exception("Invalid group by clause");
											}
										}
									}
								}
								else if(type.equals("full"))
								{
									if(condi)
									{
										flags[0][i] = true;
										flags[j][k] = true;
										for (int tt = 0; tt < qpartz[0].length; tt++)
										{	
											if(isScalarFunc(qpartz[0][tt], objtbs[0], tabns[0]))
											{
												if(aggr)
												aggVals.put(qpartz[0][tt], null);
												else
													createRow(qpartz[0][tt], jdbo, objm, mps[0], true);
											}
											else
											{
												boolean agg = createRow(qpartz[0][tt], objm, obje, aggVals, mps[0],mpt[0],mps[j],mpt[j],grpbycol,rowVals,tables[0],tables[j]);
												if((!agg && grpbycol.indexOf(qpartz[0][tt])==-1))
													throw new Exception("Invalid group by clause");
											}
										}
										for (int tt = 0; tt < qpartz[j].length; tt++)
										{	
											if(isScalarFunc(qpartz[j][tt], objtbs[j], tabns[j]))
											{
												if(aggr)
												aggVals.put(qpartz[j][tt], null);
												else
													createRow(qpartz[j][tt], jdbo, objm, mps[j], true);
											}
											else
											{
												boolean agg = createRow(qpartz[j][tt], obje, objm, aggVals, mps[j],mpt[j],mps[0],mpt[0],grpbycol,rowVals,tables[j],tables[0]);
												if((!agg && grpbycol.indexOf(qpartz[j][tt])==-1))
													throw new Exception("Invalid group by clause");
											}
										}
									}
									else
									{
										if(!flags[0][i] && k==qs[j].size()-1)
										{
											flags[0][i] = true;
											if(added)
												jdbo = new JDBObject();
											for (int tt = 0; tt < qpartz[0].length; tt++)
											{	
												if(isScalarFunc(qpartz[0][tt], objtbs[0], tabns[0]))
												{
													if(aggr)
													aggVals.put(qpartz[0][tt], null);
													else
														createRow(qpartz[0][tt], jdbo, objm, mps[0], true);
												}
												else
												{
													boolean agg = createRow(qpartz[0][tt], objm, obje, aggVals, mps[0],mpt[0],mps[j],mpt[j],grpbycol,rowVals,tables[0],tables[j]);
													if((!agg && grpbycol.indexOf(qpartz[0][tt])==-1))
														throw new Exception("Invalid group by clause");
												}
											}
											for (int tt = 0; tt < qpartz[j].length; tt++)
											{	
												String cond1 = qpartz[j][tt];
												//if(cond1.indexOf(".")!=-1)
												//	cond1 = cond1.split("\\.")[1];
												jdbo.addNullPacket(JDBObject.
															getEqvNullType(obje.getPackets().get(mps[j].get(cond1)).getType()));
											}
											if(jdbo.getPackets().size()>0)
											{
												qs[tables.length].add(jdbo);
												added = true;
											}
											
										}
										if(!flags[j][k] && i==qs[0].size()-1)
										{
											flags[j][k] = true;
											if(added)
												jdbo = new JDBObject();
											for (int tt = 0; tt < qpartz[0].length; tt++)
											{	
												String cond1 = qpartz[0][tt];
												//if(cond1.indexOf(".")!=-1)
												//	cond1 = cond1.split("\\.")[1];
												jdbo.addNullPacket(JDBObject.
															getEqvNullType(obje.getPackets().get(mps[0].get(cond1)).getType()));
											}
											for (int tt = 0; tt < qpartz[j].length; tt++)
											{	
												if(isScalarFunc(qpartz[j][tt], objtbs[j], tabns[j]))
												{
													if(aggr)
													aggVals.put(qpartz[j][tt], null);
													else
														createRow(qpartz[j][tt], jdbo, objm, mps[j], true);
												}
												else
												{
													boolean agg = createRow(qpartz[j][tt], obje, objm, aggVals, mps[j],mpt[j],mps[0],mpt[0],grpbycol,rowVals,tables[j],tables[0]);
													if((!agg && grpbycol.indexOf(qpartz[j][tt])==-1))
														throw new Exception("Invalid group by clause");
												}
											}
											if(jdbo.getPackets().size()>0)
											{
												qs[tables.length].add(jdbo);
												added = true;
											}
										}
									}
								}
								if(jdbo.getPackets().size()>0 && !added)
									qs[tables.length].add(jdbo);
							}*/
						}
					}
				}
				mps[tables.length].putAll(mps[0]);
				for (Iterator iter = mps[j].entrySet().iterator(); iter.hasNext();)
				{
					Map.Entry<String,Integer> entry = (Map.Entry<String,Integer>)iter.next();
					mps[tables.length].put(entry.getKey(), entry.getValue()+mps[0].size());
				}
				mpt[tables.length].putAll(mpt[0]);
				for (Iterator iter = mpt[j].entrySet().iterator(); iter.hasNext();)
				{
					Map.Entry<String,String> entry = (Map.Entry<String,String>)iter.next();
					mpt[tables.length].put(entry.getKey(), entry.getValue());
				}
				mps[0] = mps[tables.length];
				mpt[0] = mpt[tables.length];
				mps[tables.length] =  new HashMap<String, Integer>();
				mpt[tables.length] = new HashMap<String, String>();
				qs[0] = qs[tables.length];
				flags[0] = new boolean[qs[0].size()];
				if(distinct)
					qs[tables.length] = new HashSet<JDBObject>();
				else
					qs[tables.length] = new ArrayList<JDBObject>();
				j++;
			}
			if(aggr)
			{	
				for (Iterator<JDBObject> iter1 = qs[0].iterator();iter1.hasNext();)//(int k = 0; k < qs[j].size(); k++)
				{
					JDBObject objm = iter1.next();
					//boolean condi = evaluate(objm, objm, cond[1], mps[0], mps[0], aliases[0], aliases[0]);
					//if(!condi)continue;
					for (int tt1 = 0; tt1 < qpartz.length; tt1++)
					{
						for (int tt = 0; tt < qpartz[tt1].length; tt++)
						{	
							if(isAggrFuncF(qpartz[tt1][tt]))
							{
								createRow(qpartz[tt1][tt], objm, objm, aggVals, mps[0],mpt[0],mps[0],mpt[0],grpbycol,rowVals,tables[0],tables[0]);								
							}
							else
							{
								if(grpbycol.indexOf(qpartz[tt1][tt])==-1)
									throw new Exception("Invalid group by clause");
								aggVals.put(qpartz[tt1][tt], new AggInfo<Long>());
							}
						}
					}
				}
				qs[0].clear();
				for (Iterator iter = rowVals.entrySet().iterator(); iter.hasNext();)
				{
					JDBObject obh = new JDBObject();
					Map.Entry<Object,AggInfo> entry = (Map.Entry<Object,AggInfo>)iter.next();
					Map mpss = new HashMap<String,Object>();
					Map mpst = new HashMap<String,Character>();
					for (Iterator iter1 = aggVals.entrySet().iterator(); iter1.hasNext();)
					{
						Map.Entry<Object,AggInfo> entry1 = (Map.Entry<Object,AggInfo>)iter1.next();
						if(mps[0].get((String)entry1.getKey())!=null)
						{
							JDBObject objt = (JDBObject)entry.getKey();
							for (int jj = 0; jj < objt.getPackets().size(); jj++)
							{
								if(objt.getPackets().get(jj).getNameStr().equals((String)entry1.getKey()))
								{
									obh.addPacket(objt.getPackets().get(jj).getValue(),
										objt.getPackets().get(jj).getType());
									mpss.put((String)entry1.getKey(),
											objt.getPackets().get(jj).getValue());
									mpst.put((String)entry1.getKey(), objt.getPackets().get(jj).getType());
									break;
								}
							}
							
						}
						else if(((String)entry1.getKey()).startsWith("count"))
						{
							if(grpbycol.equals(""))
							{
								obh.addPacket((entry.getValue().cnt+1)/countss);
								mpss.put((String)entry1.getKey(),
										JdbResources.longToByteArray((long)(entry.getValue().cnt+1)/countss,8));
								mpst.put((String)entry1.getKey(), 'l');
							}
							else
							{
								obh.addPacket(entry.getValue().cnt+1);
								mpss.put((String)entry1.getKey(),
										JdbResources.longToByteArray(entry.getValue().cnt+1,8));
								mpst.put((String)entry1.getKey(), 'l');
							}
						}
						else if(isScalarFuncF((String)entry1.getKey()))
						{
							String con = createRow((String)entry1.getKey(), obh, (JDBObject)entry.getKey(), mps[0], false);
							mpss.put((String)entry1.getKey(),
									obh.getPackets().get(obh.getPackets().size()-1).getValue());
							mpst.put((String)entry1.getKey(),obh.getPackets().get(obh.getPackets().size()-1).getType());
						}					
						else
						{
							obh.addPacket(entry1.getValue().getFinalValue());
							mpss.put((String)entry1.getKey(),
									entry1.getValue().getFinalValue());
						}								
					}
					boolean condi = evaluateHv(obh, havcls, mpss, mpst, aggVals);
					if(condi)
						qs[0].add(obh);
				}
			}
			else if(imaginaryaggr)
			{						
				for (Iterator iter = rowVals.entrySet().iterator(); iter.hasNext();)
				{
					Map.Entry<Object,AggInfo> entry = (Map.Entry<Object,AggInfo>)iter.next();					
					qs[0].add((JDBObject)(entry.getValue().count));
				}
			}
			long dl = 0;
			if(limit==-1)
				limit = qs[0].size();
			for (Iterator<JDBObject> iter = qs[0].iterator();iter.hasNext();)//(int i = 0; i < qs[tables.length].size(); i++)
			{
				if(dl++<limit)
					q.add(JdbResources.getEncoder().encodeWL(iter.next(), true));
				else break;
			}
			//System.out.println(qs[tables.length]);
		}
	}
	
	private boolean isScalarFunc(String cond,JDBObject objtab, String table)
	{
		if(cond.indexOf(".")==-1)
			table = "";
		else 
			table = table + ".";
		for (int i = 0; i < objtab.getPackets().size(); i++)
		{
			if(cond.equals(table+objtab.getPackets().get(i).getNameStr()) 
					|| cond.indexOf("ucase("+table+objtab.getPackets().get(i).getNameStr())!=-1
					|| cond.indexOf("lcase("+table+objtab.getPackets().get(i).getNameStr())!=-1
					|| cond.indexOf("len("+table+objtab.getPackets().get(i).getNameStr())!=-1
					|| cond.indexOf("format("+table+objtab.getPackets().get(i).getNameStr())!=-1
					|| cond.indexOf("mid("+table+objtab.getPackets().get(i).getNameStr())!=-1
					|| cond.indexOf("round("+table+objtab.getPackets().get(i).getNameStr())!=-1)
				return true;
		}
		return false;
	}
	
	private boolean isScalarFuncF(String cond)
	{
		if(cond.indexOf("ucase(")!=-1
				|| cond.indexOf("lcase(")!=-1
				|| cond.indexOf("len(")!=-1
				|| cond.indexOf("format(")!=-1
				|| cond.indexOf("mid(")!=-1
				|| cond.indexOf("round(")!=-1)
			return true;
		return false;
	}
	
	private boolean isAggrFuncF(String cond)
	{
		if(cond.indexOf("first(")!=-1
				|| cond.indexOf("last(")!=-1
				|| cond.indexOf("count(")!=-1
				|| cond.indexOf("sum(")!=-1
				|| cond.indexOf("avg(")!=-1
				|| cond.indexOf("max(")!=-1
				|| cond.indexOf("min")!=-1)
			return true;
		return false;
	}
	
	private boolean checkColumnExists(String cond,JDBObject objtab, String table)
	{
		if(cond.indexOf(".")==-1)
			table = "";
		else 
			table = table + ".";
		for (int i = 0; i < objtab.getPackets().size(); i++)
		{
			if(cond.equals(table+objtab.getPackets().get(i).getNameStr())
					|| cond.indexOf("ucase("+table+objtab.getPackets().get(i).getNameStr())!=-1
					|| cond.indexOf("lcase("+table+objtab.getPackets().get(i).getNameStr())!=-1
					|| cond.indexOf("len("+table+objtab.getPackets().get(i).getNameStr())!=-1
					|| cond.indexOf("format("+table+objtab.getPackets().get(i).getNameStr())!=-1
					|| cond.indexOf("mid("+table+objtab.getPackets().get(i).getNameStr())!=-1
					|| cond.indexOf("first("+table+objtab.getPackets().get(i).getNameStr())!=-1
					|| cond.indexOf("last("+table+objtab.getPackets().get(i).getNameStr())!=-1
					|| cond.indexOf("round("+table+objtab.getPackets().get(i).getNameStr())!=-1
					|| cond.indexOf("count("+table+objtab.getPackets().get(i).getNameStr())!=-1
					|| cond.indexOf("max("+table+objtab.getPackets().get(i).getNameStr())!=-1
					|| cond.indexOf("min("+table+objtab.getPackets().get(i).getNameStr())!=-1
					|| cond.indexOf("sum("+table+objtab.getPackets().get(i).getNameStr())!=-1
					|| cond.indexOf("avg("+table+objtab.getPackets().get(i).getNameStr())!=-1
				    || cond.equals("now()") || cond.equals("sysdate()")
				    || cond.equals("count(*)"))
				return true;
		}
		return false;
	}
	
	
	static class AggInfo<T>
	{
		AggInfo(){}
		AggInfo(String name,String typeagg,T t,String cond, byte[] value, char type)
		{
			this.name = name;
			this.typeagg = typeagg;
			this.count = t;
			this.cond = cond;
			this.value = value;
			this.type = type;
		}
		public String name,cond;
		public Object count;	
		public String typeagg;
		public byte[] value;
		public char type;
		public long cnt;
		public void incAvg()
		{
			cnt++;
		}
		public Object getFinalValue()
		{
			if(typeagg.equals("avg"))
			{
				if(count instanceof Long)
				{
					Long val = (Long)count;
					double vald = (double)val;
					return (Double)vald/cnt;
				}
				else
				{
					Double val = (Double)count;
					return (Double)val/cnt;
				}
			}
			else 
				return count;
		}	
		public void incCount() {
			if(count instanceof Long && (Long)count!=1)
			{
				long val = (Long)count + 1;
				count = val;
			}
		}	
	}
	
	
	private Object getKey1(JDBObject objm, String grpbycol, String colnam, Map<String, Integer> mp, String type)
	{
		return colnam+type;
	}
	
	private Object getKey(JDBObject objm, JDBObject objm1 ,String grpbycol, String colnam, Map<String, Integer> mp1, Map<String, Integer> mp2,String type, String table, String table1)
	{
		if(grpbycol==null || grpbycol.equals(""))
			return colnam+type;
		else if(grpbycol.equals(table+"."+colnam) || grpbycol.equals(table1+"."+colnam))
		{
			colnam = table+"."+colnam;
			if(mp1.get(colnam)!=null)
			{
				if(objm.getPackets().get(mp1.get(colnam)).isNumber())
					return objm.getPackets().get(mp1.get(colnam)).getNumericValue();
				else if(objm.getPackets().get(mp1.get(colnam)).isFloatingPoint())
					return objm.getPackets().get(mp1.get(colnam)).getDoubleValue();
				else if(objm.getPackets().get(mp1.get(colnam)).isString() || objm.getPackets().get(mp1.get(colnam)).isDate())
					return objm.getPackets().get(mp1.get(colnam)).getValueStr();
				else if(objm.getPackets().get(mp1.get(colnam)).isChar())
					return objm.getPackets().get(mp1.get(colnam)).getValue()[0];
			}
			else if(mp2.get(colnam)!=null)
			{
				if(objm1.getPackets().get(mp2.get(colnam)).isNumber())
					return objm1.getPackets().get(mp2.get(colnam)).getNumericValue();
				else if(objm1.getPackets().get(mp2.get(colnam)).isFloatingPoint())
					return objm1.getPackets().get(mp2.get(colnam)).getDoubleValue();
				else if(objm1.getPackets().get(mp2.get(colnam)).isString() || objm.getPackets().get(mp2.get(colnam)).isDate())
					return objm1.getPackets().get(mp2.get(colnam)).getValueStr();
				else if(objm1.getPackets().get(mp2.get(colnam)).isChar())
					return objm1.getPackets().get(mp2.get(colnam)).getValue()[0];				
			}
			return colnam + type;
		}
		else
		{
			String[] grbcs = grpbycol.split(",");
			JDBObject obj = new JDBObject();
			for (int i = 0; i < grbcs.length; i++)
			{
				if(mp1.get(grbcs[i])!=null)
				{
					obj.addPacket(objm.getPackets().get(mp1.get(grbcs[i])).getValue(),
							objm.getPackets().get(mp1.get(grbcs[i])).getType());
					obj.getPackets().get(i).setName(grbcs[i]);
				}
				/*if(mp2.get(grbcs[i])!=null)
				{
					obj.addPacket(objm1.getPackets().get(mp2.get(grbcs[i])).getValue(),
							objm1.getPackets().get(mp2.get(grbcs[i])).getType());
					obj.getPackets().get(i).setName(grbcs[i]);
				}*/
				else if(table.equals(table1))
				{
					if(mp1.get(grbcs[i])!=null)
					{
						obj.addPacket(objm.getPackets().get(mp1.get(grbcs[i])).getValue(),
								objm.getPackets().get(mp1.get(grbcs[i])).getType());
						obj.getPackets().get(i).setName(grbcs[i]);
					}
				}
				else
					return "INCOMPLETE";
			}
			return obj;
		}
		/*if(objm.isNumber())
			return objm.getLongValue();
		else if(objm.isFloatingPoint())
			return objm.getDoubleValue();
		else if(objm.isString() || objm.isDate())
			return objm.getValueStr();
		else if(objm.isChar())
			return objm.getValue()[0];
		return null;*/
	}
	
	
	private String createRow(String cond,JDBObject obh1,JDBObject obh, Map<String, Integer> valInd, boolean b) throws Exception
	{			
		if(cond.equalsIgnoreCase("sysdate()"))
		{
			obh1.addPacket(new Date());
		}
		else if(cond.equalsIgnoreCase("now()"))
		{
			obh1.addPacket(new Date());
		}
		else if(cond.toLowerCase().indexOf("format(")!=-1)
		{
			String colnam = cond.substring(cond.toLowerCase().indexOf("format(")+7).trim();
			colnam = colnam.substring(0,colnam.length()-1);
			if(colnam.indexOf(".")!=-1)
				colnam = colnam.split("\\.")[1];
			String[] idnts = colnam.split(",");
			String val = null;
			if(idnts.length==2)
			{
				try
				{
					val = obh.getPackets().get(valInd.get(idnts[0])).getValueStr();
					if(val==null)
					{
						if(idnts[0].equalsIgnoreCase("now()") || idnts[0].equalsIgnoreCase("sysdate()"))
						{
							idnts[1] = idnts[1].substring(1,idnts[1].length()-1);
							val = new SimpleDateFormat(idnts[1]).format(new Date());
						}
						else
							throw new Exception("Undefined Column or Function");
					}
					else
					{
						idnts[1] = idnts[1].substring(1,idnts[1].length()-1);
						val = new SimpleDateFormat(idnts[1]).format(
								new SimpleDateFormat("ddMMyyyy HHmmss").parse(val));
					}
					obh1.addPacket(val);
				}
				catch (Exception e) 
				{
					throw new Exception("Invalid argument to format");
				}
			}
			else
			{
				throw new Exception("format() takes 2 arguments");
			}
			obh1.addPacket(new Date());
		}
		else if(cond.toLowerCase().indexOf("ucase(")!=-1)
		{
			String colnam = cond.substring(cond.toLowerCase().indexOf("ucase(")+6);
			colnam = colnam.replaceFirst("\\)", "");
			if(colnam.indexOf(".")!=-1)
				colnam = colnam.split("\\.")[1];
			if(b && obh.getPackets().get(valInd.get(colnam)).isString())
				obh1.addPacket(obh.getPackets().get(valInd.get(colnam)).getValueStr().toUpperCase().getBytes()
					,obh.getPackets().get(valInd.get(colnam)).getType());
			else if(!b)
			{
				for (int i = 0; i < obh.getPackets().size(); i++)
				{
					if(obh.getPackets().get(i).getNameStr().equals(colnam) 
							&& obh.getPackets().get(i).isString())
					{
						obh1.addPacket(obh.getPackets().get(i).getValueStr().toUpperCase().getBytes()
								,obh.getPackets().get(i).getType());
						break;
					}
				}
			}
			else throw new Exception("Invalid column name argument to function");
		}
		else if(cond.toLowerCase().indexOf("lcase(")!=-1)
		{
			String colnam = cond.substring(cond.toLowerCase().indexOf("lcase(")+6);
			colnam = colnam.replaceFirst("\\)", "");
			if(colnam.indexOf(".")!=-1)
				colnam = colnam.split("\\.")[1];
			if(b && obh.getPackets().get(valInd.get(colnam)).isString())
				obh1.addPacket(obh.getPackets().get(valInd.get(colnam)).getValueStr().toLowerCase().getBytes()
					,obh.getPackets().get(valInd.get(colnam)).getType());
			else if(!b)
			{
				for (int i = 0; i < obh.getPackets().size(); i++)
				{
					if(obh.getPackets().get(i).getNameStr().equals(colnam) 
							&& obh.getPackets().get(i).isString())
					{
						obh1.addPacket(obh.getPackets().get(i).getValueStr().toLowerCase().getBytes()
								,obh.getPackets().get(i).getType());
						break;
					}
				}
			}
			else throw new Exception("Invalid column name argument to function");
		}
		else if(cond.toLowerCase().indexOf("len(")!=-1)
		{
			String colnam = cond.substring(cond.toLowerCase().indexOf("len(")+4);
			colnam = colnam.replaceFirst("\\)", "");
			if(colnam.indexOf(".")!=-1)
				colnam = colnam.split("\\.")[1];
			if(b && obh.getPackets().get(valInd.get(colnam)).isString())
				obh1.addPacket(String.valueOf(obh.getPackets().get(valInd.get(colnam)).getValueStr().length()).getBytes()
					,obh.getPackets().get(valInd.get(colnam)).getType());
			else if(!b)
			{
				for (int i = 0; i < obh.getPackets().size(); i++)
				{
					if(obh.getPackets().get(i).getNameStr().equals(colnam) 
							&& obh.getPackets().get(i).isString())
					{
						obh1.addPacket(String.valueOf(obh.getPackets().get(i).getValueStr().length()).getBytes()
								,obh.getPackets().get(i).getType());
						break;
					}
				}
			}
			else throw new Exception("Invalid column name argument to function");
		}
		else if(cond.toLowerCase().indexOf("round(")!=-1)
		{
			if(cond.indexOf(")")==-1)
				return cond;
			String colnam = cond.substring(cond.toLowerCase().indexOf("round(")+6);
			colnam = colnam.replaceFirst("\\)", "");
			if(colnam.indexOf(".")!=-1)
				colnam = colnam.split("\\.")[1];
			String[] idnts = colnam.split(",");
			if(b && obh.getPackets().get(valInd.get(idnts[0])).isNumber())
			{					
				String val = String.valueOf(obh.getPackets().get(valInd.get(idnts[0])).getNumericValue());
				obh1.addPacket(val.getBytes(),'u');
			}
			else if(b && obh.getPackets().get(valInd.get(idnts[0])).isFloatingPoint())
			{					
				String val = null;
				val = obh.getPackets().get(valInd.get(idnts[0])).getValueStr();
				if(idnts.length==2 && val.indexOf(".")!=-1)
				{
					val = val.substring(val.indexOf(".")+1,val.indexOf(".")+1+Integer.parseInt(val.split("\\.")[1]));
				}
				obh1.addPacket(val.getBytes(),obh.getPackets().get(valInd.get(colnam)).getType());
			}
			else if(!b)
			{
				for (int i = 0; i < obh.getPackets().size(); i++)
				{
					if(obh.getPackets().get(i).getNameStr().equals(colnam) 
							&& obh.getPackets().get(i).isNumber())
					{
						String val = String.valueOf(obh.getPackets().get(i).getNumericValue());
						obh1.addPacket(val.getBytes(),'u');
						break;
					}
					else if(obh.getPackets().get(i).getNameStr().equals(colnam) 
							&& obh.getPackets().get(i).isFloatingPoint())
					{
						String val = null;
						val = obh.getPackets().get(i).getValueStr();
						if(idnts.length==2 && val.indexOf(".")!=-1)
						{
							val = val.substring(val.indexOf(".")+1,val.indexOf(".")+1+Integer.parseInt(val.split("\\.")[1]));
						}
						obh1.addPacket(val.getBytes(),obh.getPackets().get(i).getType());
						break;
					}
				}
			}
			else throw new Exception("Invalid column name argument to function");
		}
		else if(cond.toLowerCase().indexOf("mid(")!=-1)
		{
			if(cond.indexOf(")")==-1)
				return cond;
			String colnam = cond.substring(cond.toLowerCase().indexOf("mid(")+4);
			colnam = colnam.replaceFirst("\\)", "");
			//if(colnam.indexOf(".")!=-1)
			//	colnam = colnam.split("\\.")[1];
			String[] idnts = colnam.split(",");
			if(b && obh.getPackets().get(valInd.get(idnts[0])).isString())
			{
				byte[] val = null;
				if(idnts.length==2)
					val = obh.getPackets().get(valInd.get(idnts[0])).
							getValueStr().substring(Integer.parseInt(idnts[1])).getBytes();
				else if(idnts.length==3)
					val = obh.getPackets().get(valInd.get(idnts[0])).
							getValueStr().
								substring(Integer.parseInt(idnts[1]),Integer.parseInt(idnts[2])).getBytes();
				obh1.addPacket(val,obh.getPackets().get(valInd.get(idnts[0])).getType());
			}
			else if(!b)
			{
				for (int i = 0; i < obh.getPackets().size(); i++)
				{
					if(obh.getPackets().get(i).getNameStr().equals(colnam) 
							&& obh.getPackets().get(i).isString())
					{
						byte[] val = null;
						if(idnts.length==2)
							val = obh.getPackets().get(i).
									getValueStr().substring(Integer.parseInt(idnts[1])).getBytes();
						else if(idnts.length==3)
							val = obh.getPackets().get(i).
									getValueStr().
										substring(Integer.parseInt(idnts[1]),Integer.parseInt(idnts[2])).getBytes();
						obh1.addPacket(val,obh.getPackets().get(i).getType());
						break;
					}
				}
			}
			else throw new Exception("Invalid column name argument to function");
		}
		else
		{	
			//if(cond.indexOf(".")!=-1)
			//	cond = cond.split("\\.")[1];		
			obh1.addPacket(obh.getPackets().get(valInd.get(cond)).getValue(), 
					obh.getPackets().get(valInd.get(cond)).getType());
		}
		return "";
	}
	
	private boolean createRow(String cond,JDBObject objm,JDBObject objm1,Map<Object, AggInfo> aggVals,
			Map<String, Integer> mp, Map<String, String> mpt,Map<String, Integer> mp2, Map<String, String> mpt2,
			String grpbycol, Map<Object, AggInfo> rowVals, String table, String table1)
	{
		boolean aggr = false;
		if(cond.indexOf("count(*")!=-1)
		{
			Object key = getKey(objm,objm1,grpbycol,"",mp,mp2,"",table, table1);
			if(key.equals("INCOMPLETE"))
				return true;
			String colnam = table + "*";
			if(rowVals.get(key)==null)
			{	
				Object key1 = cond;//getKey1(objm, grpbycol, colnam,mp,"count");	
				if(aggVals.get(key1)==null)
					aggVals.put(key1, new AggInfo<Long>(colnam,"count",1L,cond,
							null,
							'l'));
				for(int t=0;t<objm1.getPackets().size();t++)
					objm.addPacket(objm1.getPackets().get(t).getValue(), objm1.getPackets().get(t).getType());
				rowVals.put(key, new AggInfo<JDBObject>(colnam,"count",objm,cond,
								null,
								'l'));
			}
			else
			{
				rowVals.get(key).incAvg();
				Object key1 = cond;//getKey1(objm, grpbycol, colnam,mp,"count");	
				if(aggVals.get(key1)==null)
					aggVals.put(key1, new AggInfo<JDBObject>(colnam,"count",objm,cond,
							null,
							'l'));
			}
			aggr = true;
		}
		else if(cond.indexOf("count(")!=-1)
		{
			String colnam = cond.substring(cond.toLowerCase().indexOf("count(")+6);
			colnam = colnam.replaceFirst("\\)", "");
			Object key = getKey(objm,objm1,grpbycol,"",mp,mp2,"",table, table1);	
			if(key.equals("INCOMPLETE"))
				return true;
			if(rowVals.get(key)==null)
			{	
				Object key1 = cond;//getKey1(objm, grpbycol, colnam,mp,"count");	
				if(aggVals.get(key1)==null)
					aggVals.put(key1, new AggInfo<Long>(colnam,"count",1L,cond,
							null,
							'l'));
				rowVals.put(key, new AggInfo<Long>(colnam,"count",1L,cond,
								objm.getPackets().get(mp.get(colnam)).getValue(),
								'l'));
			}
			else
			{
				rowVals.get(key).incAvg();
				Object key1 = cond;//getKey1(objm, grpbycol, colnam,mp,"count");	
				if(aggVals.get(key1)==null)
					aggVals.put(key1, new AggInfo<Long>(colnam,"count",1L,cond,
							null,
							'l'));
			}
			aggr = true;
		}
		else if(cond.indexOf("sum(")!=-1 || cond.indexOf("avg(")!=-1)
		{
			String colnam = null,typed = "sum";
			if(cond.indexOf("sum(")!=-1)
				colnam = cond.substring(cond.toLowerCase().indexOf("sum(")+4);
			else
			{
				colnam = cond.substring(cond.toLowerCase().indexOf("avg(")+4);
				typed = "avg";
			}
			colnam = colnam.replaceFirst("\\)", "");
			Object key = cond;//getKey1(objm, grpbycol, colnam,mp,typed);
			if(mpt.get(colnam).equals("int") || mpt.get(colnam).equals("long"))
			{
				if(aggVals.get(key)==null)
					aggVals.put(key, new AggInfo<Long>(colnam,typed,
						objm.getPackets().get(mp.get(colnam)).getNumericValue(),cond,
						objm.getPackets().get(mp.get(colnam)).getValue(),
						objm.getPackets().get(mp.get(colnam)).getType()));
				else
				{
					long cnt = (Long)(aggVals.get(key).count);
					aggVals.get(key).count = cnt + objm.getPackets().get(mp.get(colnam)).getNumericValue();
				}
			}
			else
			{
				if(aggVals.get(key)==null)
					aggVals.put(key, new AggInfo<Double>(colnam,typed,
						objm.getPackets().get(mp.get(colnam)).getDoubleValue(),cond,
						objm.getPackets().get(mp.get(colnam)).getValue(),
						objm.getPackets().get(mp.get(colnam)).getType()));
				else
				{
					double cnt = (Double)(aggVals.get(key).count);
					aggVals.get(key).count = cnt + objm.getPackets().get(mp.get(colnam)).getDoubleValue();
				}
			}
			aggVals.get(key).incAvg();
			aggr = true;			
		}
		else if(cond.indexOf("max")!=-1)
		{
			String colnam = cond.substring(cond.toLowerCase().indexOf("max(")+4);
			colnam = colnam.replaceFirst("\\)", "");
			Object key = cond;//getKey1(objm, grpbycol, colnam,mp,"max");		
			if(mpt.get(colnam).equals("int") || mpt.get(colnam).equals("long"))
			{
				if(aggVals.get(key)==null)
					aggVals.put(key, new AggInfo<Long>(colnam,"max",
						objm.getPackets().get(mp.get(colnam)).getNumericValue(),cond,
						objm.getPackets().get(mp.get(colnam)).getValue(),
						objm.getPackets().get(mp.get(colnam)).getType()));
				else
				{
					long cnt = (Long)(aggVals.get(key).count);
					long curr = objm.getPackets().get(mp.get(colnam)).getNumericValue();
					if(curr>cnt)
						aggVals.get(key).count = curr;
				}
			}
			else if(mpt.get(colnam).equals("float")
					|| mpt.get(colnam).equals("double"))
			{
				if(aggVals.get(key)==null)
					aggVals.put(key, new AggInfo<Double>(colnam,"max",
						objm.getPackets().get(mp.get(colnam)).getDoubleValue(),cond,
						objm.getPackets().get(mp.get(colnam)).getValue(),
						objm.getPackets().get(mp.get(colnam)).getType()));
				else
				{
					double cnt = (Double)(aggVals.get(key).count);
					double curr = objm.getPackets().get(mp.get(colnam)).getDoubleValue();
					if(curr>cnt)
						aggVals.get(key).count = curr;
				}
			}
			else
			{
				if(aggVals.get(key)==null)
					aggVals.put(key, new AggInfo<String>(colnam,"max",
						objm.getPackets().get(mp.get(colnam)).getValueStr(),cond,
						objm.getPackets().get(mp.get(colnam)).getValue(),
						objm.getPackets().get(mp.get(colnam)).getType()));
				else
				{
					String cnt = (String)(aggVals.get(key).count);
					String curr = (String)objm.getPackets().get(mp.get(colnam)).getValueStr();
					if(cnt!=null && cnt.compareTo(curr)<0)
					{
						aggVals.get(key).count = curr;
					}
					else if(cnt==null)
					{
						aggVals.get(key).count = curr;
					}
				}
			}
			aggr = true;
		}
		else if(cond.indexOf("min")!=-1)
		{
			String colnam = cond.substring(cond.toLowerCase().indexOf("min(")+4);
			colnam = colnam.replaceFirst("\\)", "");
			Object key = cond;//getKey1(objm, grpbycol, colnam,mp,"min");		
			if(mpt.get(colnam).equals("int") || mpt.get(colnam).equals("long"))
			{
				if(aggVals.get(key)==null)
					aggVals.put(key, new AggInfo<Long>(colnam,"min",
						objm.getPackets().get(mp.get(colnam)).getNumericValue(),cond,
						objm.getPackets().get(mp.get(colnam)).getValue(),
						objm.getPackets().get(mp.get(colnam)).getType()));
				else
				{
					long cnt = (Long)(aggVals.get(key).count);
					long curr = objm.getPackets().get(mp.get(colnam)).getNumericValue();
					if(curr<cnt)
						aggVals.get(key).count = curr;
				}
			}
			else if(mpt.get(colnam).equals("float")
					|| mpt.get(colnam).equals("double"))
			{
				if(aggVals.get(key)==null)
					aggVals.put(key, new AggInfo<Double>(colnam,"min",
						objm.getPackets().get(mp.get(colnam)).getDoubleValue(),cond,
						objm.getPackets().get(mp.get(colnam)).getValue(),
						objm.getPackets().get(mp.get(colnam)).getType()));
				else
				{
					double cnt = (Double)(aggVals.get(key).count);
					double curr = objm.getPackets().get(mp.get(colnam)).getDoubleValue();
					if(curr<cnt)
						aggVals.get(key).count = curr;
				}
			}
			else
			{
				if(aggVals.get(key)==null)
					aggVals.put(key, new AggInfo<String>(colnam,"min",
						objm.getPackets().get(mp.get(colnam)).getValueStr(),cond,
						objm.getPackets().get(mp.get(colnam)).getValue(),
						objm.getPackets().get(mp.get(colnam)).getType()));
				else
				{
					String cnt = (String)(aggVals.get(key).count);
					String curr = (String)objm.getPackets().get(mp.get(colnam)).getValueStr();
					if(cnt!=null && cnt.compareTo(curr)>0)
					{
						aggVals.get(key).count = curr;
					}
					else if(cnt==null)
					{
						aggVals.get(key).count = curr;
					}
				}
			}
			aggr = true;
		}
		else if(cond.indexOf("first(")!=-1)
		{
			String colnam = cond.substring(cond.toLowerCase().indexOf("first(")+6);
			colnam = colnam.replaceFirst("\\)", "");
			Object key = cond;//getKey1(objm, grpbycol, colnam,mp,"first");
			if(aggVals.get(key)==null)
			{
				Object valu = getKey(objm,objm1,colnam,colnam,mp,mp2,"first",table,table1);
				aggVals.put(key, new AggInfo<Object>(colnam,"first",valu,null,null,
							objm.getPackets().get(mp.get(colnam)).getType()));
			}
			aggr = true;
		}
		else if(cond.indexOf("last(")!=-1)
		{
			String colnam = cond.substring(cond.toLowerCase().indexOf("last(")+5);
			colnam = colnam.replaceFirst("\\)", "");
			Object key = cond;//getKey1(objm, grpbycol, colnam,mp,"last");	
			if(aggVals.get(key)==null)
			{
				Object valu = getKey(objm,objm1,colnam,colnam,mp,mp2,"last",table,table1);	
				aggVals.put(key, new AggInfo<Object>(colnam,"last",valu,null,null,
						objm.getPackets().get(mp.get(colnam)).getType()));
			}
			else
			{
				Object valu = getKey(objm,objm1,colnam,colnam,mp,mp2,"last",table,table1);
				aggVals.get(key).count = valu;
			}
			aggr = true;
		}
		Object key = getKey(objm,objm1, grpbycol, "", mp,mp2, "",table,table1);
		if(key.equals("INCOMPLETE"))
			return true;
		if(aggr && rowVals.get(key)==null)
		{
			rowVals.put(key,new AggInfo());
		}
		return aggr;
	}
	
	
	
	private boolean createRowMT(String cond,JDBObject objm,JDBObject objm1,Map<Object, AggInfo> aggVals,
			Map<String, Integer> mp, Map<String, String> mpt,Map<String, Integer> mp2, Map<String, String> mpt2,
			String grpbycol, Map<Object, AggInfo> rowVals, String table, String table1)
	{
		boolean aggr = false;
		if(cond.indexOf("count(*")!=-1)
		{
			Object key = getKey(objm,objm1,grpbycol,"",mp,mp2,"",table, table1);			
			String colnam = table + "*";
			if(rowVals.get(key)==null)
			{	
				Object key1 = getKey1(objm, grpbycol, colnam,mp,"count");	
				if(aggVals.get(key1)==null)
					aggVals.put(key1, new AggInfo<Long>(colnam,"count",1L,cond,
							null,
							'l'));
				for(int t=0;t<objm1.getPackets().size();t++)
					objm.addPacket(objm1.getPackets().get(t).getValue(), objm1.getPackets().get(t).getType());
				rowVals.put(key, new AggInfo<JDBObject>(colnam,"count",objm,cond,
								null,
								'l'));
			}
			else
			{
				rowVals.get(key).incAvg();
				Object key1 = getKey1(objm, grpbycol, colnam,mp,"count");	
				if(aggVals.get(key1)==null)
					aggVals.put(key1, new AggInfo<JDBObject>(colnam,"count",objm,cond,
							null,
							'l'));
			}
			aggr = true;
		}
		else if(cond.indexOf("count("+table)!=-1)
		{
			String colnam = cond.substring(cond.toLowerCase().indexOf("count("+table)+7+table.length());
			colnam = colnam.replaceFirst("\\)", "");
			Object key = getKey(objm,objm1,grpbycol,"",mp,mp2,"",table, table1);			
			if(rowVals.get(key)==null)
			{	
				Object key1 = getKey1(objm, grpbycol, colnam,mp,"count");	
				if(aggVals.get(key1)==null)
					aggVals.put(key1, new AggInfo<Long>(colnam,"count",1L,cond,
							null,
							'l'));
				rowVals.put(key, new AggInfo<Long>(colnam,"count",1L,cond,
								objm.getPackets().get(mp.get(colnam)).getValue(),
								'l'));
			}
			else
			{
				rowVals.get(key).incAvg();
				Object key1 = getKey1(objm, grpbycol, colnam,mp,"count");	
				if(aggVals.get(key1)==null)
					aggVals.put(key1, new AggInfo<Long>(colnam,"count",1L,cond,
							null,
							'l'));
			}
			aggr = true;
		}
		else if(cond.indexOf("sum("+table)!=-1 || cond.indexOf("avg("+table)!=-1)
		{
			String colnam = null,typed = "sum";
			if(cond.indexOf("sum(")!=-1)
				colnam = cond.substring(cond.toLowerCase().indexOf("sum("+table)+5+table.length());
			else
			{
				colnam = cond.substring(cond.toLowerCase().indexOf("avg("+table)+5+table.length());
				typed = "avg";
			}
			colnam = colnam.replaceFirst("\\)", "");
			Object key = getKey1(objm, grpbycol, colnam,mp,typed);			
			if(mpt.get(colnam).equals("int") || mpt.get(colnam).equals("long"))
			{
				if(aggVals.get(key)==null)
					aggVals.put(key, new AggInfo<Long>(colnam,typed,
						objm.getPackets().get(mp.get(colnam)).getNumericValue(),cond,
						objm.getPackets().get(mp.get(colnam)).getValue(),
						objm.getPackets().get(mp.get(colnam)).getType()));
				else
				{
					long cnt = (Long)(aggVals.get(key).count);
					aggVals.get(key).count = cnt + objm.getPackets().get(mp.get(colnam)).getNumericValue();
				}
			}
			else
			{
				if(aggVals.get(key)==null)
					aggVals.put(key, new AggInfo<Double>(colnam,typed,
						objm.getPackets().get(mp.get(colnam)).getDoubleValue(),cond,
						objm.getPackets().get(mp.get(colnam)).getValue(),
						objm.getPackets().get(mp.get(colnam)).getType()));
				else
				{
					double cnt = (Double)(aggVals.get(key).count);
					aggVals.get(key).count = cnt + objm.getPackets().get(mp.get(colnam)).getDoubleValue();
				}
			}
			aggVals.get(key).incAvg();
			aggr = true;			
		}
		else if(cond.indexOf("max"+table)!=-1)
		{
			String colnam = cond.substring(cond.toLowerCase().indexOf("max("+table)+5+table.length());
			colnam = colnam.replaceFirst("\\)", "");
			Object key = getKey1(objm, grpbycol, colnam,mp,"max");			
			if(mpt.get(colnam).equals("int") || mpt.get(colnam).equals("long"))
			{
				if(aggVals.get(key)==null)
					aggVals.put(key, new AggInfo<Long>(colnam,"max",
						objm.getPackets().get(mp.get(colnam)).getNumericValue(),cond,
						objm.getPackets().get(mp.get(colnam)).getValue(),
						objm.getPackets().get(mp.get(colnam)).getType()));
				else
				{
					long cnt = (Long)(aggVals.get(key).count);
					long curr = objm.getPackets().get(mp.get(colnam)).getNumericValue();
					if(curr>cnt)
						aggVals.get(key).count = curr;
				}
			}
			else if(mpt.get(colnam).equals("float")
					|| mpt.get(colnam).equals("double"))
			{
				if(aggVals.get(key)==null)
					aggVals.put(key, new AggInfo<Double>(colnam,"max",
						objm.getPackets().get(mp.get(colnam)).getDoubleValue(),cond,
						objm.getPackets().get(mp.get(colnam)).getValue(),
						objm.getPackets().get(mp.get(colnam)).getType()));
				else
				{
					double cnt = (Double)(aggVals.get(key).count);
					double curr = objm.getPackets().get(mp.get(colnam)).getDoubleValue();
					if(curr>cnt)
						aggVals.get(key).count = curr;
				}
			}
			else
			{
				if(aggVals.get(key)==null)
					aggVals.put(key, new AggInfo<String>(colnam,"max",
						objm.getPackets().get(mp.get(colnam)).getValueStr(),cond,
						objm.getPackets().get(mp.get(colnam)).getValue(),
						objm.getPackets().get(mp.get(colnam)).getType()));
				else
				{
					String cnt = (String)(aggVals.get(key).count);
					String curr = (String)objm.getPackets().get(mp.get(colnam)).getValueStr();
					if(cnt!=null && cnt.compareTo(curr)<0)
					{
						aggVals.get(key).count = curr;
					}
					else if(cnt==null)
					{
						aggVals.get(key).count = curr;
					}
				}
			}
			aggr = true;
		}
		else if(cond.indexOf("min"+table)!=-1)
		{
			String colnam = cond.substring(cond.toLowerCase().indexOf("min("+table)+5+table.length());
			colnam = colnam.replaceFirst("\\)", "");
			Object key = getKey1(objm, grpbycol, colnam,mp,"min");			
			if(mpt.get(colnam).equals("int") || mpt.get(colnam).equals("long"))
			{
				if(aggVals.get(key)==null)
					aggVals.put(key, new AggInfo<Long>(colnam,"min",
						objm.getPackets().get(mp.get(colnam)).getNumericValue(),cond,
						objm.getPackets().get(mp.get(colnam)).getValue(),
						objm.getPackets().get(mp.get(colnam)).getType()));
				else
				{
					long cnt = (Long)(aggVals.get(key).count);
					long curr = objm.getPackets().get(mp.get(colnam)).getNumericValue();
					if(curr<cnt)
						aggVals.get(key).count = curr;
				}
			}
			else if(mpt.get(colnam).equals("float")
					|| mpt.get(colnam).equals("double"))
			{
				if(aggVals.get(key)==null)
					aggVals.put(key, new AggInfo<Double>(colnam,"min",
						objm.getPackets().get(mp.get(colnam)).getDoubleValue(),cond,
						objm.getPackets().get(mp.get(colnam)).getValue(),
						objm.getPackets().get(mp.get(colnam)).getType()));
				else
				{
					double cnt = (Double)(aggVals.get(key).count);
					double curr = objm.getPackets().get(mp.get(colnam)).getDoubleValue();
					if(curr<cnt)
						aggVals.get(key).count = curr;
				}
			}
			else
			{
				if(aggVals.get(key)==null)
					aggVals.put(key, new AggInfo<String>(colnam,"min",
						objm.getPackets().get(mp.get(colnam)).getValueStr(),cond,
						objm.getPackets().get(mp.get(colnam)).getValue(),
						objm.getPackets().get(mp.get(colnam)).getType()));
				else
				{
					String cnt = (String)(aggVals.get(key).count);
					String curr = (String)objm.getPackets().get(mp.get(colnam)).getValueStr();
					if(cnt!=null && cnt.compareTo(curr)>0)
					{
						aggVals.get(key).count = curr;
					}
					else if(cnt==null)
					{
						aggVals.get(key).count = curr;
					}
				}
			}
			aggr = true;
		}
		else if(cond.indexOf("first("+table)!=-1)
		{
			String colnam = cond.substring(cond.toLowerCase().indexOf("first("+table)+7+table.length());
			colnam = colnam.replaceFirst("\\)", "");
			Object key = getKey1(objm, grpbycol, colnam,mp,"first");	
			if(aggVals.get(key)==null)
			{
				Object valu = getKey(objm,objm1,colnam,colnam,mp,mp2,"first",table,table1);
				aggVals.put(key, new AggInfo<Object>(colnam,"first",valu,null,null,
							objm.getPackets().get(mp.get(colnam)).getType()));
			}
			aggr = true;
		}
		else if(cond.indexOf("last("+table)!=-1)
		{
			String colnam = cond.substring(cond.toLowerCase().indexOf("last("+table)+7+table.length());
			colnam = colnam.replaceFirst("\\)", "");
			Object key = getKey1(objm, grpbycol, colnam,mp,"last");	
			if(aggVals.get(key)==null)
			{
				Object valu = getKey(objm,objm1,colnam,colnam,mp,mp2,"last",table,table1);	
				aggVals.put(key, new AggInfo<Object>(colnam,"last",valu,null,null,
						objm.getPackets().get(mp.get(colnam)).getType()));
			}
			else
			{
				Object valu = getKey(objm,objm1,colnam,colnam,mp,mp2,"last",table,table1);
				aggVals.get(key).count = valu;
			}
			aggr = true;
		}
		if(aggr && rowVals.get(getKey(objm,objm1, grpbycol, "", mp,mp2, "",table,table1))==null)
		{
			rowVals.put(getKey(objm,objm1, grpbycol, "", mp,mp2, "",table,table1),new AggInfo());
		}
		/*else if(cond.equalsIgnoreCase("sysdate()"))
		{
			jdbo.addPacket(new Date());
		}
		else
			jdbo.addPacket(objm.getPackets().get(mp.get(cond)).getValue(),
					objm.getPackets().get(mp.get(cond)).getType());*/
		return aggr;
	}
	
	
	
	
	/*private void createRow(String cond,JDBObject objm,JDBObject jdbo,Map<byte[], AggInfo> aggVals,Map<String,Integer> mps)
	{
		if(cond.indexOf("count(")!=-1)
		{
			String colnam = cond.substring(cond.toLowerCase().indexOf("count(")+6);
			colnam = colnam.replaceFirst("\\)", "");
			if(aggVals.get(objm.getPackets().get(mps.get(colnam)).getValue())==null)
				aggVals.put(objm.getPackets().get(mps.get(colnam)).getValue(), new AggInfo<Long>(colnam,"count"));
			else
			{
				long cnt = (Long)(aggVals.get(objm.getPackets().get(mps.get(colnam)).getValue()).count);
				aggVals.get(objm.getPackets().get(mps.get(colnam)).getValue()).count = cnt + 1;
			}
		}
		else if(cond.indexOf("sum(")!=-1 || cond.indexOf("avg(")!=-1)
		{
			String colnam = null,typed = "sum";
			if(cond.indexOf("sum(")!=-1)
				colnam = cond.substring(cond.toLowerCase().indexOf("sum(")+4);
			else
			{
				colnam = cond.substring(cond.toLowerCase().indexOf("avg(")+4);
				typed = "avg";
			}
			colnam = colnam.replaceFirst("\\)", "");
			if(aggVals.get(objm.getPackets().get(mps.get(colnam)).getValue())==null
					&& (objm.getPackets().get(mps.get(colnam)).isNumber()
						|| objm.getPackets().get(mps.get(colnam)).isFloatingPoint()))
			{
				if(objm.getPackets().get(mps.get(colnam)).isNumber())
					aggVals.put(objm.getPackets().get(mps.get(colnam)).getValue(), new AggInfo<Long>(colnam,typed));
				else
					aggVals.put(objm.getPackets().get(mps.get(colnam)).getValue(), new AggInfo<Double>(colnam,typed));
			}
			else if(objm.getPackets().get(mps.get(colnam)).isNumber()
					|| objm.getPackets().get(mps.get(colnam)).isFloatingPoint())
			{
				if(objm.getPackets().get(mps.get(colnam)).isNumber())
				{
					long cnt = (Long)(aggVals.get(objm.getPackets().get(mps.get(colnam)).getValue()).count);
					aggVals.get(objm.getPackets().get(mps.get(colnam)).getValue()).count 
						= cnt + JdbResources.byteArrayToLong(objm.getPackets().get(mps.get(colnam)).getValue());
				}
				else
				{
					long cnt = (Long)(aggVals.get(objm.getPackets().get(mps.get(colnam)).getValue()).count);
					aggVals.get(objm.getPackets().get(mps.get(colnam)).getValue()).count 
						= cnt + Double.valueOf(objm.getPackets().get(mps.get(colnam)).getValueStr());
				}
			}
		}
		else if(cond.indexOf("max")!=-1)
		{
			String colnam = cond.substring(cond.toLowerCase().indexOf("max(")+4);
			colnam = colnam.replaceFirst("\\)", "");
			if(aggVals.get(objm.getPackets().get(mps.get(colnam)).getValue())==null
					&& (objm.getPackets().get(mps.get(colnam)).isNumber()
						|| objm.getPackets().get(mps.get(colnam)).isFloatingPoint()))
			{
				if(objm.getPackets().get(mps.get(colnam)).isNumber())
					aggVals.put(objm.getPackets().get(mps.get(colnam)).getValue(), new AggInfo<Long>(colnam,"max"));
				else
					aggVals.put(objm.getPackets().get(mps.get(colnam)).getValue(), new AggInfo<Double>(colnam,"max"));
			}
			else
			{
				if(objm.getPackets().get(mps.get(colnam)).isNumber())
				{
					long cnt = (Long)(aggVals.get(objm.getPackets().get(mps.get(colnam)).getValue()).count);
					long curr = JdbResources.byteArrayToLong(objm.getPackets().get(mps.get(colnam)).getValue());
					if(curr>cnt)
						aggVals.get(objm.getPackets().get(mps.get(colnam)).getValue()).count = curr;
				}
				else if(objm.getPackets().get(mps.get(colnam)).isFloatingPoint())
				{
					double cnt = (Long)(aggVals.get(objm.getPackets().get(mps.get(colnam)).getValue()).count);
					double curr = Double.valueOf(objm.getPackets().get(mps.get(colnam)).getValueStr());
					if(curr>cnt)
						aggVals.get(objm.getPackets().get(mps.get(colnam)).getValue()).count = curr;
				}
				else if(objm.getPackets().get(mps.get(colnam)).isString())
				{
					String cnt = (String)(aggVals.get(objm.getPackets().get(mps.get(colnam)).getValue()).count);
					String curr = objm.getPackets().get(mps.get(colnam)).getValueStr();
					if(cnt!=null && cnt.compareTo(curr)<0)
					{
						aggVals.get(objm.getPackets().get(mps.get(colnam)).getValue()).count = curr;
					}
					else if(cnt==null)
					{
						aggVals.get(objm.getPackets().get(mps.get(colnam)).getValue()).count = curr;
					}
				}
			}
		}
		else if(cond.indexOf("min")!=-1)
		{
			String colnam = cond.substring(cond.toLowerCase().indexOf("min(")+4);
			colnam = colnam.replaceFirst("\\)", "");
			if(aggVals.get(objm.getPackets().get(mps.get(colnam)).getValue())==null
					&& (objm.getPackets().get(mps.get(colnam)).isNumber()
						|| objm.getPackets().get(mps.get(colnam)).isFloatingPoint()))
			{
				if(objm.getPackets().get(mps.get(colnam)).isNumber())
					aggVals.put(objm.getPackets().get(mps.get(colnam)).getValue(), new AggInfo<Long>(colnam,"min"));
				else
					aggVals.put(objm.getPackets().get(mps.get(colnam)).getValue(), new AggInfo<Double>(colnam,"min"));
			}
			else
			{
				if(objm.getPackets().get(mps.get(colnam)).isNumber())
				{
					long cnt = (Long)(aggVals.get(objm.getPackets().get(mps.get(colnam)).getValue()).count);
					long curr = JdbResources.byteArrayToLong(objm.getPackets().get(mps.get(colnam)).getValue());
					if(curr<cnt)
						aggVals.get(objm.getPackets().get(mps.get(colnam)).getValue()).count = curr;
				}
				else if(objm.getPackets().get(mps.get(colnam)).isFloatingPoint())
				{
					double cnt = (Long)(aggVals.get(objm.getPackets().get(mps.get(colnam)).getValue()).count);
					double curr = Double.valueOf(objm.getPackets().get(mps.get(colnam)).getValueStr());
					if(curr<cnt)
						aggVals.get(objm.getPackets().get(mps.get(colnam)).getValue()).count = curr;
				}
				else if(objm.getPackets().get(mps.get(colnam)).isString())
				{
					String cnt = (String)(aggVals.get(objm.getPackets().get(mps.get(colnam)).getValue()).count);
					String curr = objm.getPackets().get(mps.get(colnam)).getValueStr();
					if(cnt!=null && cnt.compareTo(curr)>0)
					{
						aggVals.get(objm.getPackets().get(mps.get(colnam)).getValue()).count = curr;
					}
					else if(cnt==null)
					{
						aggVals.get(objm.getPackets().get(mps.get(colnam)).getValue()).count = curr;
					}
				}
			}
		}
		else if(cond.equalsIgnoreCase("sysdate()"))
		{
			jdbo.addPacket(new Date());
		}
		else
			jdbo.addPacket(objm.getPackets().get(mps.get(cond)).getValue(),
					objm.getPackets().get(mps.get(cond)).getType());
	}*/
	
	public void selectQuery(Queue<Object> q,String quer) throws Exception
	{
		long limit = -1;
		String ordbycol = "";
		String ordbytyp = "";
		if(quer.toLowerCase().indexOf("limit")!=-1)
		{
			String val = quer.trim().substring(quer.toLowerCase().indexOf("limit")+6);
			quer = quer.substring(0,quer.toLowerCase().indexOf("limit"));			
			try
			{
				limit = Long.parseLong(val);
			}
			catch (Exception e)
			{
				throw new Exception("");
			}
		}
		if(quer.toLowerCase().indexOf(" order by ")!=-1)
		{
			String val = quer.trim().substring(quer.toLowerCase().indexOf(" order by ")+10);
			String[] vals = val.split(" ");
			if(vals.length!=2)
				throw new Exception("Invalid order by clause");
			if(vals[1].toLowerCase().equals("desc") 
					|| vals[1].toLowerCase().equals("asc"))
			{
				ordbycol = vals[0];
				ordbytyp = vals[1].toLowerCase().trim();
			}
			else throw new Exception("Invalid order by clause");
			quer = quer.substring(0,quer.toLowerCase().indexOf(" order by "));	
		}
		boolean distinct = false;
		if(quer.indexOf("select distinct")!=-1)
		{
			quer = quer.replaceFirst("distinct ","");
			distinct = true;
			if(quer.indexOf("distinct")!=-1)
			{
				throw new Exception("");
			}
		}
		String grpbycol = "";//quer.indexOf(" group by ")!=-1?quer.substring(quer.indexOf(" group by ")+10):"";
		String subq = quer.indexOf(" where ")!=-1?quer.substring(quer.indexOf(" where ")+7):"";
		String vals = quer.substring(quer.indexOf("select ")+7,quer.indexOf(" from")).trim();
		String[] qparts = vals.split(",");	
		String tablen = null,havcls = null;
		if(quer.indexOf(" where ")!=-1)
			tablen = quer.substring(quer.indexOf("from ")+5,quer.indexOf(" where "));
		else if(quer.indexOf(" group by ")!=-1)
		{
			grpbycol = quer.substring(quer.indexOf(" group by ")+10);
			tablen = quer.substring(quer.indexOf("from ")+5,quer.indexOf(" group by "));
			if(grpbycol.indexOf(" having ")!=-1)
			{
				havcls = grpbycol.substring(grpbycol.indexOf(" having ")+8);
				grpbycol = grpbycol.substring(0,grpbycol.indexOf(" having "));
			}
		}
		else
			tablen = quer.substring(quer.indexOf("from ")+5);
		if(tablen.toUpperCase().indexOf(" INNER JOIN ")!=-1)// && !subq.equals(""))
		{
			handleJoins1(tablen.toLowerCase().split(" join "),qparts,subq,q,"inner",distinct,grpbycol,havcls,limit,ordbycol,ordbytyp);
		}
		else if(tablen.toUpperCase().indexOf(" LEFT JOIN ")!=-1)// && !subq.equals(""))
		{
			handleJoins1(tablen.toLowerCase().split(" join "),qparts,subq,q,"left",distinct,grpbycol,havcls,limit,ordbycol,ordbytyp);
		}
		else if(tablen.toUpperCase().indexOf(" RIGHT JOIN ")!=-1)// && !subq.equals(""))
		{
			handleJoins1(tablen.toLowerCase().split(" join "),qparts,subq,q,"right",distinct,grpbycol,havcls,limit,ordbycol,ordbytyp);
		}
		else if(tablen.toUpperCase().indexOf(" FULL JOIN ")!=-1)// && !subq.equals(""))
		{
			handleJoins1(tablen.toLowerCase().split(" join "),qparts,subq,q,"full",distinct,grpbycol,havcls,limit,ordbycol,ordbytyp);
		}
		else
		{
			handleJoins1(tablen.split(","),qparts,subq,q,"",distinct,grpbycol,havcls,limit,ordbycol,ordbytyp);
		}
	}
	
	@SuppressWarnings("unchecked")
	public void selectAMEFObjectsb(String dbname,String tableName,
			String[] qparts,Queue<Object> q, String subq, boolean distinct)
	{
		Table table = DBManager.getTable(dbname, tableName);
		JDBObject objtab = new JDBObject();
		JDBObject objtab1 = new JDBObject();
		
		for (int i = 0; i < table.getColumnNames().length; i++)
		{
			objtab.addPacket(table.getColumnTypes()[i],table.getColumnNames()[i]);
			if(qparts!=null && qparts[0].equals("*"))
			{
				objtab1.addPacket(i,table.getColumnNames()[i]);
			}
		}
		try
		{
			q.add(JdbResources.getEncoder().encodeB(objtab, false));
			if(qparts.length>0 && objtab1.getPackets().size()==0)
			{
				for (int i = 0; i < qparts.length; i++)
				{
					objtab1.addPacket(i,qparts[i]);
				}
			}
			q.add(JdbResources.getEncoder().encodeB(objtab1, false));
			if(table.getRecords()==0)
			{
				q.add("DONE");
				return;
			}
		}
		catch (AMEFEncodeException e)
		{
			e.printStackTrace();
		}
		long st1 = System.currentTimeMillis();
		workerThreadPool = Executors.newScheduledThreadPool(table.getAlgo());	
		for (int i = 0; i < table.getAlgo(); i++)
		{			
			JdbSearcher searcher = new JdbSearcher(subq, q, table, i, objtab, false, qparts, distinct, false, false, "");
			//FutureTask future = new FutureTask<Object>(searcher);
			//workerThreadPool.execute(future);
			//futures[i] = future;
			new Thread(searcher).start();
		}
		for (int i = 0; i < table.getAlgo(); i++)
		{
			try
			{
				//futures[i].;
				//int rec = (Integer)futures[i].get();
				//System.out.println("Number of records in file = "+rec);
			}
			catch (Exception e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		//workerThreadPool.shutdown();
		//System.out.println("Time reqd to complete srch thrd file routines = "+(System.currentTimeMillis()-st1));
	}
	
	@SuppressWarnings("unchecked")
	public void selectAMEFObjectsbq(String dbname,String tableName,
			String[] qparts,Queue<Object> q, String subq, boolean distinct)
	{
		Table table = DBManager.getTable(dbname, tableName);
		JDBObject objtab = new JDBObject();
		JDBObject objtab1 = new JDBObject();
		
		for (int i = 0; i < table.getColumnNames().length; i++)
		{
			objtab.addPacket(table.getColumnTypes()[i],table.getColumnNames()[i]);
			if(qparts!=null && qparts[0].equals("*"))
			{
				objtab1.addPacket(i,table.getColumnNames()[i]);
			}
		}
		try
		{
			q.add(JdbResources.getEncoder().encodeB(objtab, false));
			if(qparts.length>0 && objtab1.getPackets().size()==0)
			{
				for (int i = 0; i < qparts.length; i++)
				{
					objtab1.addPacket(i,qparts[i]);
				}
			}
			q.add(JdbResources.getEncoder().encodeB(objtab1, false));
			if(table.getRecords()==0)
			{
				q.add("DONE");
				return;
			}
		}
		catch (AMEFEncodeException e)
		{
			e.printStackTrace();
		}
		long st1 = System.currentTimeMillis();
		workerThreadPool = Executors.newScheduledThreadPool(table.getAlgo());	
		for (int i = 0; i < table.getAlgo(); i++)
		{			
			JdbSearcher searcher = new JdbSearcher(subq, q, table, i, objtab, false, qparts, distinct, false, false, "");
			//FutureTask future = new FutureTask<Object>(searcher);
			//workerThreadPool.execute(future);
			//futures[i] = future;
			//new Thread(searcher).start();
			searcher.run();
		}
		for (int i = 0; i < table.getAlgo(); i++)
		{
			try
			{
				//futures[i].;
				//int rec = (Integer)futures[i].get();
				//System.out.println("Number of records in file = "+rec);
			}
			catch (Exception e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		//workerThreadPool.shutdown();
		//System.out.println("Time reqd to complete srch thrd file routines = "+(System.currentTimeMillis()-st1));
	}
	
	@SuppressWarnings("unchecked")
	public List<JDBObject> selectAMEFObjectso(String dbname,Table table,
			String[] qparts,Queue<Object> q, String subq, JDBObject objtab,boolean one, boolean aggr, String grpbycol)
	{
		
		long st1 = System.currentTimeMillis();
		workerThreadPool = Executors.newScheduledThreadPool(table.getAlgo());	
		for (int i = 0; i < table.getAlgo(); i++)
		{			
			JdbSearcher searcher = new JdbSearcher(subq, q, table, i, objtab, true, qparts, false, one, aggr, grpbycol);
			//FutureTask future = new FutureTask<Object>(searcher);
			//workerThreadPool.execute(future);
			//futures[i] = future;
		}
		List<JDBObject> rec = null;
		for (int i = 0; i < table.getAlgo(); i++)
		{
			try
			{
				//rec = (List<JDBObject>)futures[i].get();
				//System.out.println("Number of records in file = "+rec);
			}
			catch (Exception e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		JdbSearcher searcher = new JdbSearcher(subq, q, table, 1, objtab, true, qparts, false, one, aggr, grpbycol);
		rec = (List<JDBObject>)searcher.call();
		workerThreadPool.shutdown();
		//System.out.println("Time reqd to complete srch thrd file routines = "+(System.currentTimeMillis()-st1));
		return rec;
	}
	
	
	public Set<JDBObject> selectAMEFObjectsDo(String dbname,Table table,
			String[] qparts,Queue<Object> q, String subq, JDBObject objtab,boolean one, boolean aggr, String grpbycol)
	{
		
		long st1 = System.currentTimeMillis();
		workerThreadPool = Executors.newScheduledThreadPool(table.getAlgo());	
		for (int i = 0; i < table.getAlgo(); i++)
		{			
			JdbSearcher searcher = new JdbSearcher(subq, q, table, i, objtab, true, qparts, true, one, aggr, grpbycol);
			//FutureTask future = new FutureTask<Object>(searcher);
			//workerThreadPool.execute(future);
			//futures[i] = future;
		}
		Set<JDBObject> rec = null;
		for (int i = 0; i < table.getAlgo(); i++)
		{
			try
			{
				//rec = (Set<JDBObject>)futures[i].get();
				//System.out.println("Number of records in file = "+rec);
			}
			catch (Exception e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		workerThreadPool.shutdown();
		//System.out.println("Time reqd to complete srch thrd file routines = "+(System.currentTimeMillis()-st1));
		return rec;
	}
	
	
	/*public <T> List<T> select(String dbname,String table,String[] cols,String where)
	{
		return selectColumnsWhere(dbname,table,cols,where);
	}*/
	Table table = null;
	public boolean insert(String dbname,String tableName,byte[] row)
	{			
		if(flusher!=null && flusher.belongsTo(tableName))
			flusher.write(row);
		else
		{
			table = DBManager.getTable(dbname, tableName);
			flusher.setStreamDetails(table);
			flusher.write(row);
		}
		return true;
	}
	
	public boolean insert(String dbname,String tableName,byte[] row,long id)
	{	
		if(flusher!=null && flusher.belongsTo(tableName))
			flusher.writepk(row,id);
		else
		{
			table = DBManager.getTable(dbname, tableName);
			flusher.setStreamDetails(table);
			flusher.writepk(row,id);
		}
		return true;
	}
	
	public boolean insert(String dbname,String tableName,byte[] row,int id)
	{	
		if(flusher!=null && flusher.belongsTo(tableName))
			flusher.write(row,id);
		else
		{
			table = DBManager.getTable(dbname, tableName);
			flusher.setStreamDetails(table);
			flusher.write(row,id);
		}
		return true;
	}
	public boolean insert(String dbname,String tableName,JDBObject row,int id) throws AMEFEncodeException
	{	
		if(flusher!=null && flusher.belongsTo(tableName))
			flusher.writepk(JdbResources.getEncoder().encodeWL(row, true),id);
		else
		{
			table = DBManager.getTable(dbname, tableName);
			flusher.setStreamDetails(table);
			flusher.writepk(JdbResources.getEncoder().encodeWL(row, true),id);
		}
		return true;
	}
	
	public void flush()
	{
		if(flusher!=null && !flusher.isOpenStream())
		{
			flusher.flushIt(0);
			flusher.flushIt(1);
		}
	}
	
	
	public boolean insert(String dbname,String tableName,JDBObject row)
	{
		try
		{
			insert(dbname, tableName, JdbResources.getEncoder().encodeWL(row,true));
		}
		catch (AMEFEncodeException e)
		{
			e.printStackTrace();
			return false;
		}
		return true;
	}
		
	public void bulkInsert(String dbname,String tableName,byte[] rws,int rows)
	{
		if(flusher!=null && flusher.belongsTo(tableName))
			flusher.write(rws,rows);
		else
		{
			table = DBManager.getTable(dbname, tableName);
			flusher.setStreamDetails(table);
			flusher.write(rws,rows);
		}
	}
	
	public void bulkInsert(String dbname,String tableName,List<JDBObject> rows)
	{
		if(flusher!=null && flusher.belongsTo(tableName))
		{}
		else
		{
			table = DBManager.getTable(dbname, tableName);
			flusher.setStreamDetails(table);
		}
		int num = rows.size()/table.getAlgo();
		int rem = rows.size()%table.getAlgo();		
		int idIndex = -1;
		if(table.getMapping().get("PK")!=null)
		{
			for (int i = 0; i < table.getColumnNames().length; i++)
			{
				if(table.getColumnNames()[i].equals(table.getMapping().get("PK")) 
						&& (table.getColumnTypes()[i].equals("int") || table.getColumnTypes()[i].equals("long")))
				{
					idIndex = i;
					break;
				}
			}
		}
		int last = num;
		for (int i = 0; i < table.getAlgo(); i++)
		{
			if(i==table.getAlgo()-1)
				last = num + rem;
			try
			{
				for (int j = 0; j < last; j++)
				{
					if(idIndex!=-1)
						flusher.writepk(JdbResources.getEncoder().encodeWL(rows.get(j+i*num),true)
								,JdbResources.byteArrayToLong(rows.get(j+i*num).getPackets().get(idIndex).getValue()));
					else
						flusher.write(JdbResources.getEncoder().encodeWL(rows.get(j+i*num),true));
				}
				int index = table.getFileIndexToInsert();
				flusher.flushIt(index);
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}
	}
	
	public void update(String dbname,String tablen,Map<String,byte[]> nvalues,String where) throws Exception
	{
		Table table = DBManager.getTable("temp", tablen);
		JDBObject objtab = new JDBObject();
		for (int i = 0; i < table.getColumnNames().length; i++)
		{
			objtab.addPacket(table.getColumnTypes()[i],table.getColumnNames()[i]);
		}
		flusher.clear();
		for (int i = 0; i < table.getAlgo(); i++)
		{
			table.updateObjects(where, objtab, nvalues, i);
		}		
	}
	
	public void delete(String dbname,String table,String where)
	{
		
	}
	
	public void truncate(String dbname,String table)
	{
		
	}	
	
	public void startTransaction()
	{
		
	}
	public void commit()
	{
		while(!flusher.isCommit())
		{
			try
			{
				Thread.sleep(200);
			}
			catch (InterruptedException e)
			{
				e.printStackTrace();
			}
		}
	}
	public void rollback()
	{
		
	}


	public void selectAMEFObjectsbc(String dbname, String tableName,
			String[] qparts, Queue<Object> q, String subq, boolean distinct,
			SocketChannel channel) 
	{
			Table table = DBManager.getTable(dbname, tableName);
	
			JDBObject objtab = new JDBObject();
			JDBObject objtab1 = new JDBObject();
			
			for (int i = 0; i < table.getColumnNames().length; i++)
			{
				objtab.addPacket(table.getColumnTypes()[i],table.getColumnNames()[i]);
				if(qparts!=null && qparts[0].equals("*"))
				{
					objtab1.addPacket(i,table.getColumnNames()[i]);
				}
			}
			try
			{
				//q.add(JdbResources.getEncoder().encodeB(objtab, false));
				byte[] encData = JdbResources.getEncoder().encodeB(objtab, false);
				ByteBuffer buf = ByteBuffer.allocate(encData.length);
				buf.put(encData);
				buf.flip();
				try {
					channel.write(buf);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}	
				buf.clear();
				if(qparts.length>0 && objtab1.getPackets().size()==0)
				{
					for (int i = 0; i < qparts.length; i++)
					{
						objtab1.addPacket(i,qparts[i]);
					}
				}
				//q.add(JdbResources.getEncoder().encodeB(objtab1, false));
				encData = JdbResources.getEncoder().encodeB(objtab, false);
				buf = ByteBuffer.allocate(encData.length);
				buf.put(encData);
				buf.flip();
				try {
					channel.write(buf);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}	
				buf.clear();
				if(table.getRecords()==0)
				{
					//q.add("DONE");
					encData = new byte[]{'F'};
					buf = ByteBuffer.allocate(1);
					buf.put(encData);
					buf.flip();
					try {
						channel.write(buf);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}	
					buf.clear();
					return;
				}
			}
			catch (AMEFEncodeException e)
			{
				e.printStackTrace();
			}
			long st1 = System.currentTimeMillis();
			/*workerThreadPool = Executors.newScheduledThreadPool(table.getAlgo());	
			for (int i = 0; i < table.getAlgo(); i++)
			{			
				JdbSearcher searcher = new JdbSearcher(subq, q, table, i, objtab, false, qparts, distinct, false, false, "");
				FutureTask future = new FutureTask<Object>(searcher);
				workerThreadPool.execute(future);
				futures[i] = future;
			}
			for (int i = 0; i < table.getAlgo(); i++)
			{
				try
				{
					int rec = (Integer)futures[i].get();
					//System.out.println("Number of records in file = "+rec);
				}
				catch (Exception e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}*/
			JdbSearcherc searcher = new JdbSearcherc(subq, q, table, 1, objtab, false, qparts, distinct, false, false, "",channel);
			searcher.call();
			
			//q.add("DONE");
			byte[] encData = {'F'};
			ByteBuffer buf = ByteBuffer.allocate(1);
			buf.put(encData);
			buf.flip();
			try {
				channel.write(buf);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			buf.clear();
			//workerThreadPool.shutdown();
			//System.out.println("Time reqd to complete srch thrd file routines = "+(System.currentTimeMillis()-st1));}
	}
}
