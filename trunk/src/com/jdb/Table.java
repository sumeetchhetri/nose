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

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import com.amef.AMEFDecodeException;
import com.amef.JDBObject;
import com.server.JdbFlusher;

@SuppressWarnings("unchecked")
public final class Table implements Serializable
{
	public long currpos = 0;
	public transient ReentrantReadWriteLock lock;
	class Algo implements Serializable
	{
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		int pos,num;
		Algo(int pos)
		{
			this.pos = pos; 
		}
	}
	class ComparNum implements Comparator<Algo>
	{
		public int compare(Algo o1, Algo o2)
		{
			if(o1.num<o2.num)
				return -1;
			else if(o1.num>o2.num)
				return 1;
			return 0;
		}		
	}
	class ComparPos implements Comparator<Algo>
	{
		public int compare(Algo o1, Algo o2)
		{
			if(o1.pos<o2.pos)
				return -1;
			else if(o1.pos>o2.pos)
				return 1;
			return 0;
		}		
	}
	
	private static final long serialVersionUID = 5419498745323901072L;
	//private transient JdbFlusher flusher;
	private Integer records,algo,n1=0,n2=0;
	//private List<Algo> recordspf;
	
	private Map<String,String> mapping = new HashMap<String, String>();
	protected Table(String dbName,String name,String[] colTypesNames,
			String[] misc, Map<String, String> mapping,int algo)
	{
		lock = new ReentrantReadWriteLock();
		if(algo<0)
			this.algo = 1;
		else if(algo>4)
			this.algo = 4;
		else
			this.algo = algo;
		columnNames = "";
		columnTypes = "";
		this.name = name;
		//this.dbName = dbName;
		this.partitions = new ArrayList<String>();
		this.currentFileIndex = 0;
		this.partitions.add("jdb_"+dbName+"_"+name);		
		this.writeLocked = false;
		records = 0;
		if(mapping!=null)
			this.mapping = mapping;
		for (String string : colTypesNames)
		{
			String[] coltn = string.split("\\|");
			columnNames += coltn[0]+",";
			columnTypes += coltn[1]+",";
		}
	}
	protected void modify(String[] colTypesNames,String[] misc)
	{
		
	}
	private String name;//,dbName;
	private int currentFileIndex;
	private boolean view;
	private volatile boolean writeLocked;
	private String columnNames;
	private String columnTypes;
	private ArrayList<String> partitions;
	
	public String[] getColumnNames()
	{
		return columnNames.split("\\,");
	}
	public String[] getColumnTypes()
	{
		return columnTypes.split("\\,");
	}
	public int getCurrentFileIndex()
	{
		return currentFileIndex;
	}
	public String getName()
	{
		return name;
	}
	public boolean isView()
	{
		return view;
	}
	public boolean isWriteLocked()
	{
		return writeLocked;
	}
	public void setWriteLocked(boolean writeLocked)
	{
		this.writeLocked = writeLocked;
	}	
	
	protected <T> void addObject(T row)
	{
		String fileName = partitions.get(currentFileIndex);
		try
		{
			ObjectInputStream jdbin = new ObjectInputStream(new FileInputStream(new File(fileName)));
			List<T> objects = (List<T>)jdbin.readObject();
			jdbin.close();
			objects.add(row);
			ObjectOutputStream jdbout = new ObjectOutputStream(new FileOutputStream(new File(fileName)));
			jdbout.writeObject(objects);
			jdbout.close();
		}
		catch (FileNotFoundException e)
		{
			List<T> objects = new ArrayList<T>();
			objects.add(row);
			try
			{
				ObjectOutputStream jdbout = new ObjectOutputStream(new FileOutputStream(new File(fileName)));
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
			List<T> objects = new ArrayList<T>();
			objects.add(row);
			try
			{
				ObjectOutputStream jdbout = new ObjectOutputStream(new FileOutputStream(new File(fileName)));
				jdbout.writeObject(objects);
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
	
	protected void storeObject(JDBObject object,JdbFlusher flusher)
	{		
		//flusher.addObject(object);
	}
	
	protected void storeObjects(List<JDBObject> objects,JdbFlusher flusher)
	{		
		
	}
	
	public String getFileName(int index)
	{
		if(algo==1)
			index = 0;
		String fileName = partitions.get(currentFileIndex)+index+".dat";
		return fileName;
	}
	
	public String getIndexFileName()
	{
		String fileName = partitions.get(currentFileIndex);
		fileName = fileName + ".ind";
		return fileName;
	}
	
	public String getLastIndex()
	{
		String fileName = partitions.get(currentFileIndex);
		fileName = fileName.substring(0,fileName.indexOf(".")) + ".ind";
		return fileName;
	}
	
	protected <T> List<T> getObjects()
	{
		String fileName = partitions.get(currentFileIndex);
		List<T> objects = null;
		try
		{
			ObjectInputStream jdbin = new ObjectInputStream(new FileInputStream(new File(fileName)));
			objects = (List<T>)jdbin.readObject();
			jdbin.close();
		}
		catch (FileNotFoundException e)
		{	
			objects = new ArrayList<T>();
			try
			{
				ObjectOutputStream jdbout = new ObjectOutputStream(new FileOutputStream(new File(fileName)));
				jdbout.writeObject(objects);
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (ClassNotFoundException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return objects;
	}
	
	protected List<JDBObject> getAMEFObjects()
	{
		List<JDBObject> objects = new ArrayList<JDBObject>(1000000);
		try
		{	
			if(!new File(getIndexFileName()).exists())
				new File(getIndexFileName()).createNewFile();
			BufferedReader reader = new BufferedReader(new FileReader(getIndexFileName()));
			String temp = reader.readLine();
			String[] temp1 = null;
			if(temp!=null)
				temp1 = temp.split(",");
			reader.close();
			String fileName = partitions.get(currentFileIndex);
			
			List<FutureTask<List<JDBObject>>> futures = new ArrayList<FutureTask<List<JDBObject>>>();
			if(temp1!=null)
			{
				ExecutorService executor = Executors.newFixedThreadPool(temp1.length); 
				JdbReaderTask task = new JdbReaderTask(fileName,0);	
				FutureTask<List<JDBObject>> future = new FutureTask<List<JDBObject>>(task);
				executor.execute(future);
				futures.add(future);
				for (String temp2 : temp1)
				{
					task = new JdbReaderTask(fileName,Integer.parseInt(temp2));	
					future = new FutureTask<List<JDBObject>>(task);
					executor.execute(future);
					futures.add(future);
				}			
				for (FutureTask<List<JDBObject>> future1 : futures)
				{
					try
					{
						objects.addAll(future1.get());
					}
					catch (InterruptedException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					catch (ExecutionException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				executor.shutdown();
			}
			else
			{
				ExecutorService executor = Executors.newFixedThreadPool(1); 
				JdbReaderTask task = new JdbReaderTask(fileName,0);	
				FutureTask<List<JDBObject>> future = new FutureTask<List<JDBObject>>(task);
				executor.execute(future);
				futures.add(future);							
				try
				{
					objects.addAll(future.get());
					executor.shutdown();
				}
				catch (InterruptedException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				catch (ExecutionException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		catch (FileNotFoundException e1)
		{
			e1.printStackTrace();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		
		
		
		/*InputStream jdbin = null;
		try
		{
			jdbin = new FileInputStream(new File(fileName));
			AMEFDecoder decoder = new AMEFDecoder();
			byte[] length = new byte[4];
			while(jdbin.available()>4)
			{				
				jdbin.read(length);
				int lengthm = ((length[0] & 0xff) << 24) | ((length[1] & 0xff) << 16)
								| ((length[2] & 0xff) << 8) | ((length[3] & 0xff));
				byte[] data = new byte[lengthm];
				jdbin.read(data);
				JDBObject object = decoder.decode(data, false, false);
				objects.add(object);
			}
		}
		catch (IOException e)
		{
			
		}
		catch (AMEFDecodeException e)
		{
			// TODO Auto-generated catch block
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
		}*/
		return objects;
	}
	
	private static boolean evaluate(JDBObject objec,
			Map<String,Opval> whrandcls,Map<String,Opval> whrorcls,
			Map<String,Integer> whrclsInd,Map<Integer,String> whrclsIndv,
			Map<Integer,String> whrclsIndt)
	{
		if(whrandcls.size()==0)
			return true;
		boolean flag = true;
				
		for (String name : whrandcls.keySet())
		{
			int num = whrclsInd.get(name);
			//Integer valn = whrclsInd.get(whrclsIndv.get(num));
			String right = whrclsIndv.get(num);
			//if(valn!=null)
			//	right = objec.getPackets().get(valn).getValue();
			flag &= eval(objec.getPackets().get(num).getValue(),right,whrclsIndt.get(num),whrandcls.get(name).operator);
		}
		for (String name : whrorcls.keySet())
		{
			int num = whrclsInd.get(name);
			//Integer valn = whrclsInd.get(whrclsIndv.get(num));
			String right = whrclsIndv.get(num);
			//if(valn!=null)
			//	right = objec.getPackets().get(valn).getValue();
			flag |= eval(objec.getPackets().get(num).getValue(),right,whrclsIndt.get(num),whrandcls.get(name).operator);
		}
		return flag;
	}
	private static boolean eval(byte[] left,String right,String type, String operator)
	{
		if(type.equalsIgnoreCase("string"))
		{
			if(right.charAt(0)=='\'' || right.charAt(0)=='"')
				right = right.substring(1,right.length()-1);
			if(operator.equals("=") || operator.equals("=="))
				return new String(left).equals(right);
			else if(operator.equals("!=") || operator.equals("<>"))
				return !new String(left).equals(right);
			else if(operator.equals(">"))
				return new String(left).compareTo(right)<0;
			else if(operator.equals("<"))
				return new String(left).compareTo(right)>0;
			else if(operator.equals(">="))
				return new String(left).compareTo(right)<0 || new String(left).equals(right);
			else if(operator.equals("<="))
				return new String(left).compareTo(right)>0 || new String(left).equals(right);
			else if(operator.equals(" like "))
				return new String(left).indexOf(right)!=-1;
		}
		else if(type.equalsIgnoreCase("int"))
		{
			if(operator.equals("=") || operator.equals("=="))
				return JdbResources.byteArrayToInt(left) == Integer.parseInt(right);
			else if(operator.equals("!=") || operator.equals("<>"))
				return JdbResources.byteArrayToInt(left) != Integer.parseInt(right);
			else if(operator.equals(">"))
				return JdbResources.byteArrayToInt(left) > Integer.parseInt(right);
			else if(operator.equals("<"))
				return JdbResources.byteArrayToInt(left) < Integer.parseInt(right);
			else if(operator.equals(">="))
				return JdbResources.byteArrayToInt(left) >= Integer.parseInt(right);
			else if(operator.equals("<="))
				return JdbResources.byteArrayToInt(left) <= Integer.parseInt(right);
			else if(operator.equals(" like "))
				return String.valueOf(JdbResources.byteArrayToInt(left)).indexOf(right)!=-1;
		}
		else if(type.equalsIgnoreCase("long"))
		{
			if(operator.equals("=") || operator.equals("=="))
				return JdbResources.byteArrayToLong(left) == Long.parseLong(right);
			else if(operator.equals("!=") || operator.equals("<>"))
				return JdbResources.byteArrayToLong(left) != Long.parseLong(right);
			else if(operator.equals(">"))
				return JdbResources.byteArrayToLong(left) > Long.parseLong(right);
			else if(operator.equals("<"))
				return JdbResources.byteArrayToLong(left) < Long.parseLong(right);
			else if(operator.equals(">="))
				return JdbResources.byteArrayToLong(left) >= Long.parseLong(right);
			else if(operator.equals("<="))
				return JdbResources.byteArrayToLong(left) <= Long.parseLong(right);
			else if(operator.equals(" like "))
				return String.valueOf(JdbResources.byteArrayToInt(left)).indexOf(right)!=-1;
		}
		else if(type.equalsIgnoreCase("double") || type.equalsIgnoreCase("float"))
		{
			if(operator.equals("=") || operator.equals("=="))
				return new String(left).equals(right);
			else if(operator.equals("!=") || operator.equals("<>"))
				return !new String(left).equals(right);
			else if(operator.equals(">"))
				return new String(left).compareTo(right)<0;
			else if(operator.equals("<"))
				return new String(left).compareTo(right)>0;
			else if(operator.equals(">="))
				return new String(left).compareTo(right)<0 || new String(left).equals(right);
			else if(operator.equals("<="))
				return new String(left).compareTo(right)>0 || new String(left).equals(right);
			else if(operator.equals(" like "))
				return new String(left).indexOf(right)!=-1;
		}
		else if(type.equalsIgnoreCase("date"))
		{
			if(operator.equals("=") || operator.equals("=="))
				return new String(left).equals(right);
			else if(operator.equals("!=") || operator.equals("<>"))
				return !new String(left).equals(right);
			else if(operator.equals(">"))
				return new String(left).compareTo(right)<0;
			else if(operator.equals("<"))
				return new String(left).compareTo(right)>0;
			else if(operator.equals(">="))
				return new String(left).compareTo(right)<0 || new String(left).equals(right);
			else if(operator.equals("<="))
				return new String(left).compareTo(right)>0 || new String(left).equals(right);
			else if(operator.equals(" like "))
				return new String(left).indexOf(right)!=-1;
		}
		else if(type.equalsIgnoreCase("boolean"))
		{
			if(operator.equals("=") || operator.equals("=="))
				return new Boolean(new String(left)).booleanValue() == new Boolean(right).booleanValue();
			else if(operator.equals("!=") || operator.equals("<>"))
				return new Boolean(new String(left)).booleanValue() != new Boolean(right).booleanValue();
			else if(operator.equals(" like "))
				return new String(left).indexOf(right)!=-1;
		}
		else if(type.equalsIgnoreCase("char"))
		{
			if(right.charAt(0)=='\'' || right.charAt(0)=='"')
				right = right.substring(1,right.length()-1);
			if(operator.equals("=") || operator.equals("=="))
				return new String(left).charAt(0) == right.charAt(0);
			else if(operator.equals("!=") || operator.equals("<>"))
				return new String(left).charAt(0) != right.charAt(0);
			else if(operator.equals(">"))
				return new String(left).charAt(0) > right.charAt(0);
			else if(operator.equals("<"))
				return new String(left).charAt(0) < right.charAt(0);
			else if(operator.equals(">="))
				return new String(left).charAt(0) >= right.charAt(0);
			else if(operator.equals("<="))
				return new String(left).charAt(0) <= right.charAt(0);
			else if(operator.equals("  like  "))
				return new String(left).indexOf(right)!=-1;
		}
		return false;
	}
	
	protected void index()
	{
		//int buckets = records/100;
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
			if(colnam.indexOf(".")!=-1)
				colnam = colnam.split("\\.")[1];
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
		return "";
	}
	
	class Opval
	{
		public String value,operator;
		public Opval(String value, String operator)
		{
			super();
			this.value = value;
			this.operator = operator;
		}
		
	}
	
	private void getOperator(String ter,Map<String,Opval> whrandcls) throws Exception
	{
		if(ter.indexOf("!=")!=-1)
			whrandcls.put(ter.split("!=")[0],new Opval(ter.split("!=")[1],"!="));
		else if(ter.indexOf(">=")!=-1)
			whrandcls.put(ter.split(">=")[0],new Opval(ter.split(">=")[1],">="));
		else if(ter.indexOf("<=")!=-1)
			whrandcls.put(ter.split("<=")[0],new Opval(ter.split("<=")[1],"<="));
		else if(ter.indexOf("==")!=-1)
			whrandcls.put(ter.split("==")[0],new Opval(ter.split("==")[1],"=="));
		else if(ter.indexOf("=")!=-1)
			whrandcls.put(ter.split("=")[0],new Opval(ter.split("=")[1],"="));
		else if(ter.indexOf("<>")!=-1)
			whrandcls.put(ter.split("<>")[0],new Opval(ter.split("<>")[1],"<>"));
		else if(ter.indexOf(">")!=-1)
			whrandcls.put(ter.split(">")[0],new Opval(ter.split(">")[1],">"));
		else if(ter.indexOf("<")!=-1)
			whrandcls.put(ter.split("<")[0],new Opval(ter.split("<")[1],"<"));
		else if(ter.indexOf("<")!=-1)
			whrandcls.put(ter.split("<")[0],new Opval(ter.split("<")[1],"<"));
		else if(ter.toLowerCase().indexOf(" like ")!=-1)
			whrandcls.put(ter.split(" like ")[0],new Opval(ter.split(" like ")[1]," like "));
		else if(ter.toLowerCase().indexOf(" in ")!=-1)
			whrandcls.put(ter.split(" in ")[0],new Opval(ter.split(" in ")[1]," in "));
		else throw new Exception("Invalid operator");
	}
	
	protected int getAMEFObjectsb(Queue<Object> q,InputStream jdbin, String subq, JDBObject objtab, String[] qparts) throws Exception
	{
		int rec = 0;
		Map<String,Opval> whrandcls = new HashMap<String, Opval>();
		Map<String,Opval> whrorcls = new HashMap<String, Opval>();;
		Map<String,Integer> whrclsInd = new HashMap<String, Integer>();
		Map<String,Integer> valInd = new HashMap<String, Integer>();
		Map<Integer,String> whrclsIndv = new HashMap<Integer, String>();
		Map<Integer,String> whrclsIndt = new HashMap<Integer, String>();
		if(subq!=null && !subq.equals(""))
		{
			while(subq.indexOf(" and ")!=-1 || subq.indexOf(" or ")!=-1)
			{
				int anop = subq.indexOf(" and ");
				int orop = subq.indexOf(" or ");
				if((anop<orop && anop!=-1) || (anop!=-1 && orop==-1))
				{
					String ter = subq.split(" and ")[0].trim();					
					getOperator(ter,whrandcls);
					subq = subq.substring(anop+5);
				}
				else if(orop!=-1)
				{
					String ter = subq.split(" or ")[0].trim();
					getOperator(ter,whrorcls);
					subq = subq.substring(orop+4);
				}
			}		
			getOperator(subq,whrandcls);
		}
		for (int i = 0; i < objtab.getPackets().size(); i++)
		{
			if(whrandcls.get(objtab.getPackets().get(i).getNameStr())!=null)
			{
				whrclsInd.put(objtab.getPackets().get(i).getNameStr(), i);
				whrclsIndv.put(i, whrandcls.get(objtab.getPackets().get(i).getNameStr()).value);
				whrclsIndt.put(i, new String(objtab.getPackets().get(i).getValue()));
			}
			else if(whrorcls.get(objtab.getPackets().get(i).getNameStr())!=null)
			{
				whrclsInd.put(objtab.getPackets().get(i).getNameStr(), i);
				whrclsIndv.put(i, whrorcls.get(objtab.getPackets().get(i).getNameStr()).value);
				whrclsIndt.put(i, new String(objtab.getPackets().get(i).getValue()));
			}
			valInd.put(objtab.getPackets().get(i).getNameStr(),i);
		}
		boolean pksrch = false;
		if(whrandcls.get(mapping.get("PK"))!=null)
		{
			pksrch = true;
		}
		try
		{
			byte[] type = new byte[5];	
			if(!pksrch)
			{	
				int cuurcnt = 0;
				if(jdbin.available()>4)
				{
					boolean done = false,last = false;
					JDBObject lastobh = new JDBObject();
					while(jdbin.available()>4 && !done)
					{	
						rec++;
						jdbin.read(type);
						int lengthm = 0;
						byte[] le = null;
						if(type[0]==(byte)JDBObject.VS_OBJECT_TYPE)
						{
							//le = new byte[1];
							//jdbin.read(le);
							lengthm = JdbResources.byteArrayToInt(type,1,1)-3;
						}
						else if(type[0]==JDBObject.S_OBJECT_TYPE)
						{
							//le = new byte[2];
							//jdbin.read(le);
							lengthm = JdbResources.byteArrayToInt(type,1,2)-2;
						}
						else if(type[0]==JDBObject.B_OBJECT_TYPE)
						{
							//le = new byte[3];
							//jdbin.read(le);
							lengthm = JdbResources.byteArrayToInt(type,1,3)-1;
						}
						else
						{
							//le = new byte[4];
							//jdbin.read(le);
							lengthm = JdbResources.byteArrayToInt(type,1,4);
						}
						//ByteBuffer buf  = ByteBuffer.allocate(5+lengthm);
						byte[] data = new byte[5+lengthm];
						System.arraycopy(type, 0, data, 0, 5);
						jdbin.read(data,5,lengthm);
						//buf.put(type);
						//buf.put(le);
						//buf.put(data);
						//buf.flip();
						//buf.clear();
						JDBObject obh = JdbResources.getDecoder().decodeB(data, false,true);
						if(whrandcls.size()!=0 || whrorcls.size()!=0)
						{	
							//obh = JdbResources.getDecoder().decodeB(data, false,true);
							if(!evaluate(obh, whrandcls, whrorcls, whrclsInd, whrclsIndv, whrclsIndt))
							{
								//q.add(buf.array());
								obh = null;
							}
						}
						//else
						//	q.add(buf.array());
						if(obh==null)continue;
						cuurcnt++;
						if(qparts==null || qparts.length==0 || qparts[0].equals("*"))
						{
							//q.add(buf.array());
							q.add(data);
						}
						else
						{
							JDBObject obh1 = new JDBObject();
							for (int i = 0; i < qparts.length; i++)
							{									
								createRow(qparts[i], obh1, obh, valInd, true);
							}
							if(!last)
							{
								q.add(JdbResources.getEncoder().encodeWL(obh1,true));
							}
						}
						if(cuurcnt==100000)
						{
							Thread.sleep(10);
							cuurcnt = 0;
						}
					}
					if(last && lastobh!=null)
					{
						q.add(JdbResources.getEncoder().encodeWL(lastobh,true));						
					}
					return rec;
				}
			}
			else
			{
				long id = Long.parseLong(whrandcls.get(mapping.get("PK")).value);
				long tr = getIndex(id);
				if(tr==-1)
					return 0;
				jdbin.skip(tr);
				jdbin.read(type);
				int lengthm = 0;
				byte[] le = null;
				if(type[0]==(byte)JDBObject.VS_OBJECT_TYPE)
				{
					le = new byte[1];
					jdbin.read(le);
					lengthm = JdbResources.byteArrayToInt(le);
				}
				else if(type[0]==JDBObject.S_OBJECT_TYPE)
				{
					le = new byte[2];
					jdbin.read(le);
					lengthm = JdbResources.byteArrayToInt(le);
				}
				else if(type[0]==JDBObject.B_OBJECT_TYPE)
				{
					le = new byte[3];
					jdbin.read(le);
					lengthm = JdbResources.byteArrayToInt(le);
				}
				else
				{
					le = new byte[4];
					jdbin.read(le);
					lengthm = JdbResources.byteArrayToInt(le);
				}
				ByteBuffer buf  = ByteBuffer.allocate(1+le.length+lengthm);
				byte[] data = new byte[lengthm];
				jdbin.read(data);
				buf.put(type);
				buf.put(le);
				buf.put(data);
				buf.flip();
				buf.clear();
				JDBObject obh = JdbResources.getDecoder().decodeB(buf.array(), false,true);
				if(whrandcls.size()!=0 || whrorcls.size()!=0)
				{						
					if(!evaluate(obh, whrandcls, whrorcls, whrclsInd, whrclsIndv, whrclsIndt))
					{
						obh = null;
					}
				}
				if(qparts==null || qparts.length==0 || qparts[0].equals("*"))
				{
					q.add(buf.array());	
				}
				else
				{
					JDBObject obh1 = new JDBObject();
					for (int i = 0; i < qparts.length; i++)
					{	
						createRow(qparts[i], obh1, obh, valInd, true);
					}
					q.add(JdbResources.getEncoder().encodeWL(obh1,true));
				}
			}
			return rec;
		}
		catch (IOException e)
		{
			e.printStackTrace();
			return rec;
		}
		catch (AMEFDecodeException e)
		{
			e.printStackTrace();
			return rec;
		}
	}
	
	
	protected int getAMEFObjectsbc(Queue<Object> q,InputStream jdbin, String subq, JDBObject objtab, String[] qparts,SocketChannel channel) throws Exception
	{
		int rec = 0;
		Map<String,Opval> whrandcls = new HashMap<String, Opval>();
		Map<String,Opval> whrorcls = new HashMap<String, Opval>();;
		Map<String,Integer> whrclsInd = new HashMap<String, Integer>();
		Map<String,Integer> valInd = new HashMap<String, Integer>();
		Map<Integer,String> whrclsIndv = new HashMap<Integer, String>();
		Map<Integer,String> whrclsIndt = new HashMap<Integer, String>();
		if(subq!=null && !subq.equals(""))
		{
			while(subq.indexOf(" and ")!=-1 || subq.indexOf(" or ")!=-1)
			{
				int anop = subq.indexOf(" and ");
				int orop = subq.indexOf(" or ");
				if((anop<orop && anop!=-1) || (anop!=-1 && orop==-1))
				{
					String ter = subq.split(" and ")[0].trim();					
					getOperator(ter,whrandcls);
					subq = subq.substring(anop+5);
				}
				else if(orop!=-1)
				{
					String ter = subq.split(" or ")[0].trim();
					getOperator(ter,whrorcls);
					subq = subq.substring(orop+4);
				}
			}		
			getOperator(subq,whrandcls);
		}
		for (int i = 0; i < objtab.getPackets().size(); i++)
		{
			if(whrandcls.get(objtab.getPackets().get(i).getNameStr())!=null)
			{
				whrclsInd.put(objtab.getPackets().get(i).getNameStr(), i);
				whrclsIndv.put(i, whrandcls.get(objtab.getPackets().get(i).getNameStr()).value);
				whrclsIndt.put(i, new String(objtab.getPackets().get(i).getValue()));
			}
			else if(whrorcls.get(objtab.getPackets().get(i).getNameStr())!=null)
			{
				whrclsInd.put(objtab.getPackets().get(i).getNameStr(), i);
				whrclsIndv.put(i, whrorcls.get(objtab.getPackets().get(i).getNameStr()).value);
				whrclsIndt.put(i, new String(objtab.getPackets().get(i).getValue()));
			}
			valInd.put(objtab.getPackets().get(i).getNameStr(),i);
		}
		boolean pksrch = false;
		if(whrandcls.get(mapping.get("PK"))!=null)
		{
			pksrch = true;
		}
		try
		{
			byte[] type = new byte[1];	
			if(!pksrch)
			{	
				ByteBuffer b = ByteBuffer.allocate(64*1024);
				if(jdbin.available()>4)
				{
					boolean done = false,last = false;
					JDBObject lastobh = new JDBObject();
					while(jdbin.available()>4 && !done)
					{	
						rec++;
						jdbin.read(type);
						int lengthm = 0;
						byte[] le = null;
						if(type[0]==(byte)JDBObject.VS_OBJECT_TYPE)
						{
							le = new byte[1];
							jdbin.read(le);
							lengthm = JdbResources.byteArrayToInt(le);
						}
						else if(type[0]==JDBObject.S_OBJECT_TYPE)
						{
							le = new byte[2];
							jdbin.read(le);
							lengthm = JdbResources.byteArrayToInt(le);
						}
						else if(type[0]==JDBObject.B_OBJECT_TYPE)
						{
							le = new byte[3];
							jdbin.read(le);
							lengthm = JdbResources.byteArrayToInt(le);
						}
						else
						{
							le = new byte[4];
							jdbin.read(le);
							lengthm = JdbResources.byteArrayToInt(le);
						}
						Thread.sleep(0, 1);
						ByteBuffer buf  = ByteBuffer.allocate(1+le.length+lengthm);
						byte[] data = new byte[lengthm];
						jdbin.read(data);
						buf.put(type);
						buf.put(le);
						buf.put(data);
						buf.flip();
						buf.clear();
						JDBObject obh = JdbResources.getDecoder().decodeB(buf.array(), false,true);
						if(whrandcls.size()!=0 || whrorcls.size()!=0)
						{						
							if(!evaluate(obh, whrandcls, whrorcls, whrclsInd, whrclsIndv, whrclsIndt))
							{
								//q.add(buf.array());
								obh = null;
							}
						}
						//else
						//	q.add(buf.array());
						if(obh==null)continue;
						if(qparts==null || qparts.length==0 || qparts[0].equals("*"))
						{
							//q.add(buf.array());
							byte[] encData = buf.array();
							if(b.limit()+encData.length<b.capacity())
								b.put(encData);
							else
							{
								b.flip();
								ByteBuffer b1 = ByteBuffer.allocate(b.limit()+encData.length);
								b1.put(b.array(),0,b.limit());
								
								b1.put(encData);
								b1.flip();
								
								channel.write(b1);
								b.clear();
							}
						}
						else
						{
							JDBObject obh1 = new JDBObject();
							for (int i = 0; i < qparts.length; i++)
							{									
								createRow(qparts[i], obh1, obh, valInd, true);
							}
							if(!last)
							{
								//q.add(JdbResources.getEncoder().encodeWL(obh1,true));
								byte[] encData = JdbResources.getEncoder().encodeWL(obh1,true);
								if(b.limit()+encData.length<b.capacity())
									b.put(encData);
								else
								{
									b.flip();
									ByteBuffer b1 = ByteBuffer.allocate(b.limit()+encData.length);
									b1.put(b.array(),0,b.limit());
									
									b1.put(encData);
									b1.flip();
									
									channel.write(b1);
									b.clear();
								}
							}
						}
					}
					if(last && lastobh!=null)
					{
						//q.add(JdbResources.getEncoder().encodeWL(lastobh,true));
						byte[] encData = JdbResources.getEncoder().encodeWL(lastobh,true);
						if(b.limit()+encData.length<b.capacity())
							b.put(encData);
						else
						{
							b.flip();
							ByteBuffer b1 = ByteBuffer.allocate(b.limit()+encData.length);
							b1.put(b.array(),0,b.limit());
							
							b1.put(encData);
							b1.flip();
							
							channel.write(b1);
							b.clear();
						}
					}
					return rec;
				}
			}
			else
			{
				long id = Long.parseLong(whrandcls.get(mapping.get("PK")).value);
				long tr = getIndex(id);
				if(tr==-1)
					return 0;
				jdbin.skip(tr);
				jdbin.read(type);
				int lengthm = 0;
				byte[] le = null;
				if(type[0]==(byte)JDBObject.VS_OBJECT_TYPE)
				{
					le = new byte[1];
					jdbin.read(le);
					lengthm = JdbResources.byteArrayToInt(le);
				}
				else if(type[0]==JDBObject.S_OBJECT_TYPE)
				{
					le = new byte[2];
					jdbin.read(le);
					lengthm = JdbResources.byteArrayToInt(le);
				}
				else if(type[0]==JDBObject.B_OBJECT_TYPE)
				{
					le = new byte[3];
					jdbin.read(le);
					lengthm = JdbResources.byteArrayToInt(le);
				}
				else
				{
					le = new byte[4];
					jdbin.read(le);
					lengthm = JdbResources.byteArrayToInt(le);
				}
				ByteBuffer buf  = ByteBuffer.allocate(1+le.length+lengthm);
				byte[] data = new byte[lengthm];
				jdbin.read(data);
				buf.put(type);
				buf.put(le);
				buf.put(data);
				buf.flip();
				buf.clear();
				JDBObject obh = JdbResources.getDecoder().decodeB(buf.array(), false,true);
				if(whrandcls.size()!=0 || whrorcls.size()!=0)
				{						
					if(!evaluate(obh, whrandcls, whrorcls, whrclsInd, whrclsIndv, whrclsIndt))
					{
						obh = null;
					}
				}
				if(qparts==null || qparts.length==0 || qparts[0].equals("*"))
				{
					q.add(buf.array());	
				}
				else
				{
					JDBObject obh1 = new JDBObject();
					for (int i = 0; i < qparts.length; i++)
					{	
						createRow(qparts[i], obh1, obh, valInd, true);
					}
					q.add(JdbResources.getEncoder().encodeWL(obh1,true));
				}
			}
			return rec;
		}
		catch (IOException e)
		{
			e.printStackTrace();
			return rec;
		}
		catch (AMEFDecodeException e)
		{
			e.printStackTrace();
			return rec;
		}
	}
	
	private boolean checkColumnExists(String cond,JDBObject objtab)
	{
		for (int i = 0; i < objtab.getPackets().size(); i++)
		{
			if(cond.equals(objtab.getPackets().get(i).getNameStr())
					|| cond.indexOf("ucase("+objtab.getPackets().get(i).getNameStr())!=-1
					|| cond.indexOf("lcase("+objtab.getPackets().get(i).getNameStr())!=-1
					|| cond.indexOf("len("+objtab.getPackets().get(i).getNameStr())!=-1
					|| cond.indexOf("format("+objtab.getPackets().get(i).getNameStr())!=-1
					|| cond.indexOf("mid("+objtab.getPackets().get(i).getNameStr())!=-1
					|| cond.indexOf("first("+objtab.getPackets().get(i).getNameStr())!=-1
					|| cond.indexOf("last("+objtab.getPackets().get(i).getNameStr())!=-1
					|| cond.indexOf("round("+objtab.getPackets().get(i).getNameStr())!=-1
					|| cond.indexOf("count("+objtab.getPackets().get(i).getNameStr())!=-1
					|| cond.indexOf("max("+objtab.getPackets().get(i).getNameStr())!=-1
					|| cond.indexOf("min("+objtab.getPackets().get(i).getNameStr())!=-1
					|| cond.indexOf("sum("+objtab.getPackets().get(i).getNameStr())!=-1
					|| cond.indexOf("avg("+objtab.getPackets().get(i).getNameStr())!=-1
				    || cond.equals("now()") || cond.equals("sysdate()")
				    || cond.equals("count(*"))
				return true;
		}
		return false;
	}
	
	private boolean isAggregateFunc(String cond,JDBObject objtab)
	{
		for (int i = 0; i < objtab.getPackets().size(); i++)
		{
			if(cond.indexOf("count("+objtab.getPackets().get(i).getNameStr())!=-1
					|| cond.indexOf("max("+objtab.getPackets().get(i).getNameStr())!=-1
					|| cond.indexOf("min("+objtab.getPackets().get(i).getNameStr())!=-1
					|| cond.indexOf("sum("+objtab.getPackets().get(i).getNameStr())!=-1
					|| cond.indexOf("avg("+objtab.getPackets().get(i).getNameStr())!=-1
					|| cond.indexOf("first("+objtab.getPackets().get(i).getNameStr())!=-1
					|| cond.indexOf("last("+objtab.getPackets().get(i).getNameStr())!=-1
					|| cond.equals(objtab.getPackets().get(i).getNameStr()))
				return true;
		}
		return false;
	}
	
	private boolean isScalarFunc(String cond,JDBObject objtab)
	{
		for (int i = 0; i < objtab.getPackets().size(); i++)
		{
			if(cond.equals(objtab.getPackets().get(i).getNameStr()) 
					|| cond.indexOf("ucase("+objtab.getPackets().get(i).getNameStr())!=-1
					|| cond.indexOf("lcase("+objtab.getPackets().get(i).getNameStr())!=-1
					|| cond.indexOf("len("+objtab.getPackets().get(i).getNameStr())!=-1
					|| cond.indexOf("format("+objtab.getPackets().get(i).getNameStr())!=-1
					|| cond.indexOf("mid("+objtab.getPackets().get(i).getNameStr())!=-1
					|| cond.indexOf("round("+objtab.getPackets().get(i).getNameStr())!=-1)
				return true;
		}
		return false;
	}
	
	protected List getAMEFObjectso(Queue<Object> q,InputStream jdbin, String subq, JDBObject objtab, String[] qparts, boolean one, boolean aggr, String grpbycol) throws Exception
	{
		List list = new ArrayList();
		Map<String,Opval> whrandcls = new HashMap<String, Opval>();
		Map<String,Opval> whrorcls = new HashMap<String, Opval>();;
		Map<String,Integer> whrclsInd = new HashMap<String, Integer>();
		Map<String,Integer> valInd = new HashMap<String, Integer>();
		Map<Integer,String> whrclsIndv = new HashMap<Integer, String>();
		Map<Integer,String> whrclsIndt = new HashMap<Integer, String>();
		if(subq!=null && !subq.equals(""))
		{
			while(subq.indexOf(" and ")!=-1 || subq.indexOf(" or ")!=-1)
			{
				int anop = subq.indexOf(" and ");
				int orop = subq.indexOf(" or ");
				if((anop<orop && anop!=-1) || (anop!=-1 && orop==-1))
				{
					String ter = subq.split(" and ")[0].trim();
					getOperator(ter, whrandcls);
					subq = subq.substring(anop+5);
				}
				else if(orop!=-1)
				{
					String ter = subq.split(" or ")[0].trim();
					getOperator(ter, whrorcls);
					subq = subq.substring(orop+5);
				}
			}		
			getOperator(subq, whrandcls);
		}
		for (int i = 0; i < objtab.getPackets().size(); i++)
		{
			if(whrandcls.get(objtab.getPackets().get(i).getNameStr())!=null)
			{
				whrclsInd.put(objtab.getPackets().get(i).getNameStr(), i);
				whrclsIndv.put(i, whrandcls.get(objtab.getPackets().get(i).getNameStr()).value);
				whrclsIndt.put(i, new String(objtab.getPackets().get(i).getValue()));
			}
			else if(whrorcls.get(objtab.getPackets().get(i).getNameStr())!=null)
			{
				whrclsInd.put(objtab.getPackets().get(i).getNameStr(), i);
				whrclsIndv.put(i, whrorcls.get(objtab.getPackets().get(i).getNameStr()).value);
				whrclsIndt.put(i, new String(objtab.getPackets().get(i).getValue()));
			}
			valInd.put(objtab.getPackets().get(i).getNameStr(),i);
		}
		Map<String,Integer> mp = null;
		Map<String,String> mpt = null;
		mp = getPositions(objtab);
		mpt = getTypes(objtab);
		
		LinkedHashMap<Object, com.jdb.Table.AggInfo> aggVals = null,rowVals = null;
		if(aggr)
		{
			aggVals = new LinkedHashMap<Object, AggInfo>();
			rowVals = new LinkedHashMap<Object, AggInfo>();
		}
		boolean pksrch = false;
		if(whrandcls.get(mapping.get("PK"))!=null)
		{
			pksrch = true;
		}
		List<String> qprts =  new ArrayList<String>();
		String con = "";
		for (int i = 0; i < qparts.length; i++)
		{
			int i1 = getCharCount(qparts[i], '(');
			int i2 = getCharCount(qparts[i], ')');
			if(i1==i2)
			{
				con = qparts[i];
			}
			else
			{
				while(i1!=i2)
				{
					con += qparts[i] + ",";
					i1 = getCharCount(con, '(');
					i2 = getCharCount(con, ')');
					if(i1!=i2)i = i+1;
					
				}
				con = con.substring(0,con.length()-1);
			}
			if(checkColumnExists(con, objtab))
			{	
				qprts.add(con);
				con = "";
			}
		}
		qparts = qprts.toArray(new String[qprts.size()]);
		int rec = 0;
		try
		{
			byte[] type = new byte[1];	
			if(!pksrch)
			{						
				if(jdbin.available()>4)
				{
					int countss = 0;
					while(jdbin.available()>4)
					{	
						rec++;
						jdbin.read(type);
						int lengthm = 0;
						byte[] le = null;
						if(type[0]==(byte)JDBObject.VS_OBJECT_TYPE)
						{
							le = new byte[1];
							jdbin.read(le);
							lengthm = JdbResources.byteArrayToInt(le);
						}
						else if(type[0]==JDBObject.S_OBJECT_TYPE)
						{
							le = new byte[2];
							jdbin.read(le);
							lengthm = JdbResources.byteArrayToInt(le);
						}
						else if(type[0]==JDBObject.B_OBJECT_TYPE)
						{
							le = new byte[3];
							jdbin.read(le);
							lengthm = JdbResources.byteArrayToInt(le);
						}
						else
						{
							le = new byte[4];
							jdbin.read(le);
							lengthm = JdbResources.byteArrayToInt(le);
						}
						ByteBuffer buf  = ByteBuffer.allocate(1+le.length+lengthm);
						byte[] data = new byte[lengthm];
						jdbin.read(data);
						buf.put(type);
						buf.put(le);
						buf.put(data);
						buf.flip();
						buf.clear();
						JDBObject obh = JdbResources.getDecoder().decodeB(buf.array(), false,true);
						if(whrandcls.size()!=0 || whrorcls.size()!=0)
						{						
							if(!evaluate(obh, whrandcls, whrorcls, whrclsInd, whrclsIndv, whrclsIndt))
							{
								obh = null;
							}
						}
						if(obh==null)continue;
						if(qparts==null || qparts.length==0 || qparts[0].equals("*"))
						{
							list.add(obh);
						}
						else
						{
							if(one)
							{
								JDBObject obh1 = new JDBObject();
								for (int i = 0; i < qparts.length; i++)
								{
									createRow(qparts[i], obh1, obh, valInd, true);
								}
								list.add(obh1);
							}
							else if(aggr)
							{
								JDBObject obh1 = new JDBObject();
								for (int i = 0; i < qparts.length; i++)
								{
									if(isScalarFunc(qparts[i], objtab))
									{
										aggVals.put(qparts[i], null);
									}
									else
									{
										AggTyp agg = createRow(qparts[i], obh, obh1, aggVals, mp,grpbycol,mpt,rowVals);
										if((!agg.aggr && grpbycol.indexOf(qparts[i])==-1))
											throw new Exception("Invalid group by clause");
										if(agg.cnttyp)
											countss++;
									}
								}
							}
							else
								list.add(obh);
						}
					}	
					if(!one && aggr)
					{	
						for (Iterator iter = rowVals.entrySet().iterator(); iter.hasNext();)
						{
							JDBObject obh = new JDBObject();
							Map.Entry<Object,AggInfo> entry = (Map.Entry<Object,AggInfo>)iter.next();
							for (Iterator iter1 = aggVals.entrySet().iterator(); iter1.hasNext();)
							{
								Map.Entry<Object,AggInfo> entry1 = (Map.Entry<Object,AggInfo>)iter1.next();
								if(mp.get((String)entry1.getKey())!=null)
								{
									JDBObject objt = (JDBObject)entry.getKey();
									for (int i = 0; i < objt.getPackets().size(); i++)
									{
										if(objt.getPackets().get(i).getNameStr().equals((String)entry1.getKey()))
											obh.addPacket(objt.getPackets().get(i).getValue(),
													objt.getPackets().get(i).getType());
									}
									
								}
								else if(isScalarFunc((String)entry1.getKey(), objtab))
								{
									createRow((String)entry1.getKey(), obh, (JDBObject)entry.getKey(), valInd, false);
								}
								else if(((String)entry1.getKey()).endsWith("count"))
								{
									if(grpbycol.equals(""))
										obh.addPacket((entry.getValue().cnt+1)/countss);
									else
										obh.addPacket(entry.getValue().cnt+1);
								}
								else
								{
									obh.addPacket(entry1.getValue().getFinalValue());
								}								
							}
							list.add(obh);
						}
					}
					return list;
				}
			}
			else
			{
				long id = Long.parseLong(whrandcls.get(mapping.get("PK")).value);
				long tr = getIndex(id);
				if(tr==-1)
					return list;
				jdbin.skip(tr);
				jdbin.read(type);
				int lengthm = 0;
				byte[] le = null;
				if(type[0]==(byte)JDBObject.VS_OBJECT_TYPE)
				{
					le = new byte[1];
					jdbin.read(le);
					lengthm = JdbResources.byteArrayToInt(le);
				}
				else if(type[0]==JDBObject.S_OBJECT_TYPE)
				{
					le = new byte[2];
					jdbin.read(le);
					lengthm = JdbResources.byteArrayToInt(le);
				}
				else if(type[0]==JDBObject.B_OBJECT_TYPE)
				{
					le = new byte[3];
					jdbin.read(le);
					lengthm = JdbResources.byteArrayToInt(le);
				}
				else
				{
					le = new byte[4];
					jdbin.read(le);
					lengthm = JdbResources.byteArrayToInt(le);
				}
				ByteBuffer buf  = ByteBuffer.allocate(1+le.length+lengthm);
				byte[] data = new byte[lengthm];
				jdbin.read(data);
				buf.put(type);
				buf.put(le);
				buf.put(data);
				buf.flip();
				buf.clear();
				JDBObject obh = JdbResources.getDecoder().decodeB(buf.array(), false,true);
				if(whrandcls.size()!=0 || whrorcls.size()!=0)
				{						
					if(!evaluate(obh, whrandcls, whrorcls, whrclsInd, whrclsIndv, whrclsIndt))
					{
						obh = null;
					}
				}
				if(qparts==null || qparts.length==0 || qparts[0].equals("*"))
				{
					list.add(obh);	
				}
				else
				{
					if(one)
					{
						JDBObject obh1 = new JDBObject();
						for (int i = 0; i < qparts.length; i++)
						{	
							if(qparts[i].equalsIgnoreCase("sysdate()"))
							{
								obh1.addPacket(new Date());
							}
							else	
								obh1.addPacket(obh.getPackets().get(valInd.get(qparts[i])).getValue(),
										obh.getPackets().get(valInd.get(qparts[i])).getType());
						}
						list.add(obh1);
					}
					else
						list.add(obh);
				}
			}
			return list;
		}
		catch (IOException e)
		{
			e.printStackTrace();
			return list;
		}
		catch (AMEFDecodeException e)
		{
			e.printStackTrace();
			return list;
		}
	}
	
	private Map<String,Integer> getPositions(JDBObject objtab)
	{
		Map<String,Integer> whrandcls = new HashMap<String, Integer>();
		for (int i = 0; i < objtab.getPackets().size(); i++)
		{
			whrandcls.put(objtab.getPackets().get(i).getNameStr(),i);
		}
		return whrandcls;
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
	
	class AggInfo<T>
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
	
	/*class AggKey
	{
		public String type;
		public Object value;
		
		public AggKey(String type, Object value)
		{
			super();
			this.type = type;
			this.value = value;
		}
		@Override
		public int hashCode()
		{
			final int PRIME = 31;
			int result = 1;
			result = PRIME * result + ((type == null) ? 0 : type.hashCode());
			result = PRIME * result + ((value == null) ? 0 : value.hashCode());
			return result;
		}
		@Override
		public boolean equals(Object obj)
		{
			if (this == obj) return true;
			if (obj == null) return false;
			if (getClass() != obj.getClass()) return false;
			final AggKey other = (AggKey) obj;
			if (type == null)
			{
				if (other.type != null) return false;
			}
			else if (!type.equals(other.type)) return false;
			if (value == null)
			{
				if (other.value != null) return false;
			}
			else if (!value.equals(other.value)) return false;
			return true;
		}
		
	}*/
	
	class AggTyp
	{
		public boolean aggr,cnttyp;		
	}
	
	private AggTyp createRow(String cond,JDBObject objm,JDBObject jdbo,Map<Object, AggInfo> aggVals,Map<String, Integer> mp, String grpbycol, Map<String, String> mpt,Map<Object, AggInfo> rowVals)
	{
		AggTyp aggr = new AggTyp();;
		if(cond.indexOf("count(")!=-1)
		{
			String colnam = cond.substring(cond.toLowerCase().indexOf("count(")+6);
			colnam = colnam.replaceFirst("\\)", "");
			Object key = getKey(objm,grpbycol,"",mp,"");			
			if(rowVals.get(key)==null)
			{	
				Object key1 = getKey1(objm, grpbycol, colnam,mp,"count");	
				if(aggVals.get(key1)==null)
				{
					aggVals.put(key1, new AggInfo<Long>(colnam,"count",1L,cond,
						null,
							'l'));
				}
				rowVals.put(key, new AggInfo<Long>(colnam,"count",1L,cond,
								objm.getPackets().get(mp.get(colnam)).getValue(),
								'l'));				
			}
			else
			{
				rowVals.get(key).incAvg();
				Object key1 = getKey1(objm, grpbycol, colnam,mp,"count");	
				if(aggVals.get(key1)==null)
				{
					aggVals.put(key1, new AggInfo<Long>(colnam,"count",1L,cond,
							null,
							'l'));
				}
			}
			aggr.cnttyp = true;
			aggr.aggr = true;			
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
			aggr.aggr = true;			
		}
		else if(cond.indexOf("max")!=-1)
		{
			String colnam = cond.substring(cond.toLowerCase().indexOf("max(")+4);
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
			aggr.aggr = true;		
		}
		else if(cond.indexOf("min")!=-1)
		{
			String colnam = cond.substring(cond.toLowerCase().indexOf("min(")+4);
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
			aggr.aggr = true;		
		}
		else if(cond.indexOf("first(")!=-1)
		{
			String colnam = cond.substring(cond.toLowerCase().indexOf("first(")+6);
			colnam = colnam.replaceFirst("\\)", "");
			Object key = getKey1(objm, grpbycol, colnam,mp,"first");	
			if(aggVals.get(key)==null)
			{
				Object valu = getKey(objm, colnam, colnam,mp,"first");
				aggVals.put(key, new AggInfo<Object>(colnam,"first",valu,null,null,
							objm.getPackets().get(mp.get(colnam)).getType()));
			}
			aggr.aggr = true;		
		}
		else if(cond.indexOf("last(")!=-1)
		{
			String colnam = cond.substring(cond.toLowerCase().indexOf("last(")+5);
			colnam = colnam.replaceFirst("\\)", "");
			Object key = getKey1(objm, grpbycol, colnam,mp,"last");	
			if(aggVals.get(key)==null)
			{
				Object valu = getKey(objm, colnam, colnam,mp,"last");
				aggVals.put(key, new AggInfo<Object>(colnam,"last",valu,null,null,
						objm.getPackets().get(mp.get(colnam)).getType()));
			}
			else
			{
				Object valu = getKey(objm, colnam, colnam,mp,"last");
				aggVals.get(key).count = valu;
			}
			aggr.aggr = true;		
		}
		if(aggr.aggr && rowVals.get(getKey(objm, grpbycol, "", mp, ""))==null)
		{
			rowVals.put(getKey(objm, grpbycol, "", mp, ""),new AggInfo());
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
	
	
	private Object getKey1(JDBObject objm, String grpbycol, String colnam, Map<String, Integer> mp, String type)
	{
		return colnam+type;
	}
	
	private Object getKey(JDBObject objm, String grpbycol, String colnam, Map<String, Integer> mp, String type)
	{
		if(grpbycol==null || grpbycol.equals(""))
			return colnam+type;
		else if(grpbycol.equals(colnam))
		{
			if(objm.getPackets().get(mp.get(colnam)).isNumber())
				return objm.getPackets().get(mp.get(colnam)).getNumericValue();
			else if(objm.getPackets().get(mp.get(colnam)).isFloatingPoint())
				return objm.getPackets().get(mp.get(colnam)).getDoubleValue();
			else if(objm.getPackets().get(mp.get(colnam)).isString() || objm.getPackets().get(mp.get(colnam)).isDate())
				return objm.getPackets().get(mp.get(colnam)).getValueStr();
			else if(objm.getPackets().get(mp.get(colnam)).isChar())
				return objm.getPackets().get(mp.get(colnam)).getValue()[0];
			return colnam + type;
		}
		else
		{
			String[] grbcs = grpbycol.split(",");
			JDBObject obj = new JDBObject();
			for (int i = 0; i < grbcs.length; i++)
			{
				obj.addPacket(objm.getPackets().get(mp.get(grbcs[i])).getValue(),
							objm.getPackets().get(mp.get(grbcs[i])).getType());
				obj.getPackets().get(i).setName(grbcs[i]);
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
	protected Set getAMEFObjectsDo(Queue<Object> q,InputStream jdbin, String subq, JDBObject objtab, String[] qparts, boolean one, boolean aggr, String grpbycol) throws Exception
	{
		Set list = new HashSet();
		Map<String,Opval> whrandcls = new HashMap<String, Opval>();
		Map<String,Opval> whrorcls = new HashMap<String, Opval>();;
		Map<String,Integer> whrclsInd = new HashMap<String, Integer>();
		Map<String,Integer> valInd = new HashMap<String, Integer>();
		Map<Integer,String> whrclsIndv = new HashMap<Integer, String>();
		Map<Integer,String> whrclsIndt = new HashMap<Integer, String>();
		if(subq!=null && !subq.equals(""))
		{
			while(subq.indexOf(" and ")!=-1 || subq.indexOf(" or ")!=-1)
			{
				int anop = subq.indexOf(" and ");
				int orop = subq.indexOf(" or ");
				if((anop<orop && anop!=-1) || (anop!=-1 && orop==-1))
				{
					String ter = subq.split(" and ")[0].trim();
					getOperator(ter, whrandcls);
					subq = subq.split(" and ")[1];
				}
				else if(orop!=-1)
				{
					String ter = subq.split(" or ")[0].trim();
					getOperator(ter, whrorcls);
					subq = subq.split(" or ")[1];
				}
			}		
			getOperator(subq, whrandcls);
		}
		for (int i = 0; i < objtab.getPackets().size(); i++)
		{
			if(whrandcls.get(objtab.getPackets().get(i).getNameStr())!=null)
			{
				whrclsInd.put(objtab.getPackets().get(i).getNameStr(), i);
				whrclsIndv.put(i, whrandcls.get(objtab.getPackets().get(i).getNameStr()).value);
				whrclsIndt.put(i, new String(objtab.getPackets().get(i).getValue()));
			}
			else if(whrorcls.get(objtab.getPackets().get(i).getNameStr())!=null)
			{
				whrclsInd.put(objtab.getPackets().get(i).getNameStr(), i);
				whrclsIndv.put(i, whrorcls.get(objtab.getPackets().get(i).getNameStr()).value);
				whrclsIndt.put(i, new String(objtab.getPackets().get(i).getValue()));
			}
			valInd.put(objtab.getPackets().get(i).getNameStr(),i);
		}
		boolean pksrch = false;
		if(whrandcls.get(mapping.get("PK"))!=null)
		{
			pksrch = true;
		}
		try
		{
			byte[] type = new byte[1];	
			if(!pksrch)
			{						
				if(jdbin.available()>4)
				{
					while(jdbin.available()>4)
					{	
						jdbin.read(type);
						int lengthm = 0;
						byte[] le = null;
						if(type[0]==(byte)JDBObject.VS_OBJECT_TYPE)
						{
							le = new byte[1];
							jdbin.read(le);
							lengthm = JdbResources.byteArrayToInt(le);
						}
						else if(type[0]==JDBObject.S_OBJECT_TYPE)
						{
							le = new byte[2];
							jdbin.read(le);
							lengthm = JdbResources.byteArrayToInt(le);
						}
						else if(type[0]==JDBObject.B_OBJECT_TYPE)
						{
							le = new byte[3];
							jdbin.read(le);
							lengthm = JdbResources.byteArrayToInt(le);
						}
						else
						{
							le = new byte[4];
							jdbin.read(le);
							lengthm = JdbResources.byteArrayToInt(le);
						}
						ByteBuffer buf  = ByteBuffer.allocate(1+le.length+lengthm);
						byte[] data = new byte[lengthm];
						jdbin.read(data);
						buf.put(type);
						buf.put(le);
						buf.put(data);
						buf.flip();
						buf.clear();
						JDBObject obh = JdbResources.getDecoder().decodeB(buf.array(), false,true);
						if(whrandcls.size()!=0 || whrorcls.size()!=0)
						{						
							if(!evaluate(obh, whrandcls, whrorcls, whrclsInd, whrclsIndv, whrclsIndt))
							{
								obh = null;
							}
						}
						if(obh==null)continue;
						if((qparts==null || qparts.length==0 || qparts[0].equals("*")) 
								&& !list.contains(obh))
						{
							list.add(obh);
						}
						else if(!list.contains(obh))
						{
							if(one)
							{
								JDBObject obh1 = new JDBObject();
								for (int i = 0; i < qparts.length; i++)
								{	
									createRow(qparts[i], obh1, obh, valInd, true);
								}
								list.add(obh1);
							}
							else
								list.add(obh);							
						}
					}				
					return list;
				}
			}
			else
			{
				long id = Long.parseLong(whrandcls.get(mapping.get("PK")).value);
				long tr = getIndex(id);
				if(tr==-1)
					return list;
				jdbin.skip(tr);
				jdbin.read(type);
				int lengthm = 0;
				byte[] le = null;
				if(type[0]==(byte)JDBObject.VS_OBJECT_TYPE)
				{
					le = new byte[1];
					jdbin.read(le);
					lengthm = JdbResources.byteArrayToInt(le);
				}
				else if(type[0]==JDBObject.S_OBJECT_TYPE)
				{
					le = new byte[2];
					jdbin.read(le);
					lengthm = JdbResources.byteArrayToInt(le);
				}
				else if(type[0]==JDBObject.B_OBJECT_TYPE)
				{
					le = new byte[3];
					jdbin.read(le);
					lengthm = JdbResources.byteArrayToInt(le);
				}
				else
				{
					le = new byte[4];
					jdbin.read(le);
					lengthm = JdbResources.byteArrayToInt(le);
				}
				ByteBuffer buf  = ByteBuffer.allocate(1+le.length+lengthm);
				byte[] data = new byte[lengthm];
				jdbin.read(data);
				buf.put(type);
				buf.put(le);
				buf.put(data);
				buf.flip();
				buf.clear();
				JDBObject obh = JdbResources.getDecoder().decodeB(buf.array(), false,true);
				if(whrandcls.size()!=0 || whrorcls.size()!=0)
				{						
					if(!evaluate(obh, whrandcls, whrorcls, whrclsInd, whrclsIndv, whrclsIndt))
					{
						obh = null;
					}
				}
				if((qparts==null || qparts.length==0 || qparts[0].equals("*")) 
						&& !list.contains(obh))
				{
					list.add(obh);
				}
				else if(!list.contains(obh))
				{
					if(one)
					{
						JDBObject obh1 = new JDBObject();
						for (int i = 0; i < qparts.length; i++)
						{	
							createRow(qparts[i], obh1, obh, valInd, true);
						}
						list.add(obh1);
					}
					else
						list.add(obh);	
				}
			}
			return list;
		}
		catch (IOException e)
		{
			e.printStackTrace();
			return list;
		}
		catch (AMEFDecodeException e)
		{
			e.printStackTrace();
			return list;
		}
	}
	
	
	
	transient RandomAccessFile ramInd;
	transient Map<Long,Long> index;
	public long getIndex(long id) throws Exception
	{		
		if(index==null)
		{
			index = new ConcurrentHashMap<Long, Long>();			
		}
		if(index.get(id)!=null)
		{
			return index.get(id);
		}
		long tr = -1;
		byte[] ident = new byte[8];
		if(ramInd==null)
			ramInd = new RandomAccessFile(getIndexFileName(),"r");
		while(ramInd.read(ident)!=-1)
		{	
			long idenlgid = JdbResources.byteArrayToLong(ident,4);
			if(idenlgid==id)
			{
				tr = JdbResources.byteArrayToLong(ident,4,4);	
				index.put(idenlgid, tr);
				break;
			}
		}
		return tr;
	}
	
	protected int updateObjects(String subq, JDBObject objtab,Map<String,byte[]> nvalues, int index) throws Exception
	{
		int rec = 0;
		Map<String,Opval> whrandcls = new HashMap<String, Opval>();
		Map<String,Opval> whrorcls = new HashMap<String, Opval>();;
		Map<String,Integer> whrclsInd = new HashMap<String, Integer>();
		Map<Integer,String> whrclsIndv = new HashMap<Integer, String>();
		Map<Integer,String> whrclsIndt = new HashMap<Integer, String>();
		Map<Integer,String> setrel = new HashMap<Integer,String>();
		File nf = new File(getFileName(index)+"temp_update");
		BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(nf,true));
		if(subq!=null && !subq.equals(""))
		{
			while(subq.indexOf(" and ")!=-1 || subq.indexOf(" or ")!=-1)
			{
				int anop = subq.indexOf(" and ");
				int orop = subq.indexOf(" or ");
				if((anop<orop && anop!=-1) || (anop!=-1 && orop==-1))
				{
					String ter = subq.split(" and ")[0].trim();
					getOperator(ter, whrandcls);
					subq = subq.split(" and ")[1];
				}
				else if(orop!=-1)
				{
					String ter = subq.split(" or ")[0].trim();
					getOperator(ter, whrorcls);
					subq = subq.split(" or ")[1];
				}
			}		
			getOperator(subq, whrandcls);
		}
		for (int i = 0; i < objtab.getPackets().size(); i++)
		{
			if(whrandcls.get(objtab.getPackets().get(i).getNameStr())!=null)
			{
				whrclsInd.put(objtab.getPackets().get(i).getNameStr(), i);
				whrclsIndv.put(i, whrandcls.get(objtab.getPackets().get(i).getNameStr()).value);
				whrclsIndt.put(i, new String(objtab.getPackets().get(i).getValue()));
			}
			else if(whrorcls.get(objtab.getPackets().get(i).getNameStr())!=null)
			{
				whrclsInd.put(objtab.getPackets().get(i).getNameStr(), i);
				whrclsIndv.put(i, whrorcls.get(objtab.getPackets().get(i).getNameStr()).value);
				whrclsIndt.put(i, new String(objtab.getPackets().get(i).getValue()));
			}
			setrel.put(i,objtab.getPackets().get(i).getNameStr());
		}
		try
		{	
			File f = new File(getFileName(index));
			InputStream jdbin = new FileInputStream(f);
			byte[] type = new byte[1];			
			if(jdbin.available()>4)
			{
				while(jdbin.available()>4)
				{	
					rec++;
					jdbin.read(type);
					int lengthm = 0;
					byte[] le = null;
					if(type[0]==(byte)JDBObject.VS_OBJECT_TYPE)
					{
						le = new byte[1];
						jdbin.read(le);
						lengthm = JdbResources.byteArrayToInt(le);
					}
					else if(type[0]==JDBObject.S_OBJECT_TYPE)
					{
						le = new byte[2];
						jdbin.read(le);
						lengthm = JdbResources.byteArrayToInt(le);
					}
					else if(type[0]==JDBObject.B_OBJECT_TYPE)
					{
						le = new byte[3];
						jdbin.read(le);
						lengthm = JdbResources.byteArrayToInt(le);
					}
					else
					{
						le = new byte[4];
						jdbin.read(le);
						lengthm = JdbResources.byteArrayToInt(le);
					}
					ByteBuffer buf  = ByteBuffer.allocate(1+le.length+lengthm);
					byte[] data = new byte[lengthm];
					jdbin.read(data);
					buf.put(type);
					buf.put(le);
					buf.put(data);
					buf.flip();
					buf.clear();
					JDBObject obh = JdbResources.getDecoder().decodeB(buf.array(), false,true);	
					JDBObject obh1 = null;
					if(whrandcls.size()!=0 || whrorcls.size()!=0)
					{							
						if(evaluate(obh, whrandcls, whrorcls, whrclsInd, whrclsIndv, whrclsIndt))
						{
							obh1 = new JDBObject();
							for (int i = 0; i < obh.getPackets().size(); i++)
							{
								if(nvalues.get(setrel.get(i))==null)
									obh1.addPacket(obh.getPackets().get(i).getValue(),obh.getPackets().get(i).getType());
								else
									obh1.addPacket(nvalues.get(setrel.get(i)),obh.getPackets().get(i).getType());
							}
						}
					}
					else if(subq.equals(""))
					{
						obh1 = new JDBObject();
						for (int i = 0; i < obh.getPackets().size(); i++)
						{
							if(nvalues.get(setrel.get(i))==null)
								obh1.addPacket(obh.getPackets().get(i).getValue(),obh.getPackets().get(i).getType());
							else
								obh1.addPacket(nvalues.get(setrel.get(i)),obh.getPackets().get(i).getType());
						}
					}
					if(obh1==null)
						bos.write(buf.array());
					else
						bos.write(JdbResources.getEncoder().encodeWL(obh1, true));
				}
				if(bos!=null)
				{
					jdbin.close();
					bos.flush();
					bos.close();
					bos = null;
					f.delete();
					nf.renameTo(f);
				}
				return rec;
			}
			else
				return rec;
		}
		catch (IOException e)
		{
			e.printStackTrace();
			return rec;
		}
		catch (AMEFDecodeException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
			return rec;
		}
	}
	
	
	protected byte[] getObjectsStream()
	{
		String fileName = partitions.get(currentFileIndex);
		byte[] bytes = null;
		try
		{
			long length = new File(fileName).length();
			if (length > Integer.MAX_VALUE) {
		        // File is too large
		    }

			bytes = new byte[(int)length];
			InputStream jdbin = new FileInputStream(new File(fileName));
			
		    int offset = 0;
		    int numRead = 0;
		    while (offset < bytes.length
		           && (numRead=jdbin.read(bytes, offset, bytes.length-offset)) >= 0) {
		        offset += numRead;
		    }

		    // Ensure all the bytes have been read in
		    if (offset < bytes.length) {
		        
		    }

			jdbin.close();
		}
		catch (FileNotFoundException e)
		{	
			try
			{
				ObjectOutputStream jdbout = new ObjectOutputStream(new FileOutputStream(new File(fileName)));
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return bytes;
	}
	
	public int getRecords()
	{
		return n1+n2;
	}
	public void setRecords(int records)
	{
		//this.records = records;
	}
	public void incRecords()
	{
		synchronized (this)
		{
			records++;
		}
	}
	
	public int incRecords(int i,int j)
	{
		synchronized (this)
		{
			records++;
			/*Collections.sort(recordspf,new ComparPos());
			Algo alg = recordspf.get(i);
			alg.num++;
			recordspf.set(i, alg);*/
			if(i==0 || algo==1)
				n1 = n1+j;
			else
				n2 = n2+j;
		}
		return records;
	}
	public int incRecords(int i)
	{
		synchronized (this)
		{
			records++;
			/*Collections.sort(recordspf,new ComparPos());
			Algo alg = recordspf.get(i);
			alg.num++;
			recordspf.set(i, alg);*/
			if(i==0 || algo==1)
				n1++;
			else
				n2++;			
		}
		return records;
	}
	public Map<String, String> getMapping()
	{
		return mapping;
	}
	public int getAlgo()
	{
		return algo;
	}
	public void setAlgo(int algo)
	{
		this.algo = algo;
	}
	public String getFileNameToInsert()
	{
		//Collections.sort(recordspf,new ComparNum());
		//return getFileName(recordspf.get(0).pos);
		if(n1<n2 || algo==1)
			return getFileName(0);
		else
			return getFileName(1);
	}
	public String getFileNameToInsert(int ind)
	{
		return getFileName(ind);
	}
	public int getFileIndexToInsert()
	{
		/*Collections.sort(recordspf,new ComparNum());
		return recordspf.get(0).pos;*/
		if(n1<n2 || algo==1)
			return 0;
		else 
			return 1;
	}
}
