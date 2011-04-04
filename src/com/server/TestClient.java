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
package com.server;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import com.Temp;
import com.amef.AMEFDecodeException;
import com.amef.AMEFEncodeException;
import com.amef.JDBEncoder;
import com.amef.JDBObject;
import com.jdb.JdbNewDR;
import com.jdb.JdbResources;
import com.jdb.Reader;

public final class TestClient
{
	
	static Map<String,String> whrandcls = null;
	static Map<String,String> whrorcls = null;
	static Map<String,Integer> whrclsInd = null;
	static Map<Integer,String> whrclsIndv = null;
	static Map<Integer,String> whrclsIndt = null;
	static Map<String,String> mappik = new LinkedHashMap<String,String>();
	static Map<String,String> mappikt = new HashMap<String,String>();
	static Map<String,String> mappiktt = new HashMap<String,String>();
	static ExecutorService execs = Executors.newFixedThreadPool(2);
	
	static final class Session
	{
		Map<String,List> trxObjs = null;
		List<byte[]> updates = null;
		Map<String,Integer> trxObjLens = null;
		JdbNewDR reader = null;
		SocketChannel sock = null;
		int qsize = 0;
		boolean trans = false;
		int factor = 100000;
		public Session()
		{
			reader = new JdbNewDR();
			try
			{
				sock = SocketChannel.open(new InetSocketAddress("localhost",7001));
				sock.socket().setReceiveBufferSize(100240000);
				trxObjs = new HashMap<String, List>();
				trxObjLens = new HashMap<String, Integer>();
				updates = new ArrayList<byte[]>();
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}			
		}		
		public void beginTransaction()
		{			
			JDBObject query = new JDBObject();
			query.addPacket("start transaction");			
			try
			{
				byte[] data = JdbResources.getEncoder().encodeB(query, false);
				query = null;
				ByteBuffer buf = ByteBuffer.allocate(data.length);
				buf.put(data);
				buf.flip();
				sock.write(buf);	
				buf = null;
				reader.reset1();
				while(!reader.isDone())
				{
					data = reader.readLim4(sock,2);
				}
				if(data[0]==1)
				{
					System.out.println("Transation started");
					trans = true;
				}
				else
					System.out.println("Transation could not be started");
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}
		
		public void flush() throws Exception
		{
			partialCommit();
		}
		
		public void commit() throws Exception
		{
			if(!trans)
				return;			
			reader.reset1();
			if(trxObjs.size()>0)
			{	
				partialCommit();
				for (int i = 0; i < updates.size(); i++)
				{
					ByteBuffer buf = ByteBuffer.allocate(updates.get(i).length);
					buf.put(updates.get(i));
					buf.flip();
					sock.write(buf);	
					buf = null;
					byte[] data = null;
					reader.reset1();
					while(!reader.isDone())
					{
						data = reader.readLim4(sock,2);
					}
					if(data[0]==1)
						System.out.println("Updated Row");
					else
						System.out.println("Updated failed");
				}
				JDBObject query = new JDBObject();
				query.addPacket("commit");
				byte[] data = JdbResources.getEncoder().encodeB(query, false);
				query = null;
				ByteBuffer buf = ByteBuffer.allocate(data.length);
				buf.put(data);
				buf.flip();
				try
				{
					sock.write(buf);	
					buf = null;
					reader.reset1();
					while(!reader.isDone())
					{
						data = reader.readLim4(sock,2);
					}
					if(data[0]==1)
						System.out.println("Commit succeded");
					else
						System.out.println("Commit failed");
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
			}
		}
		
		private void singleInsert(String table,byte[] dat) throws AMEFEncodeException
		{
			reader.reset1();
			JDBObject query = new JDBObject();
			query.addPacket("insert into "+mappikt.get(table)+" values(2,'ssd')");
			query.addPacket(dat);
			byte[] data = JdbResources.getEncoder().encodeB(query, false);
			query = null;
			ByteBuffer buf = ByteBuffer.allocate(data.length);
			buf.put(data);
			buf.flip();
			try
			{
				sock.write(buf);
				buf = null;
				while(!reader.isDone())
				{
					data = reader.readLim4(sock,2);
				}
				if(data[0]==1)
					System.out.println("JDB: 1 Row inserted");
				else
					System.out.println("JDB: Insert Failed");
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
			reader.reset1();
		}
		
		public void partialCommit() throws Exception
		{
			//if(!trans)
			//	return;
			reader.reset1();
			if(trxObjs.size()>0)
			{				
				for (Map.Entry<String,List> entry : trxObjs.entrySet())
				{
					if(qsize>0)
					{						
						int lim = qsize/factor,rem=qsize%factor;
						for (int j = 0; j < lim; j++)
						{
							JDBObject query = new JDBObject();
							query.addPacket("insert into "+mappikt.get(entry.getKey())+" values(2,'ssd')");
							ByteBuffer buffe = ByteBuffer.allocate(trxObjLens.get(entry.getKey()));
							//for (int i = lim*factor; i < lim*factor+rem; i++)
							while(qsize>j*factor)
							{
								buffe.put((byte[])((LinkedList)entry.getValue()).removeFirst());
								qsize--;
							}
							buffe.flip();
							query.addPacket(buffe.array());
							byte[] data = JdbResources.getEncoder().encodeB(query, false);
							query = null;
							ByteBuffer buf = ByteBuffer.allocate(data.length);
							buf.put(data);
							buf.flip();
							try
							{
								sock.write(buf);
								trxObjLens.put(entry.getKey(), trxObjLens.get(entry.getKey())-buffe.limit());
								buf = null;
								while(!reader.isDone())
								{
									data = reader.readLim4(sock,2);
								}
								if(data[0]==1)
									System.out.println("JDB: 1 Row inserted");
								else
									System.out.println("JDB: Insert Failed");
							}
							catch (Exception e)
							{
								e.printStackTrace();
							}
							reader.reset1();
						}
						if(rem>0)
						{
							/*JDBObject query = new JDBObject();
							query.addPacket("insert into "+mappikt.get(entry.getKey())+" values(2,'ssd')");*/
							ByteBuffer buffe = ByteBuffer.allocate(trxObjLens.get(entry.getKey()));
							while(qsize>0)
							{
								buffe.put((byte[])((LinkedList)entry.getValue()).removeFirst());
								qsize--;
							}
							buffe.flip();
							singleInsert(entry.getKey(), buffe.array());
							trxObjLens.put(entry.getKey(), trxObjLens.get(entry.getKey())-buffe.limit());
							/*query.addPacket(buffe.array());
							byte[] data = JdbResources.getEncoder().encodeB(query, false);
							query = null;
							ByteBuffer buf = ByteBuffer.allocate(data.length);
							buf.put(data);
							buf.flip();
							try
							{
								sock.write(buf);
								trxObjLens.put(entry.getKey(), trxObjLens.get(entry.getKey())-buffe.limit());
								buf = null;
								while(!reader.isDone())
								{
									data = reader.readLim4(sock,2);
								}
								if(data[0]==1)
									System.out.println("JDB: 1 Row inserted");
								else
									System.out.println("JDB: Insert Failed");
							}
							catch (Exception e)
							{
								e.printStackTrace();
							}
							reader.reset1();*/
						}						
					}
				}
			}			
		}
		public void rollback()
		{
			if(!trans)
				return;
			trxObjs = null;
			JDBObject query = new JDBObject();
			query.addPacket("rollback");			
			try
			{
				byte[] data = JdbResources.getEncoder().encodeB(query, false);
				query = null;
				ByteBuffer buf = ByteBuffer.allocate(data.length);
				buf.put(data);
				buf.flip();
				sock.write(buf);	
				buf = null;
				reader.reset1();
				while(!reader.isDone())
				{
					data = reader.readLim4(sock,2);
				}
				if(data[0]==1)
				{
					System.out.println("Rollback succeeded");
					trans = true;
				}
				else
					System.out.println("Rollback failed");
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}
		
		@SuppressWarnings("unchecked")
		public void save(Object object) throws Exception
		{
			/*JDBObject row = getRow(object);
			System.out.println(getRowValue(object));
			row.addPacket("11-12-2010 12:12:12");
			row.addPacket(true);
			row.addPacket('Y');*/
			String table = object.getClass().toString().replaceFirst("class ", "");	
			JDBObject objj = (JDBObject)getJDBObject(object);
			if(objj==null){System.out.println("Severe exception");System.exit(0);}
			byte[] dat = new JDBEncoder().encodeWL(objj, true);//getRowValue(object);
			if(!trans)
			{
				singleInsert(table, dat);
				return;
			}
			if(!trxObjs.containsKey(table))
			{
				trxObjs.put(table, new LinkedList());
				trxObjLens.put(table, 0);
			}
			trxObjs.get(table).add(dat);
			trxObjLens.put(table, trxObjLens.get(table)+dat.length);
			qsize++;
			if(qsize==factor || !trans)
				partialCommit();
		}
		
		public void update(Object object) throws Exception
		{
			String table = object.getClass().toString().replaceFirst("class ", "");
			byte[] dat = JdbResources.getEncoder().encodeB(getUpdateRow(object),false);
			JDBObject query = new JDBObject();
			query.addPacket("update temp1 set id=22 where id=13");
			query.addPacket(dat);
			byte[] data = JdbResources.getEncoder().encodeB(query, false);
			if(trans)
			{
				updates.add(data);
				return;
			}
			query = null;
			ByteBuffer buf = ByteBuffer.allocate(data.length);
			buf.put(data);
			buf.flip();
			try
			{
				sock.write(buf);
				reader.reset1();
				buf = null;
				while(!reader.isDone())
				{
					data = reader.readLim4(sock,2);
				}
				if(data[0]==1)
					System.out.println("JDB: 1 Row Updated");
				else
					System.out.println("JDB: Update Failed");
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
			reader.reset1();
		}
		
		public List select() throws Exception
		{
			List objects = new LinkedList();
			JDBObject query = new JDBObject();
			query.addPacket("select * from temp1");
			byte[] data = JdbResources.getEncoder().encodeB(query, false);
			ByteBuffer buf = ByteBuffer.allocate(data.length);
			buf.put(data);
			buf.flip();
			long rdtim = 0,odtim = 0;
			try
			{
				sock.write(buf);			
				JdbAMEFObjMaker maker = new JdbAMEFObjMaker();
				JdbAMEFReader amefRead = new JdbAMEFReader(reader,sock,maker);
				FutureTask amefReadTask = new FutureTask(amefRead);
				FutureTask<List> makerTask = new FutureTask<List>(maker);
				execs.execute(amefReadTask);
				execs.execute(makerTask);
				amefReadTask.get();
				objects = makerTask.get();
				execs.shutdown();
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
			System.out.println("time reqd for comlpetion = "+(System.currentTimeMillis()-stt)
					+"\nSize of objects = "+(objects.size())
					+"\nTime to read = "+(rdtim)
					+"\nTime to object = "+(odtim));
			return objects;
		}
		
		public RowObject selectf() throws Exception
		{
			RowObject object = null;
			JDBObject query = new JDBObject();
			query.addPacket("select * from temp1");
			byte[] data = JdbResources.getEncoder().encodeB(query, false);
			ByteBuffer buf = ByteBuffer.allocate(data.length);
			buf.put(data);
			buf.flip();
			long rdtim = 0,odtim = 0;
			try
			{
				sock.write(buf);			
				//JdbAMEFObjMakef maker = new JdbAMEFObjMakef();
				JdbAMEFObjMakef2 makef = new JdbAMEFObjMakef2();
				JdbAMEFReaderf amefRead = new JdbAMEFReaderf(new JdbDataReader(),sock,makef);
				//JdbAMEFReaderfo amefRead = new JdbAMEFReaderfo(new JdbDataReader(),sock,maker);
				new Thread(amefRead).start();
				//return maker.run();
				object = makef.call();
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
			System.out.println("time reqd for comlpetion = "+(System.currentTimeMillis()-stt)
					+"\nSize of objects = "+object.size()
					+"\nTime to read = "+(rdtim)
					+"\nTime to object = "+(odtim));
			return object;
		}
	}
	
	static class DMLResReader implements Callable
	{
		JdbDataReader reader;
		SocketChannel sock;
		public DMLResReader(JdbDataReader reader, SocketChannel sock)
		{
			super();
			this.reader = reader;
			this.sock = sock;
		}

		public Object call() throws Exception
		{	
			long st = System.currentTimeMillis();
			byte data[] = null;
			while(!reader.isDone())
			{
				data = reader.read(sock,2);
			}
			tim += (System.currentTimeMillis()-st);
			if(data[0]==1)
				System.out.println("1 Row inserted");
			else
				System.out.println("Insert Failed");
			return null;
		}
	}
	
	static class Temp implements Serializable
	{
		public Temp()
		{
			
		}
		//private static final long serialVersionUID = -7070060949055791230L;
		int id;
		String name;
	}
	/**
	 * @param args
	 * @throws Exception 
	 */
	static long stt = 0;
	public static void main(String[] args) throws Exception
	{
		try
		{
			int val = JdbResources.byteArrayToInt(new byte[]{-113});
			System.out.println(JdbResources.intToByteArrayS(val, 1));
			//System.out.println(val);
			//byte[] rt = JdbResources.intToByteArrayS(val, 1).getBytes();
			//System.out.println(JdbResources.byteArrayToInt(rt));
			Integer ie = -113;
			System.out.println(Integer.toHexString(ie));
			for (int i = -128; i < 129; i++)
			{
				//System.out.println((char)i + " -> " + i + " -> " + JdbResources.byteArrayToInt(JdbResources.intToByteArrayS(i,1).getBytes()));
			}
			mappik.put("class", "com.Temp");
			mappik.put("id", "id");
			mappik.put("name", "name");
			mappik.put("date1", "date1");
			mappik.put("flag", "flag");
			mappik.put("ind", "ind");
			
			mappikt.put("com.Temp", "temp1");
			mappikt.put("id", "int");
			mappikt.put("name", "java.lang.String");
			mappikt.put("date1", "java.lang.String");
			mappikt.put("flag", "boolean");
			mappikt.put("ind", "char");
			
			mappiktt.put("class", "com.Temp");
			mappiktt.put("id", "int");
			mappiktt.put("name", "java.lang.String");
			mappiktt.put("date1", "java.lang.String");
			mappiktt.put("flag", "boolean");
			mappiktt.put("ind", "char");
			
			initialize(mappiktt);
			initializeRM(mappiktt);
			
			JdbNewDR reader = new JdbNewDR();
			JdbResources.getDecoder();
			JdbResources.getEncoder();
			SocketChannel sock = null;//SocketChannel.open(new InetSocketAddress("localhost",7001));
			//sock.configureBlocking(false);
			//sock.socket().setReceiveBufferSize(100240000);
			//sock.configureBlocking(false);
			JDBObject object = new JDBObject();
			object.addPacket(13);
			object.addPacket("saumil");
			object.addPacket("11-12-2010 12:12:12");
			object.addPacket(true);
			object.addPacket('Y');
			
			com.Temp tem = new com.Temp();
			List objt =  new ArrayList();
			tem.id  = 14;
			tem.name = "ccccccc";
			//tem.date1 = "11-12-2010 12:12:12";
			//tem.flag = true;
			//tem.ind = 'Y';
			long st = System.currentTimeMillis();
			Session session = new Session();
			session.beginTransaction();
			for (int i = 0; i < 100; i++)
			{
				session.save(tem);
				session.flush();
			}
			com.Temp tem1 = new com.Temp();
			tem1.id  = 22;
			//session.update(tem1);
			session.commit();
			//System.out.println("Time for inserts = "+(System.currentTimeMillis()-st));
			System.out.println("Time for inserts = "+(System.currentTimeMillis()-st));
			st = System.currentTimeMillis();
			RowObject obj = session.selectf();
			System.out.println("Time for select  = "+(System.currentTimeMillis()-st));
			st = System.currentTimeMillis();
			obj.get(999999);
			System.out.println("Time for all fetch  = "+(System.currentTimeMillis()-st));
			
			/*long st1 = System.currentTimeMillis();stt=st1;
			String query1 = "select * from temp1";
			List<JDBObject> data = execute(query1,sock,true,reader);	
			System.out.println("Time reqd for selects = "+(System.currentTimeMillis()-st1)+"\n");
			
			st1 = System.currentTimeMillis();stt=st1;
			query1 = "select * from temp1";
			data = execute(query1,sock,true,reader);	
			System.out.println("Time reqd for selects 2 = "+(System.currentTimeMillis()-st1)+"\n");
			
			st1 = System.currentTimeMillis();stt=st1;
			query1 = "select * from temp1";
			data = execute(query1,sock,true,reader);	
			System.out.println("Time reqd for selects 3 = "+(System.currentTimeMillis()-st1)+"\n");*/
			
			/*long st = System.currentTimeMillis();
			String query = "insert into temp1 values(13,'saumil','12-12-12 12:12:12','1','N')";
			for (int i = 0; i < 1000; i++)
			{
				execute(query, sock, tem, reader);
				//bulkexecute(query, sock, object, reader, 10000);
			}
			for (int i = 0; i < 10000; i++)
			{
				objt.add(tem);
			}		
			bulkexecute(query, sock, objt, reader);
			long st1 = System.currentTimeMillis();stt=st1;
			String query1 = "select * from temp1";
			List<JDBObject> data = execute(query1,sock,true,reader);
			System.out.println("Time reqd for inserts = "+(st1-st)+"\nTime for recv resp = "+tim);	
			//System.out.println("Time reqd for selects = "+(System.currentTimeMillis()-st1)+"\n");*/
		}
		catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	static ClassLoader loader = null;
	
	private static void initialize(Map<String, String> mappikt2) 
	{
		StringBuffer build = new StringBuffer();
		build.append("\n\npublic class runtime_"+mappikt2.get("class").replaceAll("\\.", "_")+"\n{\n");
		build.append("\tpublic "+mappikt2.get("class")+" getObject(com.amef.JDBObject obh)\n\t{\n\t\tint i=0;\n\t\t"+
				"if(obh==null || obh.getPackets().size()==1)return null;\n\t\t" +
				mappikt2.get("class")+" _object = new "+mappikt2.get("class")+"();\n");
		for (Iterator<Map.Entry<String,String>> iter = mappikt2.entrySet().iterator(); iter.hasNext();)
		{
			Map.Entry<String,String> entry = iter.next();
			if(!entry.getKey().equals("class"))
			{
				if(entry.getValue().equals("java.lang.String"))
				{
					build.append("\t\tif(!obh.getPackets().get(i).isNullString())_object.set"+entry.getKey().substring(0,1).toUpperCase() + entry.getKey().substring(1)+
							"(new String(obh.getPackets().get(i++).getValue()));\n");
				}
				else if((entry.getValue().equals("int") || entry.getValue().equals("java.lang.Integer"))
						/* && mappi.get(entry.getKey())!=null*/)
				{
					build.append("\t\tif(!obh.getPackets().get(i).isNullNumber())_object.set"+entry.getKey().substring(0,1).toUpperCase() + entry.getKey().substring(1)+
							"(com.jdb.JdbResources.byteArrayToInt(obh.getPackets().get(i++).getValue()));\n");
				}
				else if((entry.getValue().equals("long") || entry.getValue().equals("java.lang.Long"))
						/* && mappi.get(entry.getKey())!=null*/)
				{
					build.append("\t\tif(!obh.getPackets().get(i).isNullNumber())_object.set"+entry.getKey().substring(0,1).toUpperCase() + entry.getKey().substring(1)+
					"(com.jdb.JdbResources.byteArrayToLong(obh.getPackets().get(i++).getValue()));\n");
				}
				else if((entry.getValue().equals("double") || entry.getValue().equals("java.lang.Double"))
						/* && mappi.get(entry.getKey())!=null*/)
				{
					build.append("\t\tif(!obh.getPackets().get(i).isNullFPN())_object.set"+entry.getKey().substring(0,1).toUpperCase() + entry.getKey().substring(1)+
							"(Double.parseDouble(new String(obh.getPackets().get(i++).getValue())));\n");
				}
				else if((entry.getValue().equals("float") || entry.getValue().equals("java.lang.Float"))
						/* && mappi.get(entry.getKey())!=null*/)
				{
					build.append("\t\tif(!obh.getPackets().get(i).isNullFPN())_object.set"+entry.getKey().substring(0,1).toUpperCase() + entry.getKey().substring(1)+
							"(Float.parseFloat(new String(obh.getPackets().get(i++).getValue())));\n");
				}
				else if((entry.getValue().equals("char") || entry.getValue().equals("java.lang.Character"))
						/* && mappi.get(entry.getKey())!=null*/)
				{
					build.append("\t\tif(!obh.getPackets().get(i).isNullFPN())_object.set"+entry.getKey().substring(0,1).toUpperCase() + entry.getKey().substring(1)+
							"((char)obh.getPackets().get(i++).getValue()[0]);\n");
				}
				else if((entry.getValue().equals("boolean") || entry.getValue().equals("java.lang.Boolean"))
						/* && mappi.get(entry.getKey())!=null*/)
				{
					build.append("\t\tboolean flag = (obh.getPackets().get(i++).getValue()[0]=='1')?true:false;\n");
					build.append("\t\t_object.set"+entry.getKey().substring(0,1).toUpperCase() + entry.getKey().substring(1)+
							"(flag);\n");
				}
			}
		}
		build.append("\t\treturn _object;\n\t}\n}");
		System.out.println(build.toString());
		URL main = TestClient.class.getResource("TestClient.class");
		if (!"file".equalsIgnoreCase(main.getProtocol()))
		  throw new IllegalStateException("Main class is not stored in a file.");
		File path = new File(main.getPath());
		String path1 =main.getPath();
		path1 = path1.substring(0,path1.lastIndexOf("/"));
		path1 = path1.substring(0,path1.lastIndexOf("/"));
		path1 = path1.substring(0,path1.lastIndexOf("/"));
		System.out.println(path1);

		try
		{
			BufferedWriter bw = new BufferedWriter(new FileWriter(path1+"/runtime_"+mappikt2.get("class").replaceAll("\\.", "_")+".java"));
			bw.write(build.toString());
			bw.close();
			
			JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
			int compilationResult = compiler.run(null, null, null, path1+"/runtime_"+mappikt2.get("class").replaceAll("\\.", "_")+".java");
			if(compilationResult == 0){
				System.out.println("Compilation is successful");
			}else{
				System.out.println("Compilation Failed");
				System.exit(0);
			}
			File file = new File(path1+"/runtime_"+mappikt2.get("class").replaceAll("\\.", "_")+".java");
			URL url = new URI("file://"+file.getAbsolutePath()).toURL();          // file:/c:/class/
	        URL[] urls = new URL[]{url};
	    
	        // Create a new class loader with the directory
	        loader = new URLClassLoader(urls);
	    
	        // Load in the class; Class.childclass should be located in
	        // the directory file:/c:/class/user/information
	        Class cls = loader.loadClass("runtime_"+mappikt2.get("class").replaceAll("\\.", "_"));
			
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void initializeRM(Map<String, String> mappikt2) 
	{
		StringBuffer build = new StringBuffer();
		build.append("\n\npublic class runtimerm_"+mappikt2.get("class").replaceAll("\\.", "_")+"\n{\n");
		build.append("\tpublic com.amef.JDBObject getObject("+mappikt2.get("class")+" obh)\n\t{\n\t\tint i=0;\n\t\t"+
				"if(obh==null)return null;\n\t\t" +
				"com.amef.JDBObject _object = new com.amef.JDBObject();\n");
		for (Iterator<Map.Entry<String,String>> iter = mappikt2.entrySet().iterator(); iter.hasNext();)
		{
			Map.Entry<String,String> entry = iter.next();
			if(!entry.getKey().equals("class"))
			{
				if(entry.getValue().equals("java.lang.String"))
				{
					build.append("\t\tif(obh.get"+entry.getKey().substring(0,1).toUpperCase() + entry.getKey().substring(1)+
							"()==null) _object.addNullPacket(com.amef.JDBObject.NULL_STRING);\n\t\telse _object.addPacket(obh.get"
							+entry.getKey().substring(0,1).toUpperCase() + entry.getKey().substring(1)+
							"());\n");
				}
				else if(entry.getValue().equals("java.lang.Integer")
						/* && mappi.get(entry.getKey())!=null*/)
				{
					build.append("\t\tif(obh.get"+entry.getKey().substring(0,1).toUpperCase() + entry.getKey().substring(1)+
							"()==null) _object.addNullPacket(com.amef.JDBObject.NULL_NUMBER);\n\t\telse _object.addPacket(obh.get"
							+entry.getKey().substring(0,1).toUpperCase() + entry.getKey().substring(1)+
							"());\n");
				}
				else if((entry.getValue().equals("int"))
				/* && mappi.get(entry.getKey())!=null*/)
				{
					build.append("\t\t_object.addPacket(obh.get"
							+entry.getKey().substring(0,1).toUpperCase() + entry.getKey().substring(1)+
							"());\n");
				}
				else if((entry.getValue().equals("java.lang.Long"))
						/* && mappi.get(entry.getKey())!=null*/)
				{
					build.append("\t\tif(obh.get"+entry.getKey().substring(0,1).toUpperCase() + entry.getKey().substring(1)+
							"()==null) _object.addNullPacket(com.amef.JDBObject.NULL_NUMBER);\n\t\telse _object.addPacket(obh.get"
							+entry.getKey().substring(0,1).toUpperCase() + entry.getKey().substring(1)+
							"());\n");
				}
				else if((entry.getValue().equals("long"))
				/* && mappi.get(entry.getKey())!=null*/)
				{
					build.append("\t\t_object.addPacket(obh.get"
							+entry.getKey().substring(0,1).toUpperCase() + entry.getKey().substring(1)+
							"());\n");
				}
				else if((entry.getValue().equals("java.lang.Double"))
						/* && mappi.get(entry.getKey())!=null*/)
				{
					build.append("\t\tif(obh.get"+entry.getKey().substring(0,1).toUpperCase() + entry.getKey().substring(1)+
							"()==null) _object.addNullPacket(com.amef.JDBObject.NULL_FPN);\n\t\telse _object.addPacket(obh.get"
							+entry.getKey().substring(0,1).toUpperCase() + entry.getKey().substring(1)+
							"());\n");
				}
				else if((entry.getValue().equals("double") || entry.getValue().equals("float"))
				/* && mappi.get(entry.getKey())!=null*/)
				{
					build.append("\t\t_object.addPacket(obh.get"
							+entry.getKey().substring(0,1).toUpperCase() + entry.getKey().substring(1)+
							"());\n");
				}
				else if((entry.getValue().equals("java.lang.Float"))
						/* && mappi.get(entry.getKey())!=null*/)
				{
					build.append("\t\tif(obh.get"+entry.getKey().substring(0,1).toUpperCase() + entry.getKey().substring(1)+
							"()==null) _object.addNullPacket(com.amef.JDBObject.NULL_FPN);\n\t\telse _object.addPacket(obh.get"
							+entry.getKey().substring(0,1).toUpperCase() + entry.getKey().substring(1)+
							"());\n");
				}
				else if((entry.getValue().equals("java.lang.Character"))
						/* && mappi.get(entry.getKey())!=null*/)
				{
					build.append("\t\tif(obh.get"+entry.getKey().substring(0,1).toUpperCase() + entry.getKey().substring(1)+
							"()==null) _object.addNullPacket(com.amef.JDBObject.NULL_CHAR);\n\t\telse _object.addPacket(obh.get"
							+entry.getKey().substring(0,1).toUpperCase() + entry.getKey().substring(1)+
							"());\n");
				}
				else if((entry.getValue().equals("char"))
				/* && mappi.get(entry.getKey())!=null*/)
				{
					build.append("\t\t_object.addPacket(obh.get"
							+entry.getKey().substring(0,1).toUpperCase() + entry.getKey().substring(1)+
							"());\n");
				}
				else if((entry.getValue().equals("java.lang.Boolean"))
						/* && mappi.get(entry.getKey())!=null*/)
				{
					build.append("\t\tif(obh.is"+entry.getKey().substring(0,1).toUpperCase() + entry.getKey().substring(1)+
							"()==null) _object.addPacket(false);\n\t\telse _object.addPacket(obh.get"
							+entry.getKey().substring(0,1).toUpperCase() + entry.getKey().substring(1)+
							"().booleanValue());\n");
				}
				else if((entry.getValue().equals("boolean"))
				/* && mappi.get(entry.getKey())!=null*/)
				{
					build.append("\t\t_object.addPacket(obh.is"
							+entry.getKey().substring(0,1).toUpperCase() + entry.getKey().substring(1)+
							"());\n");
				}
			}
		}
		build.append("\t\treturn _object;\n\t}\n}");
		System.out.println(build.toString());
		URL main = TestClient.class.getResource("TestClient.class");
		if (!"file".equalsIgnoreCase(main.getProtocol()))
		  throw new IllegalStateException("Main class is not stored in a file.");
		File path = new File(main.getPath());
		String path1 =main.getPath();
		path1 = path1.substring(0,path1.lastIndexOf("/"));
		path1 = path1.substring(0,path1.lastIndexOf("/"));
		path1 = path1.substring(0,path1.lastIndexOf("/"));
		System.out.println(path1);

		try
		{
			BufferedWriter bw = new BufferedWriter(new FileWriter(path1+"/runtimerm_"+mappikt2.get("class").replaceAll("\\.", "_")+".java"));
			bw.write(build.toString());
			bw.close();
			
			JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
			int compilationResult = compiler.run(null, null, null, path1+"/runtimerm_"+mappikt2.get("class").replaceAll("\\.", "_")+".java");
			if(compilationResult == 0){
				System.out.println("Compilation is successful");
			}else{
				System.out.println("Compilation Failed");
				System.exit(0);
			}
			File file = new File(path1+"/runtimerm_"+mappikt2.get("class").replaceAll("\\.", "_")+".java");
			URL url = new URI("file://"+file.getAbsolutePath()).toURL();          // file:/c:/class/
	        URL[] urls = new URL[]{url};
	    
	        // Create a new class loader with the directory
	        loader = new URLClassLoader(urls);
	    
	        // Load in the class; Class.childclass should be located in
	        // the directory file:/c:/class/user/information
	        Class cls = loader.loadClass("runtimerm_"+mappikt2.get("class").replaceAll("\\.", "_"));
			
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static Object getObject(JDBObject obh)
	{
		try
		{
			Class clz = loader.loadClass("runtime_"+mappiktt.get("class").replaceAll("\\.", "_"));		
			Object ins = clz.newInstance();
			Method meth = clz.getMethod("getObject", new Class[]{JDBObject.class});
			Object obj = meth.invoke(ins, new Object[]{obh});
			return obj;
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public static Object getJDBObject(Object obh)
	{
		try
		{
			Class clz = loader.loadClass("runtimerm_"+mappiktt.get("class").replaceAll("\\.", "_"));
			Class clz1 = loader.loadClass(mappiktt.get("class"));
			Object ins = clz.newInstance();
			Method meth = clz.getMethod("getObject", new Class[]{clz1});
			Object obj = meth.invoke(ins, new Object[]{obh});
			return obj;
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}


	protected static List<JDBObject> getAMEFObjects(byte[] data1)
	{
		List<JDBObject> objects = new ArrayList<JDBObject>();
		try
		{
			InputStream jdbin = new ByteArrayInputStream(data1);
			while(jdbin.available()>4)
			{
				byte[] length = new byte[4];
				jdbin.read(length);
				int lengthm = ((length[0] & 0xff) << 24) | ((length[1] & 0xff) << 16)
								| ((length[2] & 0xff) << 8) | ((length[3] & 0xff));
				byte[] data = new byte[lengthm];
				jdbin.read(data);
				objects.add(JdbResources.getDecoder().decodeB(data, false, false));
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
		return objects;
	}
	
	private static boolean bulkexecute(String query,SocketChannel sock,List<Object> obrows, JdbDataReader reader) throws Exception
	{	
		reader.reset();
		List<JDBObject> rows = getRows(obrows);
		JDBObject quer = buildQuery(query,rows);
		byte[] data = JdbResources.getEncoder().encodeB(quer, false);
		ByteBuffer buf = ByteBuffer.allocate(data.length);
		buf.put(data);
		buf.flip();
		try
		{
			sock.write(buf);
			while(!reader.isDone())
			{
				data = reader.read(sock,2);
			}
			/*if(data[0]==1)
				System.out.println("1 Row inserted");
			else
				System.out.println("Insert Failed");*/
		}
		catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}
	static long tim = 0;
	
	
	
	private static boolean execute(String query,SocketChannel sock,Object obj, JdbDataReader reader) throws Exception
	{			
		reader.reset();		
		DMLResReader dmlRead = new DMLResReader(reader,sock);	
		JDBObject row = getRow(obj);
		row.addPacket("11-12-2010 12:12:12");
		row.addPacket(true);
		row.addPacket('Y');
		JDBObject quer = buildQuerys(query,row);
		byte[] data = JdbResources.getEncoder().encodeB(quer, false);
		ByteBuffer buf = ByteBuffer.allocate(data.length);
		buf.put(data);
		buf.flip();
		try
		{
			long ts = System.currentTimeMillis();
			sock.write(buf);	
			/*FutureTask dmlReadTask = new FutureTask(dmlRead);
			execs.execute(dmlReadTask);
							
			dmlReadTask.get();
			execs.shutdown();*/
			while(!reader.isDone())
			{
				data = reader.read(sock,2);
			}
			/*if(data[0]==1)
				System.out.println("1 Row inserted");
			else
				System.out.println("Insert Failed");*/
			tim += (System.currentTimeMillis()-ts);
		}
		catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}
	
	private static List<JDBObject> getRows(List<Object> obj) throws ClassNotFoundException, IllegalArgumentException, SecurityException, IllegalAccessException, NoSuchFieldException
	{
		List<JDBObject> rows = new ArrayList<JDBObject>();
		for (Object object : obj)
		{
			rows.add(getRow(object));
		}
		return rows;
	}
	
	private static JDBObject getRow(Object obj) throws ClassNotFoundException, IllegalArgumentException, SecurityException, IllegalAccessException, NoSuchFieldException
	{
		String clz = obj.getClass().toString().replaceFirst("class ", "");
		JDBObject obja =  new JDBObject();		
		if(mappik.containsValue(clz))
		{
			Class claz = Class.forName(clz);
			for (Map.Entry<String,String> entry : mappik.entrySet())
			{
				if(!entry.getKey().equals("class"))
				{
					Object prop = claz.getDeclaredField(entry.getValue()).get(obj);
					if(prop.getClass().toString().equals("class java.lang.String"))
					{
						obja.addPacket((String)prop,entry.getKey());
					}
					else if(prop.getClass().toString().equals("class java.lang.Integer"))
					{
						obja.addPacket((Integer)prop,entry.getKey());
					}
					else if(prop.getClass().toString().equals("class java.lang.Long"))
					{
						obja.addPacket((Long)prop,entry.getKey());
					}
					else if(prop.getClass().toString().equals("class java.lang.Double"))
					{
						obja.addPacket((Double)prop,entry.getKey());
					}
					else if(prop.getClass().toString().equals("class java.lang.Float"))
					{
						obja.addPacket((Float)prop,entry.getKey());
					}
					else if(prop.getClass().toString().equals("class java.lang.Boolean"))
					{
						obja.addPacket((Boolean)prop,entry.getKey());
					}
					else if(prop.getClass().toString().equals("class java.lang.Character"))
					{
						obja.addPacket((Character)prop,entry.getKey());
					}
				}
			}			
		}
		return obja;
	}
	
	private static JDBObject getUpdateRow(Object obj) throws ClassNotFoundException, IllegalArgumentException, SecurityException, IllegalAccessException, NoSuchFieldException
	{
		String clz = obj.getClass().toString().replaceFirst("class ", "");
		JDBObject obja =  new JDBObject();		
		if(mappik.containsValue(clz))
		{
			Class claz = Class.forName(clz);
			for (Map.Entry<String,String> entry : mappik.entrySet())
			{
				if(!entry.getKey().equals("class"))
				{
					Object prop = claz.getDeclaredField(entry.getValue()).get(obj);
					if(prop!=null && prop.getClass().toString().equals("class java.lang.String"))
					{
						obja.addPacket((String)prop,entry.getKey());
					}
					else if(prop!=null && prop.getClass().toString().equals("class java.lang.Integer"))
					{
						obja.addPacket((Integer)prop,entry.getKey());
					}
					else if(prop!=null && prop.getClass().toString().equals("class java.lang.Long"))
					{
						obja.addPacket((Long)prop,entry.getKey());
					}
					else if(prop!=null && prop.getClass().toString().equals("class java.lang.Double"))
					{
						obja.addPacket((Double)prop,entry.getKey());
					}
					else if(prop!=null && prop.getClass().toString().equals("class java.lang.Float"))
					{
						obja.addPacket((Float)prop,entry.getKey());
					}
					else if(prop!=null && prop.getClass().toString().equals("class java.lang.Boolean"))
					{
						obja.addPacket((Boolean)prop,entry.getKey());
					}
					else if(prop!=null && prop.getClass().toString().equals("class java.lang.Character"))
					{
						obja.addPacket((Character)prop,entry.getKey());
					}
				}
			}			
		}
		return obja;
	}
	
	private static byte[] getRowValue(Object obj) throws ClassNotFoundException, IllegalArgumentException, SecurityException, IllegalAccessException, NoSuchFieldException, AMEFEncodeException, UnsupportedEncodingException
	{
		String clz = obj.getClass().toString().replaceFirst("class ", "");
		byte[][] arrr = new byte[mappik.size()-1][];
		int alllen = 0;
		int pos = 0;
		if(mappik.containsValue(clz))
		{
			Class claz = Class.forName(clz);
			for (Map.Entry<String,String> entry : mappik.entrySet())
			{
				if(!entry.getKey().equals("class"))
				{
					Field prop = claz.getDeclaredField(entry.getValue());
					if(prop.getType().toString().equals("class java.lang.String"))
					{
						if(prop.get(obj)!=null) arrr[pos++] = JdbResources.getEncoder().getPacketValue((String)prop.get(obj));
						else arrr[pos++] = new byte[]{'t',0x00};
						alllen += arrr[pos-1].length;
					}
					else if(prop.getType().toString().equals("int") 
							|| prop.getType().toString().equals("class java.lang.Integer"))
					{
						if(prop.get(obj)!=null)arrr[pos++] = JdbResources.getEncoder().getPacketValue((Integer)prop.get(obj));
						else arrr[pos++] = new byte[]{'n',0x00};
						alllen += arrr[pos-1].length;
					}
					else if(prop.getType().toString().equals("long") 
							|| prop.getType().toString().equals("class java.lang.Long"))
					{
						if(prop.get(obj)!=null)arrr[pos++] = JdbResources.getEncoder().getPacketValue((Long)prop.get(obj));
						else arrr[pos++] = new byte[]{'n',0x00};
						alllen += arrr[pos-1].length;
					}
					else if(prop.getType().toString().equals("double") 
							|| prop.getType().toString().equals("class java.lang.Double"))
					{
						if(prop.get(obj)!=null)arrr[pos++] = JdbResources.getEncoder().getPacketValue((Double)prop.get(obj));
						else arrr[pos++] = new byte[]{'t',0x00};
						alllen += arrr[pos-1].length;
					}
					else if(prop.getType().toString().equals("float") 
							|| prop.getType().toString().equals("class java.lang.Float"))
					{
						if(prop.get(obj)!=null)arrr[pos++] = JdbResources.getEncoder().getPacketValue((Float)prop.get(obj));
						else arrr[pos++] = new byte[]{'t',0x00};
						alllen += arrr[pos-1].length;
					}
					else if(prop.getType().toString().equals("boolean") 
							|| prop.getType().toString().equals("class java.lang.Boolean"))
					{
						if(prop.get(obj)!=null)arrr[pos++] = JdbResources.getEncoder().getPacketValue((Boolean)prop.get(obj));
						else arrr[pos++] = new byte[]{'b','0'};
						alllen += arrr[pos-1].length;
					}
					else if(prop.getType().toString().equals("char") 
							|| prop.getType().toString().equals("class java.lang.Character"))
					{
						if(prop.get(obj)!=null)arrr[pos++] = JdbResources.getEncoder().getPacketValue((Character)prop.get(obj));
						else arrr[pos++] = new byte[]{'c','0'};
						alllen += arrr[pos-1].length;
					}
				}
			}			
		}
		byte[] fin = null;
		pos = 0;
		if(alllen<256)
		{
			fin = new byte[alllen+2];
			fin[0] = (byte)JDBObject.VS_OBJECT_TYPE;
			byte[] len = JdbResources.longToByteArray(alllen, 1);
			System.arraycopy(len, 0, fin, 1, len.length);
			pos = 2;
		}
		else if(alllen<65536)
		{
			fin = new byte[alllen+3];
			fin[0] = (byte)JDBObject.VS_OBJECT_TYPE;
			byte[] len = JdbResources.longToByteArray(alllen, 2);
			System.arraycopy(len, 0, fin, 1, len.length);pos = 3;
		}
		else if(alllen<16777216)
		{
			fin = new byte[alllen+4];
			fin[0] = (byte)JDBObject.VS_OBJECT_TYPE;
			byte[] len = JdbResources.longToByteArray(alllen, 3);
			System.arraycopy(len, 0, fin, 1, len.length);pos = 4;
		}
		else
		{
			fin = new byte[alllen+5];
			fin[0] = (byte)JDBObject.VS_OBJECT_TYPE;
			byte[] len = JdbResources.longToByteArray(alllen, 4);pos = 4;
			System.arraycopy(len, 0, fin, 1, len.length);
		}
		for (int i = 0; i < arrr.length; i++)
		{
			System.arraycopy(arrr[i], 0, fin, pos, arrr[i].length);
			pos += arrr[i].length;
		}
		return fin;
	}

	static class JdbAMEFReader implements Callable
	{
		JdbNewDR reader;
		SocketChannel sock;
		JdbAMEFObjMaker maker;
		public JdbAMEFReader(JdbNewDR reader, SocketChannel sock,
				JdbAMEFObjMaker maker)
		{
			super();
			this.reader = reader;
			this.sock = sock;
			this.maker = maker;
		}

		public Object call() throws Exception
		{	
			long st = System.currentTimeMillis();
			int i = 0;
			reader.reset4();
			outer : while(i<2)
			{
				byte[] data = null;
				while(!reader.isDone())
				{					
					data = reader.readLim4(sock,1);
					//Thread.sleep(1);
					if(reader.isReaderDone())
						break outer;
				}
				reader.reset4();
				maker.addToQ(data);
				i++;
			}
			i=0;
			reader.reset1();
			outer : while(!reader.isReaderDone())
			{
				byte[] data = null;
				while(!reader.isDone())
				{					
					data = reader.readLim1(sock,1);
					//Thread.sleep(0,1);
					if(reader.isReaderDone())
						break outer;
				}
				reader.reset1();
				maker.addToQ(data);
				
				//System.out.println(i++);
			}
			/*JDBObject amef = new JDBObject();
			amef.setName("DONE");
			amef.addPacket(true);
			String dat = JdbResources.getEncoder().encodeS(amef, false);
			dat = dat.substring(4);*/
			maker.addToQ(new byte[]{'F'});
			System.out.println("Time to finish reader task ="+( System.currentTimeMillis() -st));
			return null;
		}
	}
	
	
	static class JdbAMEFReaderf implements Runnable
	{
		JdbDataReader reader;
		SocketChannel sock;
		JdbAMEFObjMakef2 maker;
		public JdbAMEFReaderf(JdbDataReader reader, SocketChannel sock,
				JdbAMEFObjMakef2 maker)
		{
			super();
			this.reader = reader;
			this.sock = sock;
			this.maker = maker;
		}

		public void run()
		{	
			long st = System.currentTimeMillis();			
			try
			{			
				reader.reset();
				
				outer : while(!reader.isReaderDone())
				{
					byte[] data = null;
					while(!reader.isDone())
					{					
						data = reader.read(sock,1);
						LockSupport.parkNanos(1);
						if(reader.isReaderDone())
							break outer;
					}
					reader.reset();
					maker.addToQ(data);
					
				}
			}
			catch(Exception e)
			{}
			maker.addToQ(new byte[]{'F'});
			
			System.out.println("Time to finish reader task ="+( System.currentTimeMillis() -st));
			//return null;
		}
	}
	static class JdbAMEFReaderfo implements Runnable
	{
		JdbDataReader reader;
		SocketChannel sock;
		JdbAMEFObjMakef maker;
		public JdbAMEFReaderfo(JdbDataReader reader, SocketChannel sock,
				JdbAMEFObjMakef maker)
		{
			super();
			this.reader = reader;
			this.sock = sock;
			this.maker = maker;
		}

		public void run()
		{	
			long st = System.currentTimeMillis();			
			try
			{			
				reader.reset();
				
				outer : while(!reader.isReaderDone())
				{
					byte[] data = null;
					while(!reader.isDone())
					{					
						data = reader.read(sock,1);
						LockSupport.parkNanos(1);
						if(reader.isReaderDone())
							break outer;
					}
					reader.reset();
					maker.addToQ(data);
					
				}
			}
			catch(Exception e)
			{}
			maker.addToQ(new byte[]{'F'});
			
			System.out.println("Time to finish reader task ="+( System.currentTimeMillis() -st));
			//return null;
		}
	}
	
	static class JdbAMEFObjMakef// implements Runnable
	{
		private ConcurrentLinkedQueue<byte[]> q;
		public boolean done = false;
		List listobjs = new ArrayList();
		public JdbAMEFObjMakef()
		{
			q = new ConcurrentLinkedQueue<byte[]>();
		}
		public void addToQ(byte[] data)
		{
			q.add(data);
		}
		public Object run()
		{
			long st = System.currentTimeMillis();
			byte[] data = null;
			JDBObject tabDet = null;
			int num = 2;
			String claz = mappik.get("class");
			Class clas = null;
			try
			{
				clas = Class.forName(claz);
			}
			catch(Exception e){}long ttim = 0,obct = 0;
			outer : while(true)
			{
				while((data=q.poll())==null)
				{
					try {
						Thread.sleep(0,1);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				int cnter = 0;
				while(cnter<data.length)
				{
					int lengthm = 0;					
					if(num<=1)
					{
						lengthm = JdbResources.byteArrayToInt(data, cnter, 4);
						cnter += 4;
					}
					else if(data[cnter]=='m')
					{
						lengthm = JdbResources.byteArrayToInt(data, cnter+1, 1) + 2;
					}
					else if(data[cnter]=='q')
					{
						lengthm = JdbResources.byteArrayToInt(data, cnter+1, 2) + 3;
					}
					else if(data[cnter]=='p')
					{
						lengthm = JdbResources.byteArrayToInt(data, cnter+1, 3) + 4;
					}
					else if(data[cnter]=='o')
					{
						lengthm = JdbResources.byteArrayToInt(data, cnter+1, 4) + 5;
					}
					else if(data[cnter]=='F')
					{
						//make1.done = true;
						System.out.println("Done decode");
						break outer;
					}
					
					byte[] interdata = new byte[lengthm];
					System.arraycopy(data, cnter, interdata, 0, lengthm);
					cnter+=(lengthm);
					
					JDBObject obh = null;
					long st1 = System.currentTimeMillis();
					if(num<=1)
					{
						try {
							obh = JdbResources.getDecoder().decodeB(interdata, false, false);
						} catch (AMEFDecodeException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}						
					}
					else
					{
						try {
							obh = JdbResources.getDecoder().decodeB(interdata, false, true);
						} catch (AMEFDecodeException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}ttim += System.currentTimeMillis() - st1; 
					
					
					if(num>1)
					{	
						Object objr = getObject(obh);//buildObject(obh,clas);
						listobjs.add(objr);
					}
					else if(num==0)
					{					
						if(tabDet==null)
						{
							tabDet = obh;
						}
					}
					else if(num==1)
					{
						initializeMapping(obh);
					}
					num++;
					//if(num%1000==0)
					//Thread.sleep(0,1);
				}				
			}System.out.println("time for decode = "+ttim);return listobjs;
		}		
	}
	
	private static final long SLEEP_PRECISION = TimeUnit.MILLISECONDS.toNanos(2);  //TODO: Determine for current machine
	private static final long SPIN_YIELD_PRECISION = TimeUnit.MILLISECONDS.toNanos(10);

	public static void sleepNanos (long nanoDuration) throws InterruptedException {
        final long end = System.nanoTime() + nanoDuration;
        long timeLeft = nanoDuration;
        do {
            LockSupport.parkNanos(1);
            timeLeft = end - System.nanoTime();
        } while (timeLeft > 0);
    }

	
	
	static class JdbAMEFObjMakef2 implements Callable
	{
		private ConcurrentLinkedQueue<byte[]> q;
		private List<byte[]> objects = null;
		public boolean done = false;
		public JdbAMEFObjMakef2()
		{
			q = new ConcurrentLinkedQueue<byte[]>();
			objects = new ArrayList<byte[]>();
		}
		public void addToQ(byte[] data)
		{
			q.add(data);
		}
		public RowObject call() throws Exception
		{
			RowObject rowObject = new SmallRowObject();
			ByteBuffer buffl = null; 
			long st = System.currentTimeMillis();
			byte[] data = null;
			JDBObject tabDet = null;
			int num = 2;
			String claz = mappik.get("class");
			Class clas = null;
			try
			{
				clas = Class.forName(claz);
			}
			catch(Exception e){}long ttim = 0,obct = 0;
			int cuurcnt =0;
			outer : while(true)
			{
				while((data=q.poll())==null)
				{
					Thread.sleep(2);
				}
				
				int cnter = 0;
				rowObject.addData(data);
				while(cnter<data.length)
				{
					int lengthm = 0;					
					if(num<=1)
					{
						lengthm = JdbResources.byteArrayToInt(data, cnter, 4);
						cnter += 4;
					}
					else if(data[cnter]=='m')
					{
						lengthm = JdbResources.byteArrayToInt(data, cnter+1, 1) + 2;
					}
					else if(data[cnter]=='q')
					{
						lengthm = JdbResources.byteArrayToInt(data, cnter+1, 2) + 3;
					}
					else if(data[cnter]=='p')
					{
						lengthm = JdbResources.byteArrayToInt(data, cnter+1, 3) + 4;
					}
					else if(data[cnter]=='o')
					{
						lengthm = JdbResources.byteArrayToInt(data, cnter+1, 4) + 5;
					}
					else if(data[cnter]=='F')
					{
						//rowObject.buffer = buffl;
						//rowObject.objects = objects;
						break outer;
					}
					
					//byte[] interdata = new byte[lengthm];
					//System.arraycopy(data, cnter, interdata, 0, lengthm);
					rowObject.addIndex(rowObject.datasize()-1, cnter, lengthm);
					cnter+=(lengthm);
					
					/*JDBObject obh = null;
					long st1 = System.currentTimeMillis();
					if(num<=1)
					{
						obh = JdbResources.getDecoder().decodeB(interdata, false, false);						
					}
					else
					{
						obh = JdbResources.getDecoder().decodeB(interdata, false, true);
					}ttim += System.currentTimeMillis() - st1; 
					
					
					if(num>1)
					{	
						st1 = System.currentTimeMillis();
						//Object objr = getObject(obh);//buildObject(obh,clas);
						//obct += System.currentTimeMillis() - st1;
						objects.add(obh);
						
						//objects.add(obh);
					}
					else if(num==0)
					{					
						if(tabDet==null)
						{
							tabDet = obh;
						}
					}
					else if(num==1)
					{
						initializeMapping(obh);
					}*/
					num++;
					if(cuurcnt==100000)
					{
						Thread.sleep(10);
						cuurcnt = 0;
					}
				}
				//LockSupport.parkNanos(2000000);
				
			}System.out.println("time for decode = "+ttim+"; time for object conversion = "+rowObject.size());
			return rowObject;
		}		
	}
	
	static class JdbAMEFObjMakef1 implements Callable
	{
		private ConcurrentLinkedQueue<JDBObject> q;
		public boolean done = false;
		private List objects = null;
		public JdbAMEFObjMakef1()
		{
			q = new ConcurrentLinkedQueue<JDBObject>();
			objects = new LinkedList();
		}
		public void addToQ(JDBObject data)
		{
			q.add(data);
		}
		public Object call()
		{
			
			long st = System.currentTimeMillis();
			JDBObject data = null;
			JDBObject tabDet = null;
			int num = 0;
			String claz = mappik.get("class");
			Class clas = null;
			try
			{
				clas = Class.forName(claz);
			}
			catch(Exception e){}long ttim = 0,obct = 0;
			outer : while(!done)
			{
				while((data=q.poll())==null)
				{
					try {
						Thread.sleep(0,1);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}if(done)break;
				}
				long st1 = System.currentTimeMillis();
				Object objr = getObject(data);//buildObject(obh,clas);
				obct += System.currentTimeMillis() - st1;
				objects.add(objr);
			}System.out.println("time for object conversion = "+obct);
			return objects;
		}		
	}
	
	
	static class JdbAMEFObjMaker implements Callable
	{
		private ConcurrentLinkedQueue<byte[]> q;
		private List objects = null;
		public boolean done = false;
		public JdbAMEFObjMaker()
		{
			q = new ConcurrentLinkedQueue<byte[]>();
			objects = new ArrayList();
		}
		public void addToQ(byte[] data)
		{
			q.add(data);
		}
		public Object call() throws Exception
		{
			long st = System.currentTimeMillis();
			byte[] data = null;
			JDBObject tabDet = null;
			int num = 0;
			String claz = mappik.get("class");
			Class clas = null;
			try
			{
				clas = Class.forName(claz);
			}
			catch(Exception e){}
			outer : while(true)
			{
				while((data=q.poll())==null)
				{
					Thread.sleep(0,1);
				}
				JDBObject obh = null;
				if(num<=1)
					obh = JdbResources.getDecoder().decodeB(data, false, false);
				else if(!(data.length==1 && data[0]=='F'))
					obh = JdbResources.getDecoder().decodeB(data, false, true);
				else
					break outer;
				if(num==0)//obh.getName().equals("TABLE_COLUMN_DET"))
				{					
					if(tabDet==null)
					{
						tabDet = obh;	
						/*for (JDBObject b: tabDet.getPackets())
						{
							build.append(b.getName()+"\t");
						}
						build.append("\n");*/
						//initializeMapping(tabDet);
					}
				}
				else if(num==1)//obh.getName().equals("TABLE_CLASS_MAP"))
				{
					/*if(mapping==null)
					{
						JDBObject tabDet1 = obh;
						mapping = new HashMap<String, String>();
						for (JDBObject b: tabDet1.getPackets())
						{
							mapping.put(b.getNameStr(),new String(b.getValue()));
						}
					}*/
					initializeMapping(obh);
				}
				else
				{					
					/*for (JDBObject b: obh.getPackets())
					{
						build.append(b.getValue()+"\t");
					}
					build.append("\n");*/
					//long st = System.currentTimeMillis();
					Object objr = buildObject(obh,clas);
					objects.add(objr);					
					//System.out.println(num);
				}
				num++;
			}
			//System.out.println("Time to finish maker task ="+( System.currentTimeMillis() -st));
			return objects;
		}		
	}
	
	private static List execute(String query,SocketChannel sock,boolean nores, JdbNewDR reader) throws Exception
	{	
		reader.reset4();
		List objects = new LinkedList();
		JDBObject quer = buildQuery(query,null);
		//System.out.println("time reqd for 1 init = "+(System.currentTimeMillis()-stt));
		byte[] data = JdbResources.getEncoder().encodeB(quer, false);
		//System.out.println("time reqd for init = "+(System.currentTimeMillis()-stt));
		ByteBuffer buf = ByteBuffer.allocate(data.length);
		buf.put(data);
		buf.flip();
		int num = 0;
		//StringBuilder build = new StringBuilder();
		JDBObject tabDet = null;
		long rdtim = 0,odtim = 0;
		try
		{
			
			//System.out.println("Time to dispatch = "+(System.currentTimeMillis()-st));
			
			sock.write(buf);			
			JdbAMEFObjMaker maker = new JdbAMEFObjMaker();
			JdbAMEFReader amefRead = new JdbAMEFReader(reader,sock,maker);
			
			FutureTask amefReadTask = new FutureTask(amefRead);
			FutureTask<List> makerTask = new FutureTask<List>(maker);
			//long st = System.currentTimeMillis();
			execs.execute(amefReadTask);
			
			execs.execute(makerTask);
			//st = System.currentTimeMillis();
			amefReadTask.get();
			//System.out.println("Time to actual read = "+(System.currentTimeMillis()-st));
			//st = System.currentTimeMillis();
			objects = makerTask.get();
			//System.out.println("Time to actual make = "+(System.currentTimeMillis()-st));
			execs.shutdown();
			
			/*StringBuilder build = new StringBuilder();
			System.out.println("time reqd for sending query = "+(System.currentTimeMillis()-stt));
			long sttim = System.currentTimeMillis();
			if(nores)
			{							
				outer : while(!reader.isReaderDone())
				{
					long rdst = System.currentTimeMillis();
					while(!reader.isDone())
					{
						data = reader.read(sock,1);
						if(reader.isReaderDone())
							break outer;
					}
					reader.reset();
					long rdent = System.currentTimeMillis();
					rdtim += (rdent-rdst);
					num++;
					long ost = System.currentTimeMillis();
					JDBObject obh = JdbResources.getDecoder().decode(data, false, false);
					if(obh.getName().equals(""))
					{					
						for (JDBObject b: obh.getPackets())
						{
							build.append(b.getValue()+"\t");
						}
						build.append("\n");
						//long st = System.currentTimeMillis();
						Object objr = buildObject(obh);
						objects.add(objr);
						//if(num==3)System.out.println("time reqd for one object rep = "+(System.currentTimeMillis()-st));
					}
					else if(obh.getName().equals("TABLE_COLUMN_DET"))
					{
						if(tabDet==null)
						{
							tabDet = obh;	
							for (JDBObject b: tabDet.getPackets())
							{
								build.append(b.getName()+"\t");
							}
							build.append("\n");
							initializeMapping(tabDet);
						}
					}
					else if(obh.getName().equals("TABLE_CLASS_MAP"))
					{
						if(mapping==null)
						{
							JDBObject tabDet1 = obh;
							mapping = new HashMap<String, String>();
							for (JDBObject b: tabDet1.getPackets())
							{
								mapping.put(b.getName(),b.getValue());
							}
						}
					}
					long oend = System.currentTimeMillis();
					odtim += oend - ost;
					System.out.println(num);
				}
			}*/
		}
		catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("time reqd for comlpetion = "+(System.currentTimeMillis()-stt)
				+"\nSize of objects = "+(objects.size())
				+"\nTime to read = "+(rdtim)
				+"\nTime to object = "+(odtim));
		//System.out.println(build.toString()+"\nSize of objects = "+(num-1));
		return objects;
	}
	private static Object buildObject(JDBObject obh,Class clas)
	{
		Object instance = null;
		//String claz = mappik.get("class");
		try
		{
			//Class clas = Class.forName(claz);
			instance = clas.newInstance();
			for (Iterator<Map.Entry<String,String>> iter = mappik.entrySet().iterator(); iter.hasNext();)
			{
				Map.Entry<String,String> entry = iter.next();
				if(!entry.getKey().equals("class"))
				{
					Field f = clas.getDeclaredField(entry.getValue());
					if(f.getType().toString().equals("class java.lang.String")/* && mappi.get(entry.getKey())!=null*/)
					{
						f.set(instance, new String(obh.getPackets().get(mappi.get(entry.getKey())).getValue()));
					}
					else if((f.getType().toString().equals("int") || f.getType().toString().equals("class java.lang.Integer"))
							/* && mappi.get(entry.getKey())!=null*/)
					{
						f.set(instance, JdbResources.byteArrayToInt(obh.getPackets().get(mappi.get(entry.getKey())).getValue()));
					}
					else if((f.getType().toString().equals("long") || f.getType().toString().equals("class java.lang.Long"))
							/* && mappi.get(entry.getKey())!=null*/)
					{
						f.set(instance, JdbResources.byteArrayToLong(obh.getPackets().get(mappi.get(entry.getKey())).getValue()));
					}
					else if((f.getType().toString().equals("double") || f.getType().toString().equals("class java.lang.Double"))
							/* && mappi.get(entry.getKey())!=null*/)
					{
						f.set(instance, Double.parseDouble(new String(obh.getPackets().get(mappi.get(entry.getKey())).getValue())));
					}
					else if((f.getType().toString().equals("float") || f.getType().toString().equals("class java.lang.Float"))
							/* && mappi.get(entry.getKey())!=null*/)
					{
						f.set(instance, Float.parseFloat(new String(obh.getPackets().get(mappi.get(entry.getKey())).getValue())));
					}
					else if((f.getType().toString().equals("char") || f.getType().toString().equals("class java.lang.Character"))
							/* && mappi.get(entry.getKey())!=null*/)
					{
						f.set(instance, (char)obh.getPackets().get(mappi.get(entry.getKey())).getValue()[0]);
					}
					else if((f.getType().toString().equals("boolean") || f.getType().toString().equals("class java.lang.Boolean"))
							/* && mappi.get(entry.getKey())!=null*/)
					{
						boolean flag = (obh.getPackets().get(mappi.get(entry.getKey())).getValue()[0]=='1')?true:false;
						f.set(instance, flag);
					}
				}
			}			
		}
		/*catch (ClassNotFoundException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		catch (SecurityException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (NoSuchFieldException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (IllegalArgumentException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (IllegalAccessException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (InstantiationException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		return instance;
	}
	static Map<String,Integer> mappi =  new HashMap<String, Integer>();
	static Map<String,String> mapping = null;
	private static void initializeMapping(JDBObject tabdet)
	{
		for (int i = 0; i < tabdet.getPackets().size(); i++)
		{
			mappi.put(tabdet.getPackets().get(i).getNameStr(), i);
		}
	}
	
	private static boolean evaluate(JDBObject tabdet,JDBObject objec)
	{
		if(whrandcls.size()==0)
			return true;
		boolean flag = true;
		if(whrclsInd==null)
		{
			whrclsInd = new HashMap<String, Integer>();
			whrclsIndv = new HashMap<Integer, String>();
			whrclsIndt = new HashMap<Integer, String>();
		}		
		if(whrclsInd.size()==0)
		{
			for (int i = 0; i < tabdet.getPackets().size(); i++)
			{
				if(whrandcls.get(tabdet.getPackets().get(i).getNameStr())!=null)
				{
					whrclsInd.put(tabdet.getPackets().get(i).getNameStr(), i);
					whrclsIndv.put(i, whrandcls.get(tabdet.getPackets().get(i).getNameStr()));
					whrclsIndt.put(i, new String(tabdet.getPackets().get(i).getValue()));
				}
				else if(whrorcls.get(tabdet.getPackets().get(i).getNameStr())!=null)
				{
					whrclsInd.put(tabdet.getPackets().get(i).getNameStr(), i);
					whrclsIndv.put(i, whrorcls.get(tabdet.getPackets().get(i).getNameStr()));
					whrclsIndt.put(i, new String(tabdet.getPackets().get(i).getValue()));
				}
			}
		}			
		for (String name : whrandcls.keySet())
		{
			int num = whrclsInd.get(name);
			flag &= eval(new String(objec.getPackets().get(num).getValue()),whrclsIndv.get(num),whrclsIndt.get(num));
		}
		for (String name : whrorcls.keySet())
		{
			int num = whrclsInd.get(name);
			flag |= eval(new String(objec.getPackets().get(num).getValue()),whrclsIndv.get(num),whrclsIndt.get(num));
		}
		return flag;
	}
	private static boolean eval(String left,String right,String type)
	{
		if(type.equalsIgnoreCase("string"))
		{
			String chr = right.substring(0,1);
			right = right.substring(right.indexOf(chr)+1,right.lastIndexOf(chr));
			return left.equals(right);
		}
		else if(type.equalsIgnoreCase("int"))
		{
			return left.equals(right);
		}
		return false;
	}
	
	private static JDBObject buildQuerys(String query,JDBObject row) throws Exception
	{
		JDBObject object = new JDBObject();
		object.addPacket(query);
		if(query.indexOf("insert ")!=-1)
		{
			object.addPacket(JdbResources.getEncoder().encodeB(row, false),"values");
		}
		return object;
	}
	
	private static JDBObject buildQuery(String query,List<JDBObject> row) throws Exception
	{
		JDBObject object = new JDBObject();
		object.addPacket(query);
		if(query.indexOf("insert ")!=-1)
		{
			/*String columns = "",values = "";
			String subq = query.substring(query.indexOf("(")+1,query.lastIndexOf(")"));
			String temp = query.substring(0,query.indexOf("("));
			String[] qparts = temp.split(" ");
			object.addPacket("insert", "query");			
			if(!qparts[1].equalsIgnoreCase("into"))
			{
				throw new Exception("");				
			}			
			if(!qparts[3].equalsIgnoreCase("values"))
			{
				if(subq.indexOf(" values")!=-1)
				{
					columns = subq.substring(0,subq.indexOf(")"));
					values = subq.substring(subq.indexOf("("));					
				}
				else
					throw new Exception("");				
			}	
			else
				values = subq;
			object.addPacket(columns.trim(), "columns");
			object.addPacket(qparts[2].trim(), "table");*/
			for (int i = 0; i < row.size(); i++)
			{
				object.addPacket(JdbResources.getEncoder().encodeB(row.get(i), false),"values");
			}			
		}
		/*else if(query.indexOf("select ")!=-1)
		{
			String subq = query.indexOf(" where ")!=-1?query.substring(query.indexOf(" where ")+7):"";
			String[] qparts = query.split(" ");
			object.addPacket("select", "query");
			object.addPacket(qparts[1].trim(), "columns");
			if(!qparts[2].equalsIgnoreCase("from"))
			{
				throw new Exception("");				
			}
			object.addPacket(qparts[3].trim(), "table");
			object.addPacket(subq, "wtype");
			object.addPacket("", "values");
		}*/
		return object;
	}

}
