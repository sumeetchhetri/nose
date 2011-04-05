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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.amef.JDBObject;
import com.jdb.BulkConnection;
import com.jdb.ConnectionManager;
import com.jdb.JdbResources;

public final class JdbWorkerThread implements Runnable
{
	byte[] data;
	SocketChannel channel;
	long time;
	JdbDataReader reader;
	BufferedOutputStream trx_file_st = null;
	String fileName = null;
	BulkConnection con = null;
	public JdbWorkerThread(byte[] data, SocketChannel channel, JdbDataReader reader)
	{
		this.data = data;
		this.channel = channel;
		this.reader = reader;
		con = ConnectionManager.getConnection();
	}
	
	public void run()
	{	
		try
		{
			outer: do
			{
				reader.reset();
				while((data = reader.read(channel,3))==null)
				{
					if(reader.isClosed())
						break outer;
					Thread.sleep(1);
				}
				long st = System.currentTimeMillis();
				try
				{		
					JDBObject query = JdbResources.getDecoder().decodeB(data, false, false);
					String quer = new String(query.getPackets().get(0).getValue());
					if(quer.indexOf("insert into ")!=-1)
					{
						if(trx_file_st!=null)
						{
							byte[] len = JdbResources.intToByteArray(data.length, 4);
							trx_file_st.write(len);
							trx_file_st.write(data);
						}
						else
						{
							insert(quer, query);
						}
						ByteBuffer buf = ByteBuffer.allocate(1);
						buf.put((byte)'T');//buf.put((byte)'R');buf.put((byte)'U');buf.put((byte)'E');
						buf.flip();
						channel.write(buf);	
						buf.clear();	
						//System.out.println("Insert done");
					}
					else if(quer.indexOf("update ")!=-1)
					{
						if(trx_file_st!=null)
						{
							byte[] len = JdbResources.intToByteArray(data.length, 4);
							trx_file_st.write(len);
							trx_file_st.write(data);
						}
						else
						{
							update(quer, query);
						}
						ByteBuffer buf = ByteBuffer.allocate(1);
						buf.put((byte)'T');//buf.put((byte)'R');buf.put((byte)'U');buf.put((byte)'E');
						buf.flip();
						channel.write(buf);	
						buf.clear();						
					}
					else if(quer.indexOf("select ")!=-1)
					{
						st = System.currentTimeMillis();
						String subq = quer.indexOf(" where ")!=-1?quer.substring(quer.indexOf(" where ")+7):"";
						String vals = quer.substring(quer.indexOf("select ")+7,quer.indexOf(" from"));
						String[] qparts = vals.split(",");	
						String tablen = quer.indexOf(" where ")==-1?quer.substring(quer.indexOf("from ")+5)
								:quer.substring(quer.indexOf("from ")+5,quer.indexOf(" where "));		
						Queue<Object> q = new ConcurrentLinkedQueue<Object>();
						System.out.println("time reqd to read query = "+(System.currentTimeMillis()-st));
						st = System.currentTimeMillis();
						con.selectAMEFObjectsb("temp", tablen.trim(), qparts, q, subq, false);
						//System.out.println("time reqd to return from select = "+(System.currentTimeMillis()-st));
						//st = System.currentTimeMillis();
						Object obj = null;
						long pend = 0,cnt = 0;
						long sentpacks = 0;
						long spetw = 0;
						ByteBuffer b = ByteBuffer.allocate(64*1024);
						int count = 0,cuurcnt=0;
						while(pend<100000)
						{
							while((obj=q.poll())==null)
							{
								Thread.sleep(2);
								pend+= 2;
								spetw+=2;
							}
							if(obj!=null)
							{
								pend = 0;
								if(obj instanceof byte[])
								{
									byte[] encData = (byte[])obj;									
									/*ByteBuffer buf = ByteBuffer.allocate(encData.length);
									sentpacks += encData.length;
									buf.put(encData);
									buf.flip();
									channel.write(buf);
									buf.clear();*/
									if(b.position()+encData.length<b.capacity())
									{
										b.put(encData);
										count += encData.length;
									}
									else
									{
										b.flip();
										ByteBuffer b1 = ByteBuffer.allocate(b.limit()+encData.length+4);
										b1.put(JdbResources.intToByteArray(b.limit()+encData.length, 4));
										
										//ByteBuffer b1 = ByteBuffer.allocate(b.limit()+encData.length);
										
										b1.put(b.array(),0,b.limit());
										
										b1.put(encData);
										b1.flip();
										
										channel.write(b1);
										
										b.clear();
										count = 0;
									}
									//System.out.println(cnt++);
								}
								else if(obj instanceof String)
									break;
								if(cuurcnt==100000)
								{
									Thread.sleep(10);
									cuurcnt = 0;
								}
								if(sentpacks>9999999)
								{
									Thread.sleep(0,100);
									sentpacks = 0;
								}
							}
							
						}
						if(b.position()>0)
						{
							//ByteBuffer buf = ByteBuffer.allocate(b.position());
							
							ByteBuffer buf = ByteBuffer.allocate(b.position()+4);
							buf.put(JdbResources.intToByteArray(b.position(), 4));
							
							
							buf.put(b.array(),0,b.position());
							buf.flip();
							channel.write(buf);
							buf.clear();
						}
						byte[] encData = {'F','F','F','F'};
						ByteBuffer buf = ByteBuffer.allocate(4);
						buf.put(encData);
						buf.flip();
						channel.write(buf);	
						buf.clear();
						//System.out.println("Time wasted sleeping = "+spetw*0.000001);
						System.out.println("time reqd to complete select = "+(System.currentTimeMillis()-st)+" Count = "+cnt);					
					}
					else if(quer.equalsIgnoreCase("start transaction"))
					{
						fileName = "trx_"+String.valueOf(System.currentTimeMillis())+".log";
						trx_file_st = new BufferedOutputStream(
								new FileOutputStream(fileName,true),8192);
						byte[] encData = {'T'};
						ByteBuffer buf = ByteBuffer.allocate(1);
						buf.put(encData);
						buf.flip();
						channel.write(buf);	
					}
					else if(quer.equalsIgnoreCase("rollback") && trx_file_st!=null)
					{
						trx_file_st.close();
						trx_file_st = null;
						new File(fileName).delete();
						byte[] encData = {'T'};
						ByteBuffer buf = ByteBuffer.allocate(1);
						buf.put(encData);
						buf.flip();
						channel.write(buf);	
					}
					else if(quer.equalsIgnoreCase("commit") && trx_file_st!=null)
					{
						trx_file_st.flush();
						trx_file_st.close();
						trx_file_st = null;
						new File(fileName+"pos").createNewFile();					
						byte[] length = new byte[4];			
						InputStream jdbin = new FileInputStream(new File(fileName));
						int pos = 0;
						while(jdbin.available()>4)
						{
							OutputStream oos = new FileOutputStream(new File(fileName+"pos"));
							jdbin.read(length);
							int lengthm = ((length[0] & 0xff) << 24) | ((length[1] & 0xff) << 16)
											| ((length[2] & 0xff) << 8) | ((length[3] & 0xff));
							byte[] data1 = new byte[lengthm];
							pos += (4 + lengthm);
							jdbin.read(data1);
							JDBObject trxquery = JdbResources.getDecoder().decodeB(data1, false, false);
							String trxquer = new String(trxquery.getPackets().get(0).getValue());
							if(trxquer.indexOf("insert into ")!=-1)
							{
								insert(trxquer, trxquery);
							}
							else if(trxquer.indexOf("update ")!=-1)
							{
								update(trxquer, trxquery);
							}
							oos.write(pos);
							oos.close();
						}							
						byte[] encData = {'T'};
						ByteBuffer buf = ByteBuffer.allocate(1);
						buf.put(encData);
						buf.flip();
						channel.write(buf);
						jdbin.close();
						new File(fileName).delete();
						new File(fileName+"pos").delete();
						//con.refresh();
					}
					
				}
				catch (InterruptedException e)
				{
					e.printStackTrace();
					break;
				}
				catch (IOException e)
				{
					break;
				}
				catch (Exception e)
				{
					e.printStackTrace();
					break;
				}
				Thread.sleep(1);
			}while(!JdbServer.receivedShutDownRequest());
		}
		catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally
		{
			try
			{
				trx_file_st.flush();
				trx_file_st.close();
				trx_file_st = null;
				new File(fileName).delete();
				new File(fileName+"pos").delete();
			}
			catch (Exception e)
			{
				// TODO Auto-generated catch block
				//e.printStackTrace();
			}			
		}
		System.out.println("Closed thread");
	}
	
	private void insert(String quer,JDBObject query) throws Exception
	{
		String columns = "",values = "";
		String subq = quer.substring(quer.indexOf("(")+1,quer.lastIndexOf(")"));
		String temp = quer.substring(0,quer.indexOf("("));
		String[] qparts = temp.split(" ");
		/*if(!qparts[1].equalsIgnoreCase("into"))
		{
			throw new Exception("");				
		}*/			
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
		//System.out.println("time reqd to prepare insert = "+(System.currentTimeMillis()-st));
		//BulkConnection con = ConnectionManager.getConnection();		
		if(query.getPackets().size()==2)
		{
			con.insert("temp", qparts[2].trim(),query.getPackets().get(1).getValue());
		}
		else
		{
			for (int i = 1; i < query.getPackets().size()-1; i++)
			{					
				con.insert("temp", qparts[2].trim(),query.getPackets().get(i).getValue());
			}
		}
	}
	
	private void update(String quer,JDBObject query) throws Exception
	{
		String tablen = quer.substring(quer.indexOf("update ")+7,quer.indexOf(" set "));
		String subq = quer.indexOf(" where ")!=-1?quer.substring(quer.indexOf(" where ")+7):"";
		int whrend = !subq.equals("")?quer.indexOf(" where "):quer.length()-1;
		String set = quer.indexOf(" set ")!=-1?quer.substring(quer.indexOf(" set ")+5,whrend):"";
		if(set.equals(""))
			return;
		//String[] qparts = set.split(",");
		JDBObject querry = JdbResources.getDecoder().decodeB(query.getPackets().get(1).getValue(), true, false);
		Map<String,byte[]> nvalues = new HashMap<String, byte[]>();
		for (int i = 0; i < querry.getPackets().size(); i++)
		{
			nvalues.put(querry.getPackets().get(i).getNameStr(), querry.getPackets().get(i).getValue());
		}		
		con.update("temp", tablen, nvalues, subq);
	}
}
