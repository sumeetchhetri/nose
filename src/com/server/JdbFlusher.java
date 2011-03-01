package com.server;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.amef.AMEFEncodeException;
import com.amef.JDBObject;
import com.jdb.DBManager;
import com.jdb.JdbResources;
import com.jdb.Table;

public class JdbFlusher implements Runnable
{
	private BufferedOutputStream[] bos;
	private boolean commit;
	private Table table;
	private boolean[] flush;
	private volatile boolean shutdown;
	private volatile boolean done;
	public boolean isShutdown()
	{
		return shutdown;
	}
	public void setShutdown(boolean shutdown)
	{
		this.shutdown = shutdown;
	}
	public boolean belongsTo(String tab)
	{
		return (tab!=null && table!=null && tab.equals(table.getFileNameToInsert()));
	}
	
	public boolean isOpenStream()
	{
		return bos!=null;
	}
	
	public void clear()
	{
		try
		{
			if(bos!=null)
			{
				if(bos[0]!=null)
				{
					bos[0].flush();
					bos[0].close();
				}
				if(bos[1]!=null)
				{
					bos[1].flush();
					bos[1].close();
				}
				if(bos[2]!=null)
				{
					bos[2].flush();
					bos[2].close();
				}
			}
			flush[0] = false;
			flush[1] = false;
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		bos=null;
		this.table = null;
	}
	
	public void setStreamDetails(Table table)
	{		
		try
		{
			if(bos!=null)
			{
				if(bos[0]!=null)
				{
					bos[0].flush();
					bos[0].close();
				}
				if(bos[1]!=null)
				{
					bos[1].flush();
					bos[1].close();
				}
				if(bos[2]!=null)
				{
					bos[2].flush();
					bos[2].close();
				}
				flush[0] = false;
				flush[1] = false;
			}
			this.table = table;
			{
				bos = new BufferedOutputStream[3];
				bos[0] = new BufferedOutputStream(new FileOutputStream(table.getFileName(0),true),8192);				
				bos[1] = new BufferedOutputStream(new FileOutputStream(table.getFileName(1),true),8192);
				if(table.getMapping().get("PK")!=null)
				{
					bos[2] = new BufferedOutputStream(new FileOutputStream(table.getIndexFileName(),true),8192);
				}
			}
		}
		catch (FileNotFoundException e)
		{
			try
			{
				new File(table.getFileNameToInsert()).createNewFile();
			}
			catch (IOException e1)
			{
				e1.printStackTrace();
			}				
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}
	
	public JdbFlusher()
	{
		flush = new boolean[2];
	}	
	
	public void writepkbulk(byte[] row,long id)
	{
		int index = table.getFileIndexToInsert();
		try
		{			
			synchronized (table)
			{
				table.incRecords(index);				
				if(table.getMapping().get("PK")!=null)
				{
					if(table.getIndex((long)id)==-1)
					{
						bos[index].write(row);
						ByteBuffer buf = ByteBuffer.allocate(8);
						buf.put(JdbResources.longToByteArray(id,4));						
						buf.put(JdbResources.longToByteArray(table.currpos,4));
						buf.flip();
						bos[2].write(buf.array());
						table.currpos += row.length;
					}
				}
				else
				{
					bos[index].write(row);
				}
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void writepk(byte[] row,long id)
	{
		int index = table.getFileIndexToInsert();
		try
		{			
			synchronized (table)
			{
				table.incRecords(index);				
				if(table.getMapping().get("PK")!=null)
				{
					if(table.getIndex((long)id)==-1)
					{
						bos[index].write(row);
						ByteBuffer buf = ByteBuffer.allocate(8);
						buf.put(JdbResources.longToByteArray(id,4));						
						buf.put(JdbResources.longToByteArray(table.currpos,4));
						buf.flip();
						bos[2].write(buf.array());
						table.currpos += row.length;
					}
				}
				else
				{
					bos[index].write(row);
				}
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		flush[index] = true;
	}
	
	public void writepk(byte[] row,int id)
	{
		int index = table.getFileIndexToInsert();
		try
		{			
			synchronized (table)
			{
				table.incRecords(index);				
				if(table.getMapping().get("PK")!=null)
				{
					if(table.getIndex((long)id)==-1)
					{
						bos[index].write(row);
						ByteBuffer buf = ByteBuffer.allocate(8);
						buf.put(JdbResources.longToByteArray(id,4));
						buf.put(JdbResources.longToByteArray(table.currpos,4));
						buf.flip();
						bos[2].write(buf.array());
						table.currpos += row.length;
					}
				}
				else
				{
					bos[index].write(row);
				}
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		flush[index] = true;
	}
	
	public void writebulk(byte[] row)
	{
		int index = table.getFileIndexToInsert();
		try
		{			
			synchronized (table)
			{
				table.incRecords(index);				
				bos[index].write(row);
				table.currpos += row.length;
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		//flush[index] = true;
	}
	
	public void write(byte[] row)
	{
		int index = table.getFileIndexToInsert();
		try
		{			
			synchronized (table)
			{
				table.incRecords(index);				
				bos[index].write(row);
				table.currpos += row.length;
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		flush[index] = true;
	}
	public void write(byte[] row,int rows)
	{
		int index = table.getFileIndexToInsert();
		try
		{
			synchronized (table)
			{
				table.incRecords(table.getFileIndexToInsert(),rows);
				bos[index].write(row);
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		flush[index] = true;
	}
	public void write(JDBObject row)
	{
		try
		{
			write(JdbResources.getEncoder().encodeB(row, true));
		}
		catch (AMEFEncodeException e)
		{
			e.printStackTrace();
		}
	}
	
	public void flushIt(int index)
	{
		try
		{
			synchronized (table)
			{
				bos[index].flush();
				flush[index] = false;
				if(bos[2]!=null)
					bos[2].flush();
			}
			DBManager.persist();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}
	
	public void run()
	{
		while(!isShutdown())
		{
			while(!flush[0] && !flush[1])
			{
				try
				{
					Thread.sleep(100);
				}
				catch (InterruptedException e)
				{
					e.printStackTrace();
				}
				if(isShutdown())
					break;
			}
			if(table!=null)
			{
				if(flush[0])
					flushIt(0);
				else
					flushIt(1);
			}
		}
		done = true;
	}
	public boolean isCommit()
	{
		return commit;
	}
	public void setCommit(boolean commit)
	{
		this.commit = commit;
	}
	public boolean isDone()
	{
		return done;
	}
	public void setDone(boolean done)
	{
		this.done = done;
	}
}