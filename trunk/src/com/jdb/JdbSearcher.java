package com.jdb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Queue;
import java.util.concurrent.Callable;
import com.amef.JDBObject;

public class JdbSearcher implements Callable
{
	private String subq,grpbycol;
	private Queue<Object> q;
	private Table table;
	private int index;
	private JDBObject objtab;
	boolean isMemoryDB,distinct,one,aggr;
	String[] qparts;
	public JdbSearcher(String subq, Queue<Object> q, Table table,
			int index, JDBObject objtab,boolean isMemoryDB, 
			String[] qparts, boolean distinct, boolean one, boolean aggr, String grpbycol)
	{
		super();
		this.subq = subq;
		this.q = q;
		this.table = table;
		this.index = index;
		this.objtab = objtab;
		this.isMemoryDB = isMemoryDB;
		this.qparts = qparts;
		this.distinct = distinct;
		this.one = one;
		this.aggr = aggr;
		this.grpbycol = grpbycol;
	}

	public Object call()
	{
		Object rec = 0;
		InputStream jdbin = null;				
		try
		{
			//long st2 = System.currentTimeMillis();		
			if(isMemoryDB)
			{
				//JdbMemoryStore.getJdbMemoryStore();
				//JdbMemoryStore.getJdbMemoryStore().selectFromStore(q,table,index);
				jdbin = new FileInputStream(new File(table.getFileName(index)));
				if(distinct)
					rec = table.getAMEFObjectsDo(q,jdbin,subq,objtab,qparts,one,aggr,grpbycol);
				else
					rec = table.getAMEFObjectso(q,jdbin,subq,objtab,qparts,one,aggr,grpbycol);
			}
			else
			{
				jdbin = new FileInputStream(new File(table.getFileName(index)));
				rec = table.getAMEFObjectsb(q,jdbin,subq,objtab,qparts);	
			}
			//System.out.println("time reqd for search = "+(System.currentTimeMillis()-st2));
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
		return rec;
	}
}
