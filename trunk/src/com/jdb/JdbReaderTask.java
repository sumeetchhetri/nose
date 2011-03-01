package com.jdb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import com.amef.AMEFDecodeException;
import com.amef.JDBDecoder;
import com.amef.JDBObject;

public class JdbReaderTask implements Callable
{
	private String fileName;
	private int startFrom;
	public JdbReaderTask(String fileName, int startFrom)
	{
		super();
		this.fileName = fileName;
		this.startFrom = startFrom;
	}
	public List<JDBObject> call() throws Exception
	{
		List<JDBObject> objects = new ArrayList<JDBObject>(1000000);
		int record = 0;
		RandomAccessFile jdbin = null;
		try
		{
			BufferedReader reader = new BufferedReader(new FileReader(fileName));
			reader.skip(startFrom);
			String temp = null,alld="";
			StringBuilder build = new StringBuilder();
			while((temp=reader.readLine())!=null)
			{
				build.append(temp);
			}
			alld = build.toString();
			reader.close();
			JDBDecoder decoder = new JDBDecoder();
			
			int pos = 0;
			while(pos<alld.length())
			{	
				String length = alld.substring(pos,pos+4);
				pos = pos+4;
				int lengthm = ((length.getBytes()[0] & 0xff) << 24) | ((length.getBytes()[1] & 0xff) << 16)
								| ((length.getBytes()[2] & 0xff) << 8) | ((length.getBytes()[3] & 0xff));
				String data = alld.substring(pos,pos+lengthm);
				JDBObject object = decoder.decodeB(data.getBytes(), false, false);
				objects.add(object);			
				record++;
				pos = pos + lengthm;
				if(record==1000000)
					break;
			}
			/*jdbin = new RandomAccessFile(new File(fileName),"r");
			AMEFDecoder decoder = new AMEFDecoder();
			jdbin.seek(startFrom);
			byte[] length = new byte[4];
			List<byte[]> alldats = new ArrayList<byte[]>();
			while(jdbin.read(length)==4)
			{				
				int lengthm = ((length[0] & 0xff) << 24) | ((length[1] & 0xff) << 16)
								| ((length[2] & 0xff) << 8) | ((length[3] & 0xff));
				byte[] data = new byte[lengthm];
				jdbin.read(data);
				alldats.add(data);				
				record++;
				if(record==1000000)
					break;
			}
			for (byte[] bs : alldats)
			{
				JDBObject object = decoder.decode(bs, false, false);
				objects.add(object);
			}*/
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		catch (AMEFDecodeException e)
		{
			e.printStackTrace();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return objects;
	}
}
