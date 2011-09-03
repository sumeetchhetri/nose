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
import com.amef.AMEFDecoder;
import com.amef.AMEFObject;

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
	public List<AMEFObject> call() throws Exception
	{
		List<AMEFObject> objects = new ArrayList<AMEFObject>(1000000);
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
			AMEFDecoder decoder = new AMEFDecoder();
			
			int pos = 0;
			while(pos<alld.length())
			{	
				String length = alld.substring(pos,pos+4);
				pos = pos+4;
				int lengthm = ((length.getBytes()[0] & 0xff) << 24) | ((length.getBytes()[1] & 0xff) << 16)
								| ((length.getBytes()[2] & 0xff) << 8) | ((length.getBytes()[3] & 0xff));
				String data = alld.substring(pos,pos+lengthm);
				AMEFObject object = decoder.decodeB(data.getBytes(), false, false);
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
				AMEFObject object = decoder.decode(bs, false, false);
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
