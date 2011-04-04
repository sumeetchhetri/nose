package com.server;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.amef.AMEFDecodeException;
import com.amef.JDBObject;
import com.jdb.JdbResources;

@SuppressWarnings({"rawtypes","unchecked"})
public class HugeRowObject implements RowObject
{
	private boolean stthr = false;
	public boolean done = false;
	private int counter = 0;
	public ConcurrentLinkedQueue<Index> indexes = new ConcurrentLinkedQueue<Index>();
	
	public List acObjects = new ArrayList();
	public List<byte[]> objects = new ArrayList<byte[]>();
	
	public int size()
	{
		return indexes.size()+acObjects.size();
	}
	
	public void addIndex(int i,int j,int len)
	{
		counter ++;
		indexes.add(new Index(i, j, len));
		if(!stthr)
		{
			new Thread(new Runnable() {
				
				@Override
				public void run() {
					long st = System.currentTimeMillis();
					while(counter>acObjects.size())
					{
						Index data = null;
						while((data=indexes.poll())==null)
						{
							try {
								Thread.sleep(0,1);
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
						byte[] interdata = new byte[data.length];
						System.arraycopy(objects.get(data.i), data.j, interdata, 0, data.length);
						JDBObject obh = null;
						try {
							obh = JdbResources.getDecoder().decodeB(interdata, false, true);
							acObjects.add(TestClient.getObject(obh));
						} catch (AMEFDecodeException e) {
							e.printStackTrace();
						}
					}
					System.out.println("Done all decoding in "+(System.currentTimeMillis()-st));
				}
			}).start();
		}
		stthr = true;
	}
	
	public Object get(int index)
	{
		if(index>indexes.size()+acObjects.size())
			return null;
		while(acObjects.size()<index)
		{
			try {
				Thread.sleep(0,1);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return acObjects.get(index);
	}

	@Override
	public void addData(byte[] data) {
		objects.add(data);
	}
	
	@Override
	public int datasize() {
		// TODO Auto-generated method stub
		return objects.size();
	}
}
