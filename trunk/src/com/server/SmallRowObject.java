package com.server;

import java.util.ArrayList;
import java.util.List;

import com.amef.AMEFDecodeException;
import com.amef.JDBObject;
import com.jdb.JdbResources;

public final class SmallRowObject implements RowObject
{
	
	public List<Index> indexes = new ArrayList<Index>();
	public List<byte[]> objects = new ArrayList<byte[]>();
	
	public int size()
	{
		return indexes.size();
	}
	
	public void addIndex(int i,int j,int len)
	{
		indexes.add(new Index(i, j, len));
	}
	
	public Object get(int index)
	{
		byte[] interdata = new byte[indexes.get(index).length];
		System.arraycopy(objects.get(indexes.get(index).i), indexes.get(index).j, interdata, 0, indexes.get(index).length);
		JDBObject obh = null;
		try {
			obh = JdbResources.getDecoder().decodeB(interdata, false, true);
			return TestClient.getObject(obh);
		} catch (AMEFDecodeException e) {
			e.printStackTrace();
		}
		return null;
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

	@Override
	public void spawn() {
		// TODO Auto-generated method stub
		
	}
}
