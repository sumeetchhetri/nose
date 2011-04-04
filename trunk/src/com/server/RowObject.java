package com.server;


public interface RowObject {
	
	static class Index
	{
		public Index(int i,int j,int len)
		{
			this.i = i;
			this.j = j;
			this.length = len;
		}
		public int i,j,length;
	}
	
	public int size();
	
	public int datasize();
	
	public void addIndex(int i,int j,int len);
	
	public Object get(int index);
	
	public void addData(byte[] data);
}
