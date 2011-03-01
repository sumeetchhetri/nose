package com.jdb;

import com.amef.JDBDecoder;
import com.amef.JDBEncoder;

public class JdbResources
{
	private JdbResources()
	{
		encoder = new JDBEncoder();
		decoder = new JDBDecoder();
	}
	private volatile static JdbResources jdbResources;
	
	private JDBEncoder encoder;
	
	private JDBDecoder decoder;
	
	private static JdbResources get()
	{
		if(jdbResources==null) 
		{
			synchronized(JdbResources.class)
			{
				if(jdbResources==null)
				{
					jdbResources= new JdbResources();
				}
			}
		}
		return jdbResources;
	}
	
	public static JDBEncoder getEncoder()
	{
		return get().encoder;
	}
	
	public static JDBDecoder getDecoder()
	{
		return get().decoder;
	}
	
	public static byte[] longToByteArray(long l,int ind) 
	{
		byte[] result = new byte[ind];
		for (int i = 0; i<ind; i++)
		{
			int offset = (ind - 1 - i) * 8;
            result[i] = (byte) ((l >>> offset) & 0xFF);
		}
        return result;
    }
	
	public static String longToByteArrayS(long l,int ind) 
	{
		byte[] result = new byte[ind];
		for (int i = 0; i<ind; i++)
		{
			int offset = (ind - 1 - i) * 8;
            result[i] = (byte) ((l >>> offset) & 0xFF);
		}
        return new String(result);
    }
	
	public static byte[] intToByteArray(int l,int ind) 
	{
		byte[] result = new byte[ind];
		for (int i = 0; i<ind; i++)
		{
			int offset = (ind - 1 - i) * 8;
            result[i] = (byte) ((l >>> offset) & 0xFF);
		}
        return result;
    }
	
	public static byte[] intToByteArrayWI(int l) 
	{
		int ind = 1;
		if(l<256)
			ind =1;
		else if(l<65536)
			ind = 2;
		else if(l<16777216)
			ind =3;
		else
			ind =4;
		byte[] result = new byte[ind];
		for (int i = 0; i<ind; i++)
		{
			int offset = (ind - 1 - i) * 8;
            result[i] = (byte) ((l >>> offset) & 0xFF);
		}
        return result;
    }
	
	public static int byteArrayToInt(byte[] l) 
	{
		int t = 0;
		int ind = l.length;
        for (int i = 0; i < l.length; i++)
		{
        	int offset = (ind -1 - i) * 8;
        	t += (l[i] & 0x000000FF) << offset; 
		}
        return t;
    }
	
	public static int byteArrayToInt(byte[] l,int off,int ind) 
	{
		int t = 0;
		for (int i = 0; i < ind; i++)
		{
        	int offset = (ind -1 - i) * 8;
        	t += (l[off+i] & 0x000000FF) << offset; 
		}
        return t;
    }
	
	public static long byteArrayToLong(byte[] l) 
	{
		long t = 0;
		int ind = l.length;
        for (int i = 0; i < l.length; i++)
		{
        	int offset = (ind -1 - i) * 8;
        	t += (l[i] & 0x000000FF) << offset; 
		}
        return t;
    }
	public static long byteArrayToLong(byte[] l,int off,int ind) 
	{
		long t = 0;
		for (int i = 0; i < ind; i++)
		{
        	int offset = (ind -1 - i) * 8;
        	t += (l[off+i] & 0x000000FF) << offset; 
		}
        return t;
    }
	public static long byteArrayToLong(byte[] l,int ind) 
	{
		long t = 0;
		for (int i = 0; i < ind; i++)
		{
        	int offset = (ind -1 - i) * 8;
        	t += (l[i] & 0x000000FF) << offset; 
		}
        return t;
    }

	public static String intToByteArrayS(int l, int ind)
	{
		byte[] result = new byte[ind];
		for (int i = 0; i<ind; i++)
		{
			int offset = (ind - 1 - i) * 8;
            result[i] = (byte) ((l >>> offset) & 0xFF);
		}
        return new String(result);
    }
	
	
}
