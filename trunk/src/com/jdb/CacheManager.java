package com.jdb;

public class CacheManager
{
	private volatile static CacheManager cacheManager;
	
	public static CacheManager getCacheManager()
	{
		if(cacheManager==null) 
		{
			synchronized(CacheManager.class)
			{
				if(cacheManager==null)
				{
					cacheManager= new CacheManager();
				}
			}
		}
		return cacheManager;
	}
}
