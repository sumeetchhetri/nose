package com.jdb;

import java.util.ArrayList;
import java.util.List;

public class ConnectionManager
{
	private ConnectionManager()
	{
		connections = new ArrayList<BulkConnection>();
		for (int i = 0; i < 1; i++)
		{
			connections.add(new BulkConnection());
		}
	}
	private volatile static ConnectionManager connectionManager;
	
	private int currentConnections = 0;
	
	private List<BulkConnection> connections;
	
	public static ConnectionManager getConnectionManager()
	{
		if(connectionManager==null) 
		{
			synchronized(ConnectionManager.class)
			{
				if(connectionManager==null)
				{
					connectionManager= new ConnectionManager();
				}
			}
		}
		return connectionManager;
	}
	
	public static BulkConnection getConnection()
	{
		return getConnectionManager().connections.get(0);
	}
	
	protected void shutdown()
	{
		for (BulkConnection conn : connections)
		{
			conn.getFlusher().setShutdown(true);
			while(!conn.getFlusher().isDone())
			{
				try
				{
					Thread.sleep(500);
				}
				catch (InterruptedException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
}
