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
