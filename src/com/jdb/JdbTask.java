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

import java.nio.channels.SocketChannel;

public final class JdbTask
{
	byte[] data;
	SocketChannel channel;
	
	public JdbTask(byte[] data, SocketChannel channel)
	{
		this.data = data;
		this.channel = channel;
	}

	public SocketChannel getChannel()
	{
		return channel;
	}

	public void setChannel(SocketChannel channel)
	{
		this.channel = channel;
	}

	public byte[] getData()
	{
		return data;
	}

	public void setData(byte[] data)
	{
		this.data = data;
	}
	
}
