package com.jdb;

import java.nio.channels.SocketChannel;

public class JdbTask
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
