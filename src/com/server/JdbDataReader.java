package com.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import com.jdb.Reader;

class JdbDataReader implements Reader
{
	public JdbDataReader()
	{
		state = 0;
		buf.limit(4);
		setDone(false);
		setReaderDone(false);
		setReadStart(false);
	}
	private ByteBuffer buf = ByteBuffer.allocate(16000000);
	private boolean done = false;
	private boolean readerDone;
	private boolean readStart = false;
	public boolean isReadStart()
	{
		return readStart;
	}
	public void setReadStart(boolean readStart)
	{
		this.readStart = readStart;
	}
	public boolean isDone()
	{
		return done;
	}
	public void setDone(boolean done)
	{
		this.done = done;
	}
	private int state = 0;	
	/** Read from the channel until we have a full message. */
	public byte[] read(SocketChannel chan,int reader) throws Exception 
	{
		byte[] data = null;
		if (state == 0) 
		{
			if (chan.read(buf) == -1) 
			{
				setDone(true);
			} 
			else if (buf.remaining() == 0) {
				//read header
				state++;
				buf.clear();
				if(reader==1)
				{
					if(buf.get(0)=='F' && buf.get(1)=='F' && buf.get(2)=='F' && buf.get(3)=='F')
					{
						readerDone = true;
						setDone(true);
						return data;
					}
				}
				else if(reader==2)
				{
					if(buf.get(0)=='T')
					{
						readerDone = true;
						setDone(true);
						return new byte[]{1};
					}
					else
					{
						readerDone = true;
						setDone(true);
						return new byte[]{0};
					}
				}
				buf.limit(((buf.get(0) & 0xff) << 24) | ((buf.get(1) & 0xff) << 16)
						| ((buf.get(2) & 0xff) << 8) | ((buf.get(3) & 0xff)));
				if (state == 1) 
				{
					if (chan.read(buf) == -1)
					{
						setDone(true);
					} 
					else if (buf.remaining() == 0) 
					{
						state++;
						buf.flip();
						data = new byte[buf.limit()];
						System.arraycopy(buf.array(), 0, data, 0, data.length);
						setDone(true);
					}
					else if(reader==3)
					{
						while(buf.remaining() > 0)
						{
							chan.read(buf);
						}
						data = new byte[buf.limit()];
						System.arraycopy(buf.array(), 0, data, 0, data.length);
						setDone(true);
					}
				}
			}
		} 
		else if (state == 1) 
		{
			if (chan.read(buf) == -1) 
			{
				setDone(true);
			} 
			else if (buf.remaining() == 0) 
			{
				state++;
				buf.flip();
				data = new byte[buf.limit()];
				System.arraycopy(buf.array(), 0, data, 0, data.length);
				setDone(true);
			}
		}
		return data;
	}

	public void reset() 
	{
		state = 0;
		buf.clear();
		buf.limit(4);
		setDone(false);
		setReaderDone(false);
		setReadStart(false);
	}
	public boolean isReaderDone()
	{
		return readerDone;
	}
	public void setReaderDone(boolean readerDone)
	{
		this.readerDone = readerDone;
	}
}
