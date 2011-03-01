package com.server;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.jdb.JdbTask;

public class JdbServer
{
	
	public static ScheduledExecutorService workerThreadPool = null;
	
	//private static List<JdbWorkerThread> threadList = null;
	
	//private static List<Thread> threadListt = null;
	
	/**
	 * The Control file for Shutdown
	 */
	private static File controlFile = null;
	
	/**
	 * The List of All Selectors
	 */
	private List<Selector> selectors = new ArrayList<Selector>();

	public static boolean receivedShutDownRequest()
	{
		if(controlFile ==null)
			return false;
		return !controlFile.exists();
	}
	
	class ShutdownRequest implements Runnable
	{
		public void run()
		{
			try
			{
				while (!receivedShutDownRequest())
				{
					Thread.sleep(1000);
				}
				/*If controlFile doesn't exist then wakeip the 
				 * selector so that the server can be shutdown*/
				selectors.get(0).wakeup();
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}
	}	
	
	class SocketListener implements Runnable
	{
		private Selector sselector = null;
		private ConcurrentLinkedQueue<SocketChannel> sockq = null;
		public void addToRegisterQ(SocketChannel chan)
		{
			this.sockq.add(chan);
		}
		public SocketListener(Selector selector)
		{
			sselector = selector;
			sockq = new ConcurrentLinkedQueue<SocketChannel>();
		}
		public void run()
		{
			SelectionKey skey = null;
			try
			{
				long tir = 0;
				while (!receivedShutDownRequest())
				{
					
					SocketChannel chan = null;
					
					while((chan=sockq.poll())!=null)
					{
						JdbDataReader reader = new JdbDataReader();						
						reader.reset();						
						chan.register(sselector, SelectionKey.OP_READ, reader);	
					}
					sselector.select(10);	
					
					for (Iterator<SelectionKey> iter = sselector.selectedKeys().iterator(); iter.hasNext();)
					{						
						skey = iter.next();
						JdbDataReader reader = (JdbDataReader)skey.attachment();
						if ((skey.readyOps() & SelectionKey.OP_READ) == SelectionKey.OP_READ)
						{	
							try
							{
								byte[] data = reader.read((SocketChannel)skey.channel(),0);
								if (data != null) 
								{	
									skey.cancel();
									((SocketChannel)skey.channel()).configureBlocking(true);
									JdbWorkerThread task = new JdbWorkerThread(data,(SocketChannel)skey.channel(),reader);
									workerThreadPool.execute(task);
									reader.reset();
								}
								else if(data==null && reader.isDone())
								{
									skey.cancel();
									try
									{
										skey.channel().close();
									}
									catch (IOException e1)
									{}
								}
							}
							catch (Exception e)
							{
								skey.cancel();
								try
								{
									skey.channel().close();
								}
								catch (IOException e1)
								{}
								e.printStackTrace();
							}
						}
						iter.remove();
					}
				}
			}
			catch (Exception e)
			{
				if(skey!=null)
				{ 
					skey.cancel();
					try
					{
						skey.channel().close();
					}
					catch (IOException e1)
					{}
					e.printStackTrace();
				}
			}
		}
	}
	
	public void run() throws IOException
	{
		ServerSocketChannel server = null;
		try
		{
			controlFile = new File("jdb_server.cntrl");		
			BufferedWriter buffw = new BufferedWriter(new FileWriter(controlFile));
			buffw.write("file");
			buffw.close();
			int parSockListeners = 1;		
			int threads = 5;
			int portNumber = 7001;
			
			new Thread(new ShutdownRequest(),"Shutdown Thread").start();
			
			/*threadList = new ArrayList<JdbWorkerThread>();
			threadListt = new ArrayList<Thread>();
			for (int i = 0; i < threads; i++)
			{	
				JdbWorkerThread thre = new JdbWorkerThread();
				Thread thr = new Thread(thre);
				thr.start();
				threadListt.add(thr);
				threadList.add(thre);
			}*/
			workerThreadPool = Executors.newScheduledThreadPool(threads);
			
			server = ServerSocketChannel.open();
	
			Selector selector = Selector.open();
			selectors.add(selector);		
			server.socket().bind(new InetSocketAddress(portNumber ));
			server.configureBlocking(false);
			server.register(selector, SelectionKey.OP_ACCEPT);
	
			List<SocketListener> clientHandlers = new ArrayList<SocketListener>();
			for (int i = 0; i < parSockListeners; i++)
			{
				Selector sel = Selector.open();
				selectors.add(sel);
				SocketListener clHandler = new SocketListener(sel);
				clientHandlers.add(clHandler);
				new Thread(clHandler,"Parent Socket Listener "+i).start();
			}
			
			while (!receivedShutDownRequest())
			{
				selector.select();
				int sockListener = 1;
				for (Iterator<SelectionKey> iter = selector.selectedKeys().iterator(); iter
						.hasNext();)
				{
					SelectionKey skey = iter.next();
					if (skey.isValid() && skey.isAcceptable() && (skey.readyOps() & SelectionKey.OP_ACCEPT) == SelectionKey.OP_ACCEPT)
					{
						if (sockListener == parSockListeners)
						{
							sockListener = 1;
						}
						
						SocketChannel socket = server.accept();					
						socket.configureBlocking(false);
						clientHandlers.get(sockListener-1).addToRegisterQ(socket);					
						sockListener++;
					}
					iter.remove();
				}
			}			
		}
		catch (Exception throwable)
		{
			throwable.printStackTrace();
		}
		finally
		{			
			for (int i = 0; i < selectors.size(); i++)
			{
				if (selectors.get(i) != null && selectors.get(i).isOpen())
				{
					try
					{
						selectors.get(i).wakeup();
						selectors.get(i).close();
					}
					catch (Exception e)
					{
						e.printStackTrace();
					}
				}
			}
			if (server != null && server.isOpen())
			{
				try
				{
					server.close();
				}
				catch (IOException e)
				{
					e.printStackTrace();
				}
			}
		}
	}
	
	public static void main(String[] args)
	{
		try
		{
			new JdbServer().run();
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
