package com.jdb;

import java.nio.channels.SocketChannel;

public interface Reader
{

	boolean isDone();

	byte[] read(SocketChannel sock, int i) throws Exception ;

	void reset();

	boolean isReaderDone();

}
