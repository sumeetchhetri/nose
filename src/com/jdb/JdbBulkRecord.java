package com.jdb;

public class JdbBulkRecord
{
	private String records;
	private boolean complete;
	public String getRecords()
	{
		return records;
	}
	public void setRecords(String records)
	{
		this.records = records;
	}
	public boolean isComplete()
	{
		return complete;
	}
	public void setComplete(boolean complete)
	{
		this.complete = complete;
	}
}
