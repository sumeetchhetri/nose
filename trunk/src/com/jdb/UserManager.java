package com.jdb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;

@SuppressWarnings("unchecked")
public class UserManager
{
	private volatile static UserManager userManager;
	private HashMap<String,User> users;
	
	public static UserManager getUserManager()
	{
		if(userManager==null) 
		{
			synchronized(UserManager.class)
			{
				if(userManager==null)
				{
					userManager= new UserManager();
				}
			}
		}
		return userManager;
	}
	
	protected void loadUsers()
	{
		try
		{
			users = (HashMap<String,User>)new ObjectInputStream(
									new FileInputStream(new File(""))).readObject();
		}
		catch (FileNotFoundException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (ClassNotFoundException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
