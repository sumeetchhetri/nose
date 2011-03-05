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
package com.amef;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import com.jdb.JdbResources;

/**
 * @author sumeet.chhetri
 * The Automated Message Exchange Format Object type
 * Is a wrapper for basic as well as complex object heirarchies
 * can be a string, number, date, boolean, character or any complex object
 * Every message consists of only one JDBObjectNew *
 */
public class JDBObject
{
	public static final char NULL_STRING = 'a';
	
	public static final char NULL_NUMBER = 'g';
	
	public static final char NULL_DATE = 'j';
	
	public static final char NULL_FPN = 'k';
	
	public static final char NULL_BOOL = 'v';
	
	public static final char NULL_CHAR = 'z';
	
	/*The Date type*/
	public static final char DATE_TYPE = 'd';
	
	/*The 4GB string type*/
	public static final char STRING_TYPE = 's';
	
	/*The max 256 length string type*/
	public static final char STRING_256_TYPE = 't';
	
	/*The max 65536 length string type*/
	public static final char STRING_65536_TYPE = 'h';
	
	public static final char STRING_16777216_TYPE = 'y';
		
	/*The boolean type*/
	public static final char BOOLEAN_TYPE = 'b';
	
	/*The character type*/
	public static final char CHAR_TYPE = 'c';
	
	/*The Number types*/
	public static final char VERY_SMALL_INT_TYPE = 'n';
	
	public static final char SMALL_INT_TYPE = 'w';
	
	public static final char BIG_INT_TYPE = 'r';
	
	public static final char INT_TYPE = 'i';
	
	public static final char VS_LONG_INT_TYPE = 'f';
	
	public static final char S_LONG_INT_TYPE = 'x';
	
	public static final char B_LONG_INT_TYPE = 'e';
	
	public static final char LONG_INT_TYPE = 'l';
	
	public static final char DOUBLE_FLOAT_TYPE = 'u';
	
	/*The Object type*/
	public static final char VS_OBJECT_TYPE = 'm';
	
	/*The Object type*/
	public static final char S_OBJECT_TYPE = 'q';
	
	/*The Object type*/
	public static final char B_OBJECT_TYPE = 'p';
	
	/*The Object type*/
	public static final char OBJECT_TYPE = 'o';
	
	/*The type of the Object can be string, number, date, boolean, character or any complex object*/
	private char type;
	
	/*The name of the Object if required can be used to represent object properties*/
	private String name;
	
	/*The Length of the Object value*/
	private int length;
	
	/*The Length of the Object value*/
	private int namedLength = 2;
	
	/*The Object value in String format*/
	private byte[] value;
	
	/*The properties of a complex object*/
	private List<JDBObject> packets;
	
	/**
	 * @return Array of JDBObjectNew	 
	 *  
	 */
	
	public static char getEqvNullType(char type)
	{
		if(type=='s' || type=='t' || type=='h' || type=='y')
			return NULL_STRING;
		else if(type=='n' || type=='w' || type=='r' || type=='q'
				|| type=='f' || type=='x' || type=='e' || type=='l')
			return NULL_NUMBER;
		else if(type=='d')
			return NULL_DATE;
		else if(type=='b')
			return NULL_BOOL;
		else if(type=='c')
			return NULL_CHAR;
		else 
			return 0;
	}
	
	public JDBObject[] getObjects()
	{
		return packets.toArray(new JDBObject[packets.size()]);
	}
	
	public void clear()
	{
		this.packets.clear();
	}
	
	/*Create a new AMEF object which will initilaize the values*/
	public JDBObject()
	{
		type = OBJECT_TYPE;
		length = 0;
		name = "";
		packets = new ArrayList<JDBObject>();
	}
	
	public void addNullPacket(char type)
	{
		JDBObject JDBObjectNew = new JDBObject();
		JDBObjectNew.namedLength += 1;
		length += 1;
		namedLength += 3;
		JDBObjectNew.type = type;
		packets.add(JDBObjectNew);
	}
	
	public void addNullPacket(char type,String name)
	{
		JDBObject JDBObjectNew = new JDBObject();
		JDBObjectNew.name = name;		
		namedLength += name.length();
		JDBObjectNew.namedLength += name.length() + 1;
		length += 1;
		namedLength += 3;
		JDBObjectNew.type = type;
		packets.add(JDBObjectNew);
	}
	
	/**
	 * @param string
	 * @param name
	 * Add a String property to an Object
	 */
	public void addPacket(String string,String name)
	{
		JDBObject JDBObjectNew = addPacket(string);
		JDBObjectNew.name = name;
		//length += name.length();
		namedLength += name.length();
		JDBObjectNew.namedLength += name.length();
	}
	/**
	 * @param string
	 * Add a String property to an Object
	 */
	public JDBObject addPacket(String string)
	{
		JDBObject JDBObjectNew = new JDBObject();		
		JDBObjectNew.name = "";
		
		if(string.length()<=256)
		{
			JDBObjectNew.type = STRING_256_TYPE;
			length += string.length() + 2;
			namedLength += string.length() + 4;
			JDBObjectNew.namedLength = string.length() + 2;
		}
		else if(string.length()<=65536)
		{
			JDBObjectNew.type = STRING_65536_TYPE;
			length += string.length() + 3;
			namedLength += string.length() + 5;
			JDBObjectNew.namedLength = string.length() + 3;
		}
		else if(string.length()<=16777216)
		{
			JDBObjectNew.type = STRING_16777216_TYPE;
			length += string.length() + 4;
			namedLength += string.length() + 6;
			JDBObjectNew.namedLength = string.length() + 4;
		}
		else
		{
			JDBObjectNew.type = STRING_TYPE;
			length += string.length() + 5;
			namedLength += string.length() + 7;
			JDBObjectNew.namedLength = string.length() + 5;
		}
		JDBObjectNew.length = string.length();		
		JDBObjectNew.value = string.getBytes();
		packets.add(JDBObjectNew);
		return JDBObjectNew;
	}
	
	/**
	 * @param string
	 * @param name
	 * Add a String property to an Object
	 */
	public void addPacket(byte[] string,String name)
	{
		JDBObject JDBObjectNew = addPacket(string);
		JDBObjectNew.name = name;
		//length += name.length();
		namedLength += name.length();
		JDBObjectNew.namedLength += name.length();
	}
	/**
	 * @param string
	 * Add a String property to an Object
	 */
	public JDBObject addPacket(byte[] string)
	{
		JDBObject JDBObjectNew = new JDBObject();		
		JDBObjectNew.name = "";
		
		if(string.length<=256)
		{
			JDBObjectNew.type = STRING_256_TYPE;
			length += string.length + 2;
			namedLength += string.length + 4;
			JDBObjectNew.namedLength = string.length + 2;
		}
		else if(string.length<=65536)
		{
			JDBObjectNew.type = STRING_65536_TYPE;
			length += string.length + 3;
			namedLength += string.length + 5;
			JDBObjectNew.namedLength = string.length + 3;
		}
		else if(string.length<=16777216)
		{
			JDBObjectNew.type = STRING_16777216_TYPE;
			length += string.length + 4;
			namedLength += string.length + 6;
			JDBObjectNew.namedLength = string.length + 4;
		}
		else
		{
			JDBObjectNew.type = STRING_TYPE;
			length += string.length + 5;
			namedLength += string.length + 7;
			JDBObjectNew.namedLength = string.length + 5;
		}
		JDBObjectNew.length = string.length;		
		JDBObjectNew.value = string;
		packets.add(JDBObjectNew);
		return JDBObjectNew;
	}
	
	/**
	 * @param bool
	 * @param name
	 * Add a boolean property to an Object
	 */
	public void addPacket(boolean bool,String name)
	{
		JDBObject JDBObjectNew = addPacket(bool);
		JDBObjectNew.name = name;
		//length += name.length();
		namedLength += name.length();
		JDBObjectNew.namedLength += name.length();
	}
	/**
	 * @param bool
	 * Add a boolean property to an Object
	 */
	public JDBObject addPacket(boolean bool)
	{
		JDBObject JDBObjectNew = new JDBObject();
		JDBObjectNew.type = BOOLEAN_TYPE;
		JDBObjectNew.name = "";
		JDBObjectNew.length = 1;
		if(bool==true)
		{			
			JDBObjectNew.value = new byte[]{'1'};
		}
		else
		{
			JDBObjectNew.value = new byte[]{'0'};		
		}		
		packets.add(JDBObjectNew);
		length += 2;
		namedLength += 4;
		JDBObjectNew.namedLength = 2;
		return JDBObjectNew;
	}
	
	public void addPacket(char chr,String name)
	{
		JDBObject JDBObjectNew = addPacket(chr);
		JDBObjectNew.name = name;
		//length += name.length();
		namedLength += name.length();
		JDBObjectNew.namedLength += name.length();
	}
	/**
	 * @param bool
	 * Add a boolean property to an Object
	 */
	public JDBObject addPacket(char chr)
	{
		JDBObject JDBObjectNew = new JDBObject();
		JDBObjectNew.type = 'c';
		JDBObjectNew.name = "";
		JDBObjectNew.length = 1;
		JDBObjectNew.value = new byte[]{(byte)chr};	
		packets.add(JDBObjectNew);
		length += 2;
		namedLength += 4;
		JDBObjectNew.namedLength = 2;
		return JDBObjectNew;
	}
	
	/**
	 * @param lon
	 * @param name
	 * Add a long property to an Object
	 */
	public void addPacket(long lon,String name)
	{
		JDBObject JDBObjectNew = addPacket(lon);
		JDBObjectNew.name = name;
		//length += name.length();
		namedLength += name.length();
		JDBObjectNew.namedLength += name.length();
	}
	/**
	 * @param lon
	 * Add a long property to an Object
	 */
	public JDBObject addPacket(long lon)
	{
		JDBObject JDBObjectNew = new JDBObject();
		if(lon<256)
		{
			JDBObjectNew.type = VERY_SMALL_INT_TYPE;
			JDBObjectNew.value = JdbResources.longToByteArray(lon, 1);
			length += 2;
			namedLength += 4;
			JDBObjectNew.namedLength  = 2;
			JDBObjectNew.length = 1;
		}
		else if(lon<65536)
		{
			JDBObjectNew.type = SMALL_INT_TYPE;
			JDBObjectNew.value = JdbResources.longToByteArray(lon, 2);
			length += 3;
			namedLength += 5;
			JDBObjectNew.namedLength  = 3;
			JDBObjectNew.length = 2;
		}
		else if(lon<16777216)
		{
			JDBObjectNew.type = BIG_INT_TYPE;
			JDBObjectNew.value = JdbResources.longToByteArray(lon, 3);
			length += 4;
			namedLength += 6;
			JDBObjectNew.namedLength  = 4;
			JDBObjectNew.length = 3;
		}
		else if(lon<4294967296L)
		{
			JDBObjectNew.type = INT_TYPE;
			JDBObjectNew.value = JdbResources.longToByteArray(lon, 4);
			length += 5;
			namedLength += 7;
			JDBObjectNew.namedLength  = 5;
			JDBObjectNew.length = 4;
		}
		else if(lon<1099511627776L)
		{
			JDBObjectNew.type = VS_LONG_INT_TYPE;
			JDBObjectNew.value = JdbResources.longToByteArray(lon, 5);
			length += 6;
			namedLength += 8;
			JDBObjectNew.namedLength  = 6;
			JDBObjectNew.length = 5;
		}
		else if(lon<281474976710656L)
		{
			JDBObjectNew.type = S_LONG_INT_TYPE;
			JDBObjectNew.value = JdbResources.longToByteArray(lon, 6);
			length += 7;
			namedLength += 9;
			JDBObjectNew.namedLength  = 7;
			JDBObjectNew.length = 6;
		}
		else if(lon<72057594037927936L)
		{
			JDBObjectNew.type = B_LONG_INT_TYPE;
			JDBObjectNew.value = JdbResources.longToByteArray(lon, 7);
			length += 8;
			namedLength += 10;
			JDBObjectNew.namedLength  = 8;
			JDBObjectNew.length = 7;
		}
		else
		{
			JDBObjectNew.type = LONG_INT_TYPE;
			JDBObjectNew.value = JdbResources.longToByteArray(lon, 8);
			length += 9;
			namedLength += 11;
			JDBObjectNew.namedLength  = 9;
			JDBObjectNew.length = 8;
		}
		JDBObjectNew.name = "";		
		packets.add(JDBObjectNew);		
		return JDBObjectNew;
	}
	
	/**
	 * @param doub
	 * @param name
	 * Add a double property to an Object
	 */
	public void addPacket(float doub,String name)
	{
		JDBObject JDBObjectNew = addPacket(doub);
		JDBObjectNew.name = name;
		//length += name.length();
		namedLength += name.length();
		JDBObjectNew.namedLength += name.length();
	}
	/**
	 * @param doub
	 * Add a double property to an Object
	 */
	public JDBObject addPacket(float doub)
	{
		JDBObject JDBObjectNew = new JDBObject();
		JDBObjectNew.type = DOUBLE_FLOAT_TYPE;
		JDBObjectNew.name = "";
		JDBObjectNew.length = String.valueOf(doub).length();
		JDBObjectNew.value = String.valueOf(doub).getBytes();
		packets.add(JDBObjectNew);
		length += JDBObjectNew.value.length + 2;
		namedLength += JDBObjectNew.value.length + 4;
		JDBObjectNew.namedLength = JDBObjectNew.value.length + 2;
		return JDBObjectNew;
	}
	
	
	/**
	 * @param doub
	 * @param name
	 * Add a double property to an Object
	 */
	public void addPacket(double doub,String name)
	{
		JDBObject JDBObjectNew = addPacket(doub);
		JDBObjectNew.name = name;
		//length += name.length();
		namedLength += name.length();
		JDBObjectNew.namedLength += name.length();
	}
	/**
	 * @param doub
	 * Add a double property to an Object
	 */
	public JDBObject addPacket(double doub)
	{
		JDBObject JDBObjectNew = new JDBObject();
		JDBObjectNew.type = DOUBLE_FLOAT_TYPE;
		JDBObjectNew.name = "";
		JDBObjectNew.length = String.valueOf(doub).length();
		JDBObjectNew.value = String.valueOf(doub).getBytes();
		packets.add(JDBObjectNew);
		length += JDBObjectNew.value.length + 2;
		namedLength += JDBObjectNew.value.length + 4;
		JDBObjectNew.namedLength = JDBObjectNew.value.length + 2;
		return JDBObjectNew;
	}
	
	/**
	 * @param integer
	 * @param name
	 * Add an integer property to an Object
	 */
	public void addPacket(int integer,String name)
	{
		JDBObject JDBObjectNew = addPacket(integer);
		JDBObjectNew.name = name;
		//length += name.length();
		namedLength += name.length();
		JDBObjectNew.namedLength += name.length();
	}
	/**
	 * @param integer
	 * Add an integer property to an Object
	 */
	public JDBObject addPacket(int integer)
	{
		JDBObject JDBObjectNew = new JDBObject();
		if(integer<256)
		{
			JDBObjectNew.type = VERY_SMALL_INT_TYPE;
			JDBObjectNew.value = JdbResources.intToByteArray(integer, 1);
			length += 2;
			namedLength += 4;
			JDBObjectNew.namedLength  = 2;
			JDBObjectNew.length = 1;
		}
		else if(integer<65536)
		{
			JDBObjectNew.type = SMALL_INT_TYPE;
			JDBObjectNew.value = JdbResources.intToByteArray(integer, 2);
			length += 3;
			namedLength += 5;
			JDBObjectNew.namedLength  = 3;
			JDBObjectNew.length = 2;
		}
		else if(integer<16777216)
		{
			JDBObjectNew.type = BIG_INT_TYPE;
			JDBObjectNew.value = JdbResources.intToByteArray(integer, 3);
			length += 4;
			namedLength += 6;
			JDBObjectNew.namedLength  = 4;
			JDBObjectNew.length = 3;
		}
		else
		{
			JDBObjectNew.type = INT_TYPE;
			JDBObjectNew.value = JdbResources.intToByteArray(integer, 4);
			length += 5;
			namedLength += 7;
			JDBObjectNew.namedLength  = 5;
			JDBObjectNew.length = 4;
		}
		JDBObjectNew.name = "";
		packets.add(JDBObjectNew);		
		return JDBObjectNew;
	}
	
	/**
	 * @param date
	 * @param name
	 * Add a Date property to an Object
	 */
	public void addPacket(Date date,String name)
	{
		JDBObject JDBObjectNew = addPacket(date);
		JDBObjectNew.name = name;
		namedLength += name.length();
		JDBObjectNew.namedLength += name.length();
	}
	/**
	 * @param date
	 * Add a Date property to an Object
	 */
	public JDBObject addPacket(Date date)
	{
		JDBObject JDBObjectNew = new JDBObject();
		JDBObjectNew.type = DATE_TYPE;
		JDBObjectNew.name = "";
		SimpleDateFormat format = new SimpleDateFormat("ddMMyyyy HHmmss");
		JDBObjectNew.length = 15;
		JDBObjectNew.value = format.format(date).getBytes();
		JDBObjectNew.namedLength += JDBObjectNew.value.length;
		length += JDBObjectNew.value.length + 2;
		namedLength += JDBObjectNew.value.length + 4;
		JDBObjectNew.namedLength  = JDBObjectNew.value.length + 2;
		JDBObjectNew.length = JDBObjectNew.value.length;
		packets.add(JDBObjectNew);
		return JDBObjectNew;
	}
	
	/**
	 * @param packet
	 * Add a JDBObjectNew property to an Object
	 */
	public void addPacket(JDBObject packet)
	{
		packets.add(packet);
		if(packet.type=='o')
		{
			if(packet.length+1<256)
				packet.type = 'm';
			else if(packet.length+1<65536)
				packet.type = 'q';
			else if(packet.length+1<16777216)
				packet.type = 'p';
			else
				packet.type = 'o';
		}
		length += packet.getLength();
		namedLength += packet.getNamedLength(false);
	}
	
	
	public void set(int i,JDBObject jdbo)
	{
		packets.set(i,jdbo); 
	}
	
	/**
	 * @param packet
	 * Add a JDBObjectNew property to an Object
	 */
	public void addPacket(byte[] packet,char type)
	{
		if(type=='s' || type=='d' || type=='t' || type=='h' || type=='y')
		{
			addPacket(packet);
		}
		else if(type=='n' || type=='w' || type=='r' || type=='i')
		{
			addPacket(JdbResources.byteArrayToInt(packet));
		}
		else if(type=='f' || type=='x' || type=='e' || type=='l')
		{
			addPacket(JdbResources.byteArrayToLong(packet));
		}
		else if(type=='u')
		{
			addPacket(Double.parseDouble(new String(packet)));
		}
		else if(type=='b')
		{
			addPacket(packet[0]=='1'?true:false);
		}
		else if(type=='c')
		{
			addPacket((char)packet[0]);
		}
		else if(type=='a' || type=='g' || type=='j' || type=='k' || type=='v' || type=='z')
		{
			addNullPacket(type);
		}
	}
	
	public void addPacket(Object obj)
	{
		if(obj instanceof Long)
			addPacket(((Long)obj).longValue());
		else if(obj instanceof Double)
			addPacket(((Double)obj).doubleValue());
		else if(obj instanceof String)
			addPacket((String)obj);
		else if(obj instanceof Byte)
			addPacket(((Byte)obj).byteValue());
	}
	
	public int getlength()
	{
		return length;
	}
	
	public int getLength()
	{
		if(type=='m')
		{
			return 2 + length;
		}
		else if(type=='q')
		{
			return 3 + length;
		}
		else if(type=='p')
		{
			return 4 + length;
		}
		else if(type=='o')
		{
			return 5 + length;
		}
		else
			return length;
	}
	
	
	public static boolean isString(char type)
	{
		if(type=='s' || type=='t' || type=='h' || type=='y' || type=='a')
			return true;
		return false;		
	}
	
	public static boolean isFloatingPoint(char type)
	{
		if(type=='u' || type=='k')
			return true;
		return false;		
	}
	
	public static boolean isNumber(char type)
	{
		if(type=='n' || type=='w' || type=='r' || type=='i'
			|| type=='f' || type=='x' || type=='e' || type=='l' || type=='g')
			return true;	
		return false;
	}
	
	public static boolean isInteger(char type)
	{
		if(type=='n' || type=='w' || type=='r' || type=='i')
			return true;	
		return false;
	}
	
	public static boolean isLong(char type)
	{
		if(type=='n' || type=='w' || type=='r' || type=='i'
			|| type=='f' || type=='x' || type=='e' || type=='l')
			return true;	
		return false;
	}
	
	public static boolean isChar(char type)
	{
		if(type=='c' ||  type=='z')
			return true;	
		return false;
	}
	
	public static boolean isBoolean(char type)
	{
		if(type=='b' || type=='v')
			return true;	
		return false;
	}
	
	public static boolean isDate(char type)
	{
		if(type=='d' || type=='j')
			return true;
		return false;
	}
	
	public boolean isString()
	{
		if(type=='s' || type=='t' || type=='h' || type=='y' || type=='a')
			return true;
		return false;		
	}
	
	public boolean isFloatingPoint()
	{
		if(type=='u' || type=='k')
			return true;
		return false;		
	}
	
	public boolean isNumber()
	{
		if(type=='n' || type=='w' || type=='r' || type=='i'
			|| type=='f' || type=='x' || type=='e' || type=='l' || type=='g')
			return true;	
		return false;
	}
	
	public boolean isChar()
	{
		if(type=='b' || type=='c' || type=='v' || type=='z')
			return true;	
		return false;
	}
	
	public boolean isDate()
	{
		if(type=='d' || type=='j')
			return true;
		return false;
	}
	
	public int getNamedLength(boolean ignoreName)
	{
		if(ignoreName)
		{
			if(getType()=='o')
			{
				if(length<256)
					type = 'm';
				else if(length<65536)
					type = 'q';
				else if(length<16777216)
					type = 'p';
				else
					type = 'o';
				return getLength();
			}
			else
			{
				int len = length;
				if(getType()!='n' && getType()!='w' && getType()!='r' 
					&& getType()!='i' && getType()!='f' && getType()!='x' 
						&& getType()!='e' && getType()!='l' && getType()!='b'
							&& getType()!='c')
				{
					len++;
				}
				if(getType()=='a' || getType()=='g'
					|| getType()=='j' || getType()=='v' || getType()=='z')
					return len;
				if(length<256)
					len++;
				else if(length<65536)
					len+=2;
				else if(length<16777216)
					len+=3;
				else
					len+=4;
				return len;
			}
		}
		else
		{
			if(getType()=='o')
			{
				if(2 + namedLength<256)
				{
					type = 'm';
					namedLength += 2;
				}
				else if(2 + namedLength<65536)
				{
					type = 'q';
					namedLength += 3;
				}
				else if(2 + namedLength<16777216)
				{
					type = 'p';
					namedLength += 4;
				}
				else
				{
					type = 'o';
					namedLength += 5;
				}
				return namedLength;
			}
			else if(getType()=='m' || getType()=='p' || getType()=='q')
			{
				return namedLength;
			}
			else
			{
				return 2 + namedLength;
			}
		}
		
	}
	
	public void setLength(int length)
	{
		this.length = length;
	}
	
	public byte[] getName()
	{
		return name.getBytes();
	}
	public String getNameStr()
	{
		return name;
	}
	public void setName(String name)
	{
		this.name = name;
	}
	public void setName(byte[] name)
	{
		this.name = new String(name);
	}
	public List<JDBObject> getPackets()
	{
		return packets;
	}
	public void setPackets(List<JDBObject> packets)
	{
		this.packets = packets;
	}
	
	public char getType()
	{
		return type;
	}
	public void setType(char type)
	{
		this.type = type;
	}
	
	public byte[] getValue()
	{
		return value;
	}
	public Object getTValue()
	{
		if(type=='s' || type=='t' || type=='d' || type=='h' || type=='y' || type=='u')
			return value;
		else if(getType()!='n' && getType()!='w' && getType()!='r' 
				&& getType()!='i')
		{
			return getIntValue();
		}
		else if(getType()!='f' && getType()!='x' 
					&& getType()!='e' && getType()!='l')
		{
			return getLongValue();
		}
		else if(getType()!='b')
		{
			return getBooleanValue();
		}
		else if(getType()!='c')
		{
			return (char)value[0];
		}
		return this;
	}
	public String getValueStr()
	{
		return new String(value);
	}
	public void setValue(byte[] value)
	{
		this.value = value;
	}
	public void setValue(String value)
	{
		this.value = value.getBytes();
	}
	
	
	/**
	 * @return boolean value of this object if its type is boolean
	 */
	public boolean getBooleanValue()
	{
		if(type=='b')
			return (value.equals("1")?true:false);
		else
			return false;
	}
	
	/**
	 * @return integer value of this object if its type is integer
	 */
	public int getIntValue()
	{
		if(type=='n' || type=='w' || type=='r' || type=='i')
		{
			return JdbResources.byteArrayToInt(value);
		}
		else
			return -1;
	}
	
	/**
	 * @return double value of this object if its type is double
	 */
	public double getDoubleValue()
	{
		if(type=='u')
			return (Double.valueOf(new String(value)));
		else
			return -1;
	}
	
	/**
	 * @return long value of this object if its type is long
	 */
	public long getLongValue()
	{
		if(type=='f' || type=='x' || type=='e' || type=='l')
		{
			return JdbResources.byteArrayToLong(value);
		}
		else
			return -1;
	}
	
	public long getNumericValue()
	{
		if(type=='f' || type=='x' || type=='e' || type=='l'
			|| type=='n' || type=='w' || type=='r' || type=='i')
		{
			return JdbResources.byteArrayToLong(value);
		}
		else
			return -1;
	}
	
	/**
	 * @return Date value of this object if its type is Date
	 */
	public Date getDateValue()
	{
		if(type=='b') 
		{
			try
			{
				return new SimpleDateFormat("ddMMyyyy HHmmss").parse(new String(value));
			}
			catch (ParseException e)
			{
				return new Date();
			}
		}
		else
			return new Date();
	}
	
	public String toString()
	{
		return displayObject("");
	}
	
	private String displayObject(String tab)
	{
		String displ = "";
		for (int i=0;i<(int)getPackets().size();i++)
		{
			JDBObject obj = getPackets().get(i);		
			displ += tab + "Object Type = ";
			displ += obj.type;
			displ += "\n" + tab + "Object Name = " + obj.name + "\n";
			displ += tab + "Object Value = ";
			if(obj.isString() || obj.isFloatingPoint() || obj.isDate())
				displ += obj.getValueStr() + "\n";
			else if(obj.isChar())
			{
				if(type=='b')
					displ += obj.getBooleanValue() + "\n";
				else
					displ += (char)obj.value[0] + "\n";
			}
			else if(obj.isNumber())
			{
				displ += obj.getNumericValue() + "\n";
			}
			if(obj.type=='o' || obj.type=='p' || obj.type=='q' || obj.type=='m')
			{
				displ += obj.displayObject(tab+"\t");
			}
		}
		return displ;
	}

	@Override
	public int hashCode()
	{
		final int PRIME = 31;
		int result = 1;
		result = PRIME * result + ((name == null) ? 0 : name.hashCode());
		result = PRIME * result + ((packets == null) ? 0 : packets.hashCode());
		result = PRIME * result + type;
		result = PRIME * result + Arrays.hashCode(value);
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		final JDBObject other = (JDBObject) obj;
		if (name == null)
		{
			if (other.name != null) return false;
		}
		else if (!name.equals(other.name)) return false;
		if (packets == null)
		{
			if (other.packets != null) return false;
		}
		else if (!packets.equals(other.packets)) return false;
		if (type != other.type) return false;
		if (!Arrays.equals(value, other.value)) return false;
		return true;
	}

	public void addStaticPacket(JDBObject obj)
	{
		packets.add(obj);
	}
	
	public int compare(JDBObject obj)
	{
		if(value==null && obj.value!=null)
			return -1;
		else if(value!=null && obj.value==null)
			return 1;
		else if(value==null && obj.value==null)
			return 0;
		else if(isString())
		{
			return getValueStr().compareTo(obj.getValueStr());
		}
		else if(isNumber())
		{
			return new Long(getNumericValue()).
						compareTo(new Long(obj.getNumericValue()));
		}
		else if(isChar())
		{
			return new Character((char)getValue()[0]).
						compareTo(new Character((char)obj.getValue()[0]));
		}
		else if(isFloatingPoint())
		{
			return getValueStr().compareTo(obj.getValueStr());
		}
		else if(isDate())
		{
			return getDateValue().compareTo(obj.getDateValue());
		}
		else
			return 0;
	}
}
