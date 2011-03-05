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

import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.jdb.JdbResources;

/**
 * @author sumeet.chhetri
 * The Automated Message Exchange Format Object type
 * Is a wrapper for basic as well as complex object heirarchies
 * can be a string, number, date, boolean, character or any complex object
 * Every message consists of only one JDBObject *
 */
public class JDBObjectOld
{
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
	private String value;
	
	/*The properties of a complex object*/
	private List<JDBObjectOld> packets;
	
	/**
	 * @return Array of JDBObject	 
	 *  
	 */
	public JDBObjectOld[] getObjects()
	{
		return packets.toArray(new JDBObjectOld[packets.size()]);
	}
	
	/*Create a new AMEF object which will initilaize the values*/
	public JDBObjectOld()
	{
		type = OBJECT_TYPE;
		length = 0;
		name = "";
		packets = new ArrayList<JDBObjectOld>();
	}
	
	
	/**
	 * @param string
	 * @param name
	 * Add a String property to an Object
	 */
	public void addPacket(String string,String name)
	{
		JDBObjectOld JDBObject = addPacket(string);
		JDBObject.name = name;
		length += name.length();
		namedLength += name.length();
	}
	/**
	 * @param string
	 * Add a String property to an Object
	 */
	public JDBObjectOld addPacket(String string)
	{
		JDBObjectOld JDBObject = new JDBObjectOld();		
		JDBObject.name = "";
		if(string.length()<=256)
		{
			JDBObject.type = STRING_256_TYPE;
			length += string.length() + 2;
			namedLength += string.length() + 4;
		}
		else if(string.length()<=65536)
		{
			JDBObject.type = STRING_65536_TYPE;
			length += string.length() + 3;
			namedLength += string.length() + 5;
		}
		else if(string.length()<=16777216)
		{
			JDBObject.type = STRING_16777216_TYPE;
			length += string.length() + 4;
			namedLength += string.length() + 6;
		}
		else
		{
			JDBObject.type = STRING_TYPE;
			length += string.length() + 5;
			namedLength += string.length() + 7;
		}
		JDBObject.length = string.length();		
		JDBObject.value = string;
		packets.add(JDBObject);
		return JDBObject;
	}
	
	/**
	 * @param bool
	 * @param name
	 * Add a boolean property to an Object
	 */
	public void addPacket(boolean bool,String name)
	{
		JDBObjectOld JDBObject = addPacket(bool);
		JDBObject.name = name;
		length += name.length();
		namedLength += name.length();
	}
	/**
	 * @param bool
	 * Add a boolean property to an Object
	 */
	public JDBObjectOld addPacket(boolean bool)
	{
		JDBObjectOld JDBObject = new JDBObjectOld();
		JDBObject.type = BOOLEAN_TYPE;
		JDBObject.name = "";
		JDBObject.length = 1;
		if(bool==true)
		{			
			JDBObject.value = "1";
		}
		else
		{
			JDBObject.value = "0";		
		}		
		packets.add(JDBObject);
		length += 2;
		namedLength += 4;
		return JDBObject;
	}
	
	public void addPacket(char chr,String name)
	{
		JDBObjectOld JDBObject = addPacket(chr);
		JDBObject.name = name;
		length += name.length();
		namedLength += name.length();
	}
	/**
	 * @param bool
	 * Add a boolean property to an Object
	 */
	public JDBObjectOld addPacket(char chr)
	{
		JDBObjectOld JDBObject = new JDBObjectOld();
		JDBObject.type = 'c';
		JDBObject.name = "";
		JDBObject.length = 1;
		JDBObject.value = String.copyValueOf(new char[]{chr});	
		packets.add(JDBObject);
		length += 2;
		namedLength += 4;
		return JDBObject;
	}
	
	/**
	 * @param lon
	 * @param name
	 * Add a long property to an Object
	 */
	public void addPacket(long lon,String name)
	{
		JDBObjectOld JDBObject = addPacket(lon);
		JDBObject.name = name;
		length += name.length();
		namedLength += name.length();
	}
	/**
	 * @param lon
	 * Add a long property to an Object
	 */
	public JDBObjectOld addPacket(long lon)
	{
		JDBObjectOld JDBObject = new JDBObjectOld();
		if(lon<256)
		{
			JDBObject.type = VERY_SMALL_INT_TYPE;
			JDBObject.value = JdbResources.longToByteArrayS(lon, 1);
			length += 2;
			namedLength += 4;
		}
		else if(lon<65536)
		{
			JDBObject.type = SMALL_INT_TYPE;
			JDBObject.value = JdbResources.longToByteArrayS(lon, 2);
			length += 3;
			namedLength += 5;
		}
		else if(lon<16777216)
		{
			JDBObject.type = BIG_INT_TYPE;
			JDBObject.value = JdbResources.longToByteArrayS(lon, 3);
			length += 4;
			namedLength += 6;
		}
		else if(lon<4294967296L)
		{
			JDBObject.type = INT_TYPE;
			JDBObject.value = JdbResources.longToByteArrayS(lon, 4);
			length += 5;
			namedLength += 7;
		}
		else if(lon<1099511627776L)
		{
			JDBObject.type = VS_LONG_INT_TYPE;
			JDBObject.value = JdbResources.longToByteArrayS(lon, 5);
			length += 6;
			namedLength += 8;
		}
		else if(lon<281474976710656L)
		{
			JDBObject.type = S_LONG_INT_TYPE;
			JDBObject.value = JdbResources.longToByteArrayS(lon, 6);
			length += 7;
			namedLength += 9;
		}
		else if(lon<72057594037927936L)
		{
			JDBObject.type = B_LONG_INT_TYPE;
			JDBObject.value = JdbResources.longToByteArrayS(lon, 7);
			length += 8;
			namedLength += 10;
		}
		else
		{
			JDBObject.type = LONG_INT_TYPE;
			JDBObject.value = JdbResources.longToByteArrayS(lon, 8);
			length += 9;
			namedLength += 11;
		}
		JDBObject.name = "";
		JDBObject.length = String.valueOf(lon).length();
		packets.add(JDBObject);		
		return JDBObject;
	}
	
	/**
	 * @param doub
	 * @param name
	 * Add a double property to an Object
	 */
	public void addPacket(float doub,String name)
	{
		JDBObjectOld JDBObject = addPacket(doub);
		JDBObject.name = name;
		length += name.length();
		namedLength += name.length();
	}
	/**
	 * @param doub
	 * Add a double property to an Object
	 */
	public JDBObjectOld addPacket(float doub)
	{
		JDBObjectOld JDBObject = new JDBObjectOld();
		JDBObject.type = DOUBLE_FLOAT_TYPE;
		JDBObject.name = "";
		JDBObject.length = String.valueOf(doub).length();
		JDBObject.value = String.valueOf(doub);
		packets.add(JDBObject);
		length += JDBObject.value.length();
		namedLength += JDBObject.value.length() + 4;
		return JDBObject;
	}
	
	
	/**
	 * @param doub
	 * @param name
	 * Add a double property to an Object
	 */
	public void addPacket(double doub,String name)
	{
		JDBObjectOld JDBObject = addPacket(doub);
		JDBObject.name = name;
		length += name.length();
		namedLength += name.length();
	}
	/**
	 * @param doub
	 * Add a double property to an Object
	 */
	public JDBObjectOld addPacket(double doub)
	{
		JDBObjectOld JDBObject = new JDBObjectOld();
		JDBObject.type = DOUBLE_FLOAT_TYPE;
		JDBObject.name = "";
		JDBObject.length = String.valueOf(doub).length();
		JDBObject.value = String.valueOf(doub);
		packets.add(JDBObject);
		length += JDBObject.value.length();
		namedLength += JDBObject.value.length() + 4;
		return JDBObject;
	}
	
	/**
	 * @param integer
	 * @param name
	 * Add an integer property to an Object
	 */
	public void addPacket(int integer,String name)
	{
		JDBObjectOld JDBObject = addPacket(integer);
		JDBObject.name = name;
		length += name.length();
		namedLength += name.length();
	}
	/**
	 * @param integer
	 * Add an integer property to an Object
	 */
	public JDBObjectOld addPacket(int integer)
	{
		JDBObjectOld JDBObject = new JDBObjectOld();
		if(integer<256)
		{
			JDBObject.type = VERY_SMALL_INT_TYPE;
			JDBObject.value = JdbResources.intToByteArrayS(integer, 1);
			length += 2;
			namedLength += 4;
		}
		else if(integer<65536)
		{
			JDBObject.type = SMALL_INT_TYPE;
			JDBObject.value = JdbResources.intToByteArrayS(integer, 2);
			length += 3;
			namedLength += 5;
		}
		else if(integer<16777216)
		{
			JDBObject.type = BIG_INT_TYPE;
			JDBObject.value = JdbResources.intToByteArrayS(integer, 3);
			length += 4;
			namedLength += 6;
		}
		else
		{
			JDBObject.type = INT_TYPE;
			JDBObject.value = JdbResources.intToByteArrayS(integer, 4);
			length += 5;
			namedLength += 7;
		}
		JDBObject.name = "";
		JDBObject.length = String.valueOf(integer).length();
		packets.add(JDBObject);		
		return JDBObject;
	}
	
	/**
	 * @param date
	 * @param name
	 * Add a Date property to an Object
	 */
	public void addPacket(Date date,String name)
	{
		JDBObjectOld JDBObject = addPacket(date);
		JDBObject.name = name;
		length += name.length();
	}
	/**
	 * @param date
	 * Add a Date property to an Object
	 */
	public JDBObjectOld addPacket(Date date)
	{
		JDBObjectOld JDBObject = new JDBObjectOld();
		JDBObject.type = DATE_TYPE;
		JDBObject.name = "";
		SimpleDateFormat format = new SimpleDateFormat("ddMMyyyy HHmmss");
		JDBObject.length = 15;
		JDBObject.value = format.format(date);
		packets.add(JDBObject);
		return JDBObject;
	}
	
	/**
	 * @param packet
	 * Add a JDBObject property to an Object
	 */
	public void addPacket(JDBObjectOld packet)
	{
		packets.add(packet);
		if(packet.length+1<256)
			packet.type = 'm';
		else if(packet.length+1<65536)
			packet.type = 'q';
		else if(packet.length+1<16777216)
			packet.type = 'p';
		else
			packet.type = 'o';
		length += packet.getLength();
		namedLength += packet.getNamedLength();
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
	
	public int getNamedLength()
	{
		return 2 + namedLength;
	}
	
	public void setLength(int length)
	{
		this.length = length;
	}
	
	public String getName()
	{
		return name;
	}
	public void setName(String name)
	{
		this.name = name;
	}
	
	public List<JDBObjectOld> getPackets()
	{
		return packets;
	}
	public void setPackets(List<JDBObjectOld> packets)
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
	
	public String getValue()
	{
		return value;
	}
	public void setValue(String value)
	{
		this.value = value;
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
			return ByteBuffer.wrap(value.getBytes()).asIntBuffer().get();
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
			return (Double.valueOf(value));
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
			return ByteBuffer.wrap(value.getBytes()).asLongBuffer().get();
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
				return new SimpleDateFormat("ddMMyyyy HHmmss").parse(value);
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
			JDBObjectOld obj = getPackets().get(i);		
			displ += tab + "Object Type = ";
			displ += obj.type;
			displ += "\n" + tab + "Object Name = " + obj.name + "\n";
			displ += tab + "Object Length = ";
			displ += obj.length;
			displ += "\n" + tab + "Object Value = " + obj.value + "\n";
			if(obj.type=='o')
			{
				displ += obj.displayObject("\t");
			}
		}
		return displ;
	}

	@Override
	public int hashCode()
	{
		final int PRIME = 31;
		int result = 1;
		result = PRIME * result + length;
		result = PRIME * result + ((name == null) ? 0 : name.hashCode());
		result = PRIME * result + ((packets == null) ? 0 : packets.hashCode());
		result = PRIME * result + type;
		result = PRIME * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		final JDBObjectOld other = (JDBObjectOld) obj;
		if (length != other.length) return false;
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
		if (value == null)
		{
			if (other.value != null) return false;
		}
		else if (!value.equals(other.value)) return false;
		return true;
	}
}
