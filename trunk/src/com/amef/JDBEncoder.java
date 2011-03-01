/*
	Copyright 2010, Sumeet Chhetri 
  
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

import com.jdb.JdbResources;


/**
 * @author sumeetc
 * The AMEF Encoder Class
 * provides the encode method to encode the JDBObject
 */
public class JDBEncoder
{
	/*The default delimiter for single object representation*/
	//private String delim = ",";
		
	/**
	 * @param packet
	 * @return byte[]
	 * @throws AMEFEncodeException
	 * encode the JDBObject to the bytestream for wire transmission
	 */
	/*public byte[] encode(JDBObject packet,boolean ignoreName) throws AMEFEncodeException
	{
		String dat = encodeSinglePacket(packet,ignoreName);
		int l = dat.length();		
		return (JdbResources.intToByteArrayS(l, 4) + dat).getBytes();
	}	*/
	
	public byte[] encodeB(JDBObject packet,boolean ignoreName) throws AMEFEncodeException
	{
		byte[] dat = new byte[packet.getNamedLength(ignoreName) + 4];
		byte[] enc = encodeSinglePacketB(packet, ignoreName);
		byte[] len = JdbResources.intToByteArray(enc.length, 4);
		System.arraycopy(len, 0, dat, 0, 4);
		System.arraycopy(enc, 0, dat, 4, enc.length);
		return dat;
	}
	
	public byte[] encodeWL(JDBObject packet,boolean ignoreName) throws AMEFEncodeException
	{
		//byte[] dat = new byte[packet.getNamedLength(ignoreName)];
		byte[] enc = encodeSinglePacketB(packet, ignoreName);
		//System.arraycopy(enc, 0, dat, 0, enc.length);
		return enc;
	}
	
	/*public byte[] encodeWL(JDBObject packet,boolean ignoreName) throws AMEFEncodeException
	{
		String dat = encodeSinglePacket(packet,ignoreName);
		int l = dat.length();		
		return dat.getBytes();
	}
	
	*//**
	 * @param packet
	 * @return byte[]
	 * @throws AMEFEncodeException
	 * encode the JDBObject to the bytestream for wire transmission
	 *//*
	public String encodeS(JDBObject packet,boolean ignoreName) throws AMEFEncodeException
	{
		String dat = null;
		if(ignoreName)
			dat = encodeJDBObject(packet);
		else
			dat = encodeSinglePacket(packet,ignoreName);
		int l = dat.length();		
		return (JdbResources.intToByteArrayS(l, 4) + dat);
	}
	
	public String encodeJDBObject(JDBObject packet) throws AMEFEncodeException
	{
		String dat = encodeSingleJDBPacket(packet);
		int l = dat.length();		
		return (dat);
		//return dat;
	}
	
	private String getValue(int val,int ind)
	{
		byte[] buf = new byte[ind];
		for (int i = 0; i < buf.length; i++)
		{
			
		}
	}*/
	
	
	private void getValue(String value,char type,StringBuilder buffer)
	{
		/*if(type==JDBObject.DATE_TYPE || type==JDBObject.STRING_65536_TYPE
					|| type==JDBObject.STRING_256_TYPE || type==JDBObject.DOUBLE_FLOAT_TYPE
					|| type==JDBObject.STRING_16777216_TYPE || type==JDBObject.STRING_TYPE
					|| type==JDBObject.BOOLEAN_TYPE || type==JDBObject.CHAR_TYPE)
		{
			buffer.append(value);
		}
		else if(type==JDBObject.VERY_SMALL_INT_TYPE)
		{
			buffer.append(value);
		}
		else if(type==JDBObject.SMALL_INT_TYPE)
		{
			int intvalue = Integer.parseInt(value);
			buffer.append(JdbResources.intToByteArray(intvalue, 2));
		}
		else if(type==JDBObject.BIG_INT_TYPE)
		{
			int intvalue = Integer.parseInt(value);
			buffer.append(new String(JdbResources.intToByteArray(intvalue, 3)));
		}
		else if(type==JDBObject.INT_TYPE)
		{
			int intvalue = Integer.parseInt(value);	
			buffer.append(new String(JdbResources.intToByteArray(intvalue, 4)));
		}
		else if(type==JDBObject.VS_LONG_INT_TYPE)
		{
			long l = Long.parseLong(value);	
			buffer.append(new String(JdbResources.longToByteArray(l, 5)));
		}
		else if(type==JDBObject.S_LONG_INT_TYPE)
		{
			long l = Long.parseLong(value);		
			buffer.append(new String(JdbResources.longToByteArray(l, 6)));
		}
		else if(type==JDBObject.B_LONG_INT_TYPE)
		{
			long l = Long.parseLong(value);	
			buffer.append(new String(JdbResources.longToByteArray(l, 7)));
		}
		else if(type==JDBObject.LONG_INT_TYPE)
		{
			long l = Long.parseLong(value);			
			buffer.append(new String(JdbResources.longToByteArray(l, 8)));
		}*/
		buffer.append(value);
	}
	
	private String getFinalVal(char type,StringBuilder buffer,int length,String delim, String name) throws AMEFEncodeException
	{
		String retval = type + delim + name + delim;
		if(type==JDBObject.DATE_TYPE || type==JDBObject.STRING_256_TYPE 
				|| type==JDBObject.DOUBLE_FLOAT_TYPE)
		{
			retval += JdbResources.intToByteArrayS(length, 1) + buffer.toString();
		}
		else if(type==JDBObject.STRING_65536_TYPE)
		{
			retval += JdbResources.intToByteArrayS(length, 2) + buffer.toString();
		}
		else if(type==JDBObject.STRING_16777216_TYPE)
		{
			retval += new String(JdbResources.intToByteArrayS(length, 3)) + buffer.toString();
		}
		else if(type==JDBObject.STRING_TYPE)
		{
			retval += JdbResources.intToByteArrayS(length, 4) + buffer.toString();
		}
		else if(type==JDBObject.BOOLEAN_TYPE || type==JDBObject.CHAR_TYPE 
				|| type==JDBObject.SMALL_INT_TYPE || type==JDBObject.VERY_SMALL_INT_TYPE 
				|| type==JDBObject.BIG_INT_TYPE || type==JDBObject.INT_TYPE 
				|| type==JDBObject.VS_LONG_INT_TYPE || type==JDBObject.S_LONG_INT_TYPE
				|| type==JDBObject.B_LONG_INT_TYPE || type==JDBObject.LONG_INT_TYPE)
		{
			retval += buffer.toString();
		}
		else if(type==JDBObject.OBJECT_TYPE)
		{
			if(length<256)
			{
				retval = JDBObject.VS_OBJECT_TYPE + delim + name + delim + JdbResources.intToByteArrayS(length, 1) + buffer.toString();
			}
			else if(length<65536)
			{
				retval = JDBObject.S_OBJECT_TYPE + delim + name + delim + JdbResources.intToByteArrayS(length, 2) + buffer.toString();
			}
			else if(length<16777216)
			{
				retval = JDBObject.B_OBJECT_TYPE + delim + name + delim + JdbResources.intToByteArrayS(length, 3) + buffer.toString();
			}
			else
			{
				retval = JDBObject.OBJECT_TYPE + delim + name + delim + JdbResources.intToByteArrayS(length, 4) + buffer.toString();
			}
		}
		else if(type==JDBObject.VS_OBJECT_TYPE)
		{
			retval = JDBObject.VS_OBJECT_TYPE + delim + name + delim + JdbResources.intToByteArrayS(length, 1) + buffer.toString();
		}
		else if(type==JDBObject.S_OBJECT_TYPE)
		{
			retval = JDBObject.S_OBJECT_TYPE + delim + name + delim + JdbResources.intToByteArrayS(length, 2) + buffer.toString();
		}
		else if(type==JDBObject.B_OBJECT_TYPE)
		{
			retval = JDBObject.B_OBJECT_TYPE + delim + name + delim + JdbResources.intToByteArrayS(length, 3) + buffer.toString();
		}
		else
		{
			throw new AMEFEncodeException("Not a valid AMEF Object type,only types string,number,boolean,character,date allowed");
		}
		return retval;
	}
	
	/*private String encodeSingleJDBPacket(JDBObject packet) throws AMEFEncodeException
	{
		StringBuilder buffer = new StringBuilder();
		if(packet==null)
		{
			throw new AMEFEncodeException("Objcet to be encoded is null");
		}
		int length = packet.getLength();
		for (JDBObject pack : packet.getPackets())
		{
			buffer.append(encodeSingleJDBPacket(pack));
		}
		if(packet.getPackets().size()==0)
			getValue(new String(packet.getValue()),packet.getType(), buffer);
		if(buffer.length()>0)
		{
			length = buffer.length();			
		}
		return getFinalVal(packet.getType(), buffer, length, "","");
	}

	*//**
	 * @param packet
	 * @return String
	 * @throws AMEFEncodeException
	 * encode a given AMEF Object to its transmission form
	 *//*
	private String encodeSinglePacket(JDBObject packet,boolean ignoreName) throws AMEFEncodeException
	{
		StringBuilder buffer = new StringBuilder();
		if(packet==null)
		{
			throw new AMEFEncodeException("Objcet to be encoded is null");
		}
		int length = packet.getLength();
		for (JDBObject pack : packet.getPackets())
		{
			buffer.append(encodeSinglePacket(pack,ignoreName));
		}
		if(packet.getPackets().size()==0)
			getValue(new String(packet.getValue()),packet.getType(), buffer);
		if(buffer.length()>0)
		{
			length = buffer.length();			
		}
		String retval = "";
		if(!ignoreName)
			retval +=  packet.getName();
		return getFinalVal(packet.getType(), buffer, length, delim, retval);
	}
	
	
	private byte[] encodeSinglePacketB(JDBObject packet,boolean ignoreName) throws AMEFEncodeException
	{		
		ByteArrayOutputStream buffer = new ByteArrayOutputStream();
		if(packet==null)
		{
			throw new AMEFEncodeException("Objcet to be encoded is null");
		}
		int length = packet.getLength();
		for (JDBObject pack : packet.getPackets())
		{
			buffer.write(encodeSinglePacketB(pack,ignoreName));
		}
		if(packet.getPackets().size()==0)
		{
			buffer.write(packet.getValue().getBytes());
		}
		String retval = "";
		if(!ignoreName)
			retval +=  packet.getName();
		char type = packet.getType();
		if(type==JDBObject.DATE_TYPE || type==JDBObject.STRING_256_TYPE 
				|| type==JDBObject.DOUBLE_FLOAT_TYPE)
		{
			buffer.write(JdbResources.intToByteArray(length, 1));
		}
		else if(type==JDBObject.STRING_65536_TYPE)
		{
			buffer.write(JdbResources.intToByteArray(length, 2));
		}
		else if(type==JDBObject.STRING_16777216_TYPE)
		{
			buffer.write(JdbResources.intToByteArray(length, 3));
		}
		else if(type==JDBObject.STRING_TYPE)
		{
			buffer.write(JdbResources.intToByteArray(length, 4));
		}
		else if(type==JDBObject.BOOLEAN_TYPE || type==JDBObject.CHAR_TYPE 
				|| type==JDBObject.SMALL_INT_TYPE || type==JDBObject.VERY_SMALL_INT_TYPE 
				|| type==JDBObject.BIG_INT_TYPE || type==JDBObject.INT_TYPE 
				|| type==JDBObject.VS_LONG_INT_TYPE || type==JDBObject.S_LONG_INT_TYPE
				|| type==JDBObject.B_LONG_INT_TYPE || type==JDBObject.LONG_INT_TYPE)
		{
			retval += buffer.toString();
		}
		else if(type==JDBObject.OBJECT_TYPE)
		{
			if(length<256)
			{
				retval = JDBObject.VS_OBJECT_TYPE + delim + name + delim + JdbResources.intToByteArrayS(length, 1) + buffer.toString();
			}
			else if(length<65536)
			{
				retval = JDBObject.S_OBJECT_TYPE + delim + name + delim + JdbResources.intToByteArrayS(length, 2) + buffer.toString();
			}
			else if(length<16777216)
			{
				retval = JDBObject.B_OBJECT_TYPE + delim + name + delim + JdbResources.intToByteArrayS(length, 3) + buffer.toString();
			}
			else
			{
				retval = JDBObject.OBJECT_TYPE + delim + name + delim + JdbResources.intToByteArrayS(length, 4) + buffer.toString();
			}
		}
		return getFinalVal(packet.getType(), buffer, length, delim, retval);
	}*/
	
	
	public byte[] getPacketValue(String value) throws AMEFEncodeException
	{
		byte[] arr = new byte[2+value.length()];
		char type;
		if(value.length()<=256)
			type = JDBObject.STRING_256_TYPE;
		else if(value.length()<=65536)
			type = JDBObject.STRING_65536_TYPE;
		else if(value.length()<=16777216)
			type = JDBObject.STRING_16777216_TYPE;
		else
			type = JDBObject.STRING_TYPE;
		
		if(type==JDBObject.DATE_TYPE || type==JDBObject.STRING_256_TYPE 
				|| type==JDBObject.DOUBLE_FLOAT_TYPE)
		{
			arr[0] = (byte)type;
			byte[] len = JdbResources.intToByteArray(value.length(), 1);
			System.arraycopy(len, 0, arr, 1, len.length);
			System.arraycopy(value.getBytes(), 0, arr, len.length+1, value.getBytes().length);
		}
		else if(type==JDBObject.STRING_65536_TYPE)
		{
			arr[0] = (byte)type;
			byte[] len = JdbResources.intToByteArray(value.length(), 2);
			System.arraycopy(len, 0, arr, 1, len.length);
			System.arraycopy(value.getBytes(), 0, arr, len.length, value.getBytes().length);
		}
		else if(type==JDBObject.STRING_16777216_TYPE)
		{
			arr[0] = (byte)type;
			byte[] len = JdbResources.intToByteArray(value.length(), 3);
			System.arraycopy(len, 0, arr, 1, len.length);
			System.arraycopy(value.getBytes(), 0, arr, len.length, value.getBytes().length);
		}
		else if(type==JDBObject.STRING_TYPE)
		{
			arr[0] = (byte)type;
			byte[] len = JdbResources.intToByteArray(value.length(), 4);
			System.arraycopy(len, 0, arr, 1, len.length);
			System.arraycopy(value.getBytes(), 0, arr, len.length, value.getBytes().length);
		}
		else if(type==JDBObject.BOOLEAN_TYPE || type==JDBObject.CHAR_TYPE 
				|| type==JDBObject.SMALL_INT_TYPE || type==JDBObject.VERY_SMALL_INT_TYPE 
				|| type==JDBObject.BIG_INT_TYPE || type==JDBObject.INT_TYPE 
				|| type==JDBObject.VS_LONG_INT_TYPE || type==JDBObject.S_LONG_INT_TYPE
				|| type==JDBObject.B_LONG_INT_TYPE || type==JDBObject.LONG_INT_TYPE)
		{
			
		}
		else if(type==JDBObject.OBJECT_TYPE)
		{
			int length = value.length();
			if(length<256)
			{
				arr[0] = (byte)JDBObject.VS_OBJECT_TYPE;
				byte[] len = JdbResources.intToByteArray(value.length(), 1);
				System.arraycopy(len, 0, arr, 1, len.length);
				System.arraycopy(value.getBytes(), 0, arr, len.length, value.getBytes().length);
			}
			else if(length<65536)
			{
				arr[0] = (byte)JDBObject.S_OBJECT_TYPE;
				byte[] len = JdbResources.intToByteArray(value.length(), 2);
				System.arraycopy(len, 0, arr, 1, len.length);
			}
			else if(length<16777216)
			{
				arr[0] = (byte)JDBObject.B_OBJECT_TYPE;
				byte[] len = JdbResources.intToByteArray(value.length(),3);
				System.arraycopy(len, 0, arr, 1, len.length);
			}
			else
			{
				arr[0] = (byte)JDBObject.OBJECT_TYPE;
				byte[] len = JdbResources.intToByteArray(value.length(),4);
				System.arraycopy(len, 0, arr, 1, len.length);
			}
		}
		return arr;
	}
	
	public byte[] getPacketValue(int integer) throws AMEFEncodeException
	{
		char type;
		int ind = 1;
		if(integer<256)
			type = JDBObject.VERY_SMALL_INT_TYPE;
		else if(integer<65536)
		{
			type = JDBObject.SMALL_INT_TYPE;
			ind = 2;
		}
		else if(integer<16777216)
		{
			type = JDBObject.BIG_INT_TYPE;
			ind = 3;
		}
		else
		{
			type = JDBObject.INT_TYPE;
			ind = 4;
		}
		byte[] arr = new byte[ind+1];
		arr[0] = (byte)type;
		byte[] len = JdbResources.intToByteArray(integer, ind);
		System.arraycopy(len, 0, arr, 1, len.length);
		return arr;
	}
	
	public byte[] getPacketValue(long lon) throws AMEFEncodeException
	{
		char type;
		int ind = 1;
		if(lon<256)
			type = JDBObject.VERY_SMALL_INT_TYPE;
		else if(lon<65536)
		{
			type = JDBObject.SMALL_INT_TYPE;
			ind = 2;
		}
		else if(lon<16777216)
		{
			type = JDBObject.BIG_INT_TYPE;
			ind = 3;
		}
		else if(lon<4294967296L)
		{
			type = JDBObject.INT_TYPE;
			ind = 4;
		}
		else if(lon<1099511627776L)
		{
			type = JDBObject.VS_LONG_INT_TYPE;
			ind = 5;
		}
		else if(lon<281474976710656L)
		{
			type = JDBObject.S_LONG_INT_TYPE;
			ind = 6;
		}
		else if(lon<72057594037927936L)
		{
			type = JDBObject.B_LONG_INT_TYPE;
			ind = 7;
		}
		else
		{
			type = JDBObject.LONG_INT_TYPE;
			ind = 8;
		}
		byte[] arr = new byte[ind+1];
		arr[0] = (byte)type;
		byte[] len = JdbResources.longToByteArray(lon, ind);
		System.arraycopy(len, 0, arr, 1, len.length);
		return arr;
	}
	
	public byte[] getPacketValue(double lon) throws AMEFEncodeException
	{
		StringBuilder buffer = new StringBuilder();
		char type = JDBObject.DOUBLE_FLOAT_TYPE;
		getValue(String.valueOf(lon),type, buffer);
		String retVal = getFinalVal(type, buffer, 1, "", "");
		return retVal.getBytes();
	}
	
	public byte[] getPacketValue(float lon) throws AMEFEncodeException
	{
		StringBuilder buffer = new StringBuilder();
		char type = JDBObject.DOUBLE_FLOAT_TYPE;
		getValue(String.valueOf(lon),type, buffer);
		String retVal = getFinalVal(type, buffer, 1, "", "");
		return retVal.getBytes();
	}
	
	public byte[] getPacketValue(boolean lon) throws AMEFEncodeException
	{
		return (JDBObject.BOOLEAN_TYPE+(lon?"1":"0")).getBytes();
	}
	
	public byte[] getPacketValue(char lon) throws AMEFEncodeException
	{
		return (JDBObject.CHAR_TYPE+""+lon).getBytes();
	}
	
	
	
	
	
	
	
	
	
	//---------------------------------------------------------------------------------------//
	private byte[] encodeSinglePacketB(JDBObject packet,boolean ignoreName) throws AMEFEncodeException
	{
		byte[] buffer = new byte[packet.getNamedLength(ignoreName)];
		int pos = 0;
		if(!ignoreName)
		{
			if(packet.getType()=='o')
			{
				if(buffer.length+4<256)
				{
					buffer[0] = JDBObject.VS_OBJECT_TYPE;
				}
				else if(buffer.length+4<65536)
				{
					buffer[0] = JDBObject.S_OBJECT_TYPE;
				}
				else if(buffer.length+4<16777216)
				{
					buffer[0] = JDBObject.B_OBJECT_TYPE;
				}
			}
			else
				buffer[0] = (byte)packet.getType();
			buffer[1] = (byte)',';
			System.arraycopy(packet.getName(), 0, buffer, 2, packet.getName().length);
			pos = packet.getName().length + 2;
			buffer[pos] = (byte)',';
			pos ++;
			if(packet.getType()!='n' && packet.getType()!='w' && packet.getType()!='r' 
				&& packet.getType()!='i' && packet.getType()!='f' && packet.getType()!='x' 
					&& packet.getType()!='e' && packet.getType()!='l' && packet.getType()!='b'
						&& packet.getType()!='c' && packet.getType()!='a' && packet.getType()!='g'
							&& packet.getType()!='j' && packet.getType()!='v' && packet.getType()!='z')
			{
				byte[] lengthb = JdbResources.intToByteArrayWI(packet.getlength());
				System.arraycopy(lengthb, 0, buffer, pos, lengthb.length);
				pos += lengthb.length;
			}			
		}
		else
		{
			buffer[0] = (byte)packet.getType();
			pos++;
			if(packet.getType()!='n' && packet.getType()!='w' && packet.getType()!='r' 
				&& packet.getType()!='i' && packet.getType()!='f' && packet.getType()!='x' 
					&& packet.getType()!='e' && packet.getType()!='l' && packet.getType()!='b'
						&& packet.getType()!='c' && packet.getType()!='a' && packet.getType()!='g'
							&& packet.getType()!='j' && packet.getType()!='v' && packet.getType()!='z')
			{
				byte[] lengthb = JdbResources.intToByteArrayWI(packet.getlength());
				System.arraycopy(lengthb, 0, buffer, pos, lengthb.length);
				pos += lengthb.length;
			}	
		}
		if(packet==null)
		{
			throw new AMEFEncodeException("Objcet to be encoded is null");
		}
		for (JDBObject pack : packet.getPackets())
		{
			byte[] val = encodeSinglePacketB(pack,ignoreName);
			System.arraycopy(val, 0, buffer, pos, val.length);
			pos += val.length;
		}
		if(packet.getPackets().size()==0 && packet.getType()!='a' && packet.getType()!='g'
			&& packet.getType()!='j' && packet.getType()!='v' && packet.getType()!='z')
		{
			System.arraycopy(packet.getValue(), 0, buffer, pos, packet.getValue().length);
		}
		return buffer;
	}
}
