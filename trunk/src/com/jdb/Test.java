package com.jdb;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.amef.AMEFDecodeException;
import com.amef.AMEFEncodeException;
import com.amef.JDBDecoder;
import com.amef.JDBObject;

public class Test
{

	/**
	 * @param args
	 */
	static class Temp implements Serializable
	{
		/**
		 * 
		 */
		public Temp()
		{
			
		}
		//private static final long serialVersionUID = -7070060949055791230L;
		int id;
		String name;
	}
	
	public static void main1(String[] args) throws IOException
	{	
		OutputStream os = new FileOutputStream("terr",true);
		long st1 = System.currentTimeMillis();
		for (int i = 0; i < 1000; i++)
		{
			os.write("asdasdasdasdasdasdasdfsdfsdfsdfsdfsdfsdfsdsdsdsdsdsdsdfsdfsdfsdfsdfsdfsdflheo;hqo;ehqwo;ehqwo;ehqwoehqwoehqwo;ehqwo;ehqwohsdasdasdasdasd".getBytes());
			os.flush();
		}
		System.out.println("total time is = "+(System.currentTimeMillis()-st1));
		os.close();
		
		BufferedWriter wr = new BufferedWriter(new FileWriter("terr",true));
		st1 = System.currentTimeMillis();
		for (int i = 0; i < 1000; i++)
		{
			wr.write("asdasdasdasdasdasdasdfsdfsdfsdfsdfsdfsdfsdsdsdsdsdsdsdfsdfsdfsdfsdfsdfsdflheo;hqo;ehqwo;ehqwo;ehqwoehqwoehqwo;ehqwo;ehqwohsdasdasdasdasd");
			wr.flush();
		}
		System.out.println("total time is = "+(System.currentTimeMillis()-st1));
		wr.close();
	}
	
	public static void main(String[] args) throws Exception
	{	
		//new File("jdb").delete();
		JDBObject onk = new JDBObject();
		onk.addPacket(new Date());
		new JDBDecoder().decodeB(JdbResources.getEncoder().encodeWL(onk, true), false, true);
		String[] misc = null;
		String[] colTypesNames =  new String[]{"id|int","name|string","date1|string","flag|boolean","ind|char"};
		Map<String,String> mappi = new HashMap<String,String>();
		mappi.put("PK", "id");
		//mappi.put("id", "id");
		//mappi.put("name", "name");
		
		DBManager.createDatabase("temp", "temp", "temp");
		DBManager.createTable("temp", "temp1", "temp", "temp", colTypesNames, misc, mappi, 1);
		DBManager.createTable("temp", "temp2", "temp", "temp", colTypesNames, misc, null, 1);
		DBManager.createTable("temp", "temp3", "temp", "temp", colTypesNames, misc, null, 1);
		
		BulkConnection con = ConnectionManager.getConnection() ;
		/*List<Temp> objects = con.selectAll("temp", "temp", null);
		System.out.println(objects);
		Temp t = new Temp();
		t.id = 1;
		t.name = "name";*/
		JDBObject object = new JDBObject();
		object.addPacket(1);
		object.addPacket("namsfsfe");
		object.addPacket("11-12-2010 12:12:12");
		object.addPacket(true);
		object.addPacket('Y');
		
		/*object.addPacket(12);
		object.addPacket("namsfsfe");
		object.addPacket("11-12-2010 12:12:12");
		object.addPacket(true);
		object.addPacket('Y');
		
		object.addPacket(12);
		object.addPacket("namsfsfe");
		object.addPacket("11-12-2010 12:12:12");
		object.addPacket(true);
		object.addPacket('Y');
		
		object.addPacket(12);
		object.addPacket("namsfsfe");
		object.addPacket("11-12-2010 12:12:12");
		object.addPacket(true);
		object.addPacket('Y');
		
		object.addPacket(12);
		object.addPacket("namsfsfe");
		object.addPacket("11-12-2010 12:12:12");
		object.addPacket(true);
		object.addPacket('Y');
		
		object.addPacket(12);
		object.addPacket("namsfsfe");
		object.addPacket("11-12-2010 12:12:12");
		object.addPacket(true);
		object.addPacket('Y');
		
		object.addPacket(12);
		object.addPacket("namsfsfe");
		object.addPacket("11-12-2010 12:12:12");
		object.addPacket(true);
		object.addPacket('Y');
		
		object.addPacket(12);
		object.addPacket("namsfsfe");
		object.addPacket("11-12-2010 12:12:12");
		object.addPacket(true);
		object.addPacket('Y');
		
		object.addPacket(12);
		object.addPacket("namsfsfe");
		object.addPacket("11-12-2010 12:12:12");
		object.addPacket(true);
		object.addPacket('Y');
		
		object.addPacket(12);
		object.addPacket("namsfsfe");
		object.addPacket("11-12-2010 12:12:12");
		object.addPacket(true);
		object.addPacket('Y');
		
		object.addPacket(12);
		object.addPacket("namsfsfe");
		object.addPacket("11-12-2010 12:12:12");
		object.addPacket(true);
		object.addPacket('Y');
		
		object.addPacket(12);
		object.addPacket("namsfsfe");
		object.addPacket("11-12-2010 12:12:12");
		object.addPacket(true);
		object.addPacket('Y');
		
		object.addPacket(12);
		object.addPacket("namsfsfe");
		object.addPacket("11-12-2010 12:12:12");
		object.addPacket(true);
		object.addPacket('Y');
		
		object.addPacket(12);
		object.addPacket("namsfsfe");
		object.addPacket("11-12-2010 12:12:12");
		object.addPacket(true);
		object.addPacket('Y');*/
		List<JDBObject> objects1 = new ArrayList<JDBObject>();
		long sti = System.currentTimeMillis();
		object = new JDBObject();
		object.addPacket(101);
		object.addPacket("namsfsfe1");
		object.addPacket("12122010 121212");
		object.addPacket(true);
		object.addPacket('Y');
		objects1.add(object);
		for(int i=0;i<100;i++)
		{
			object = new JDBObject();
			object.addPacket(i+1);
			object.addPacket("namsfsfe");
			object.addPacket("11122010 121212");
			object.addPacket(true);
			object.addPacket('Y');
			objects1.add(object);
		}
		
		con.bulkInsert("temp", "temp1",objects1);
		System.out.println("Time for Indexed Inserts = "+(System.currentTimeMillis()-sti));
		
		sti = System.currentTimeMillis();
		objects1.clear();
		for(int i=0;i<100;i++)
		{
			object = new JDBObject();
			object.addPacket(i+1);
			object.addPacket("namsfsfe");
			object.addPacket("11122010 121212");
			object.addPacket(true);
			object.addPacket('Y');
			objects1.add(object);
		}
		object = new JDBObject();
		object.addPacket(101);
		object.addPacket("namsfsfe2");
		object.addPacket("11122010 121212");
		object.addPacket(true);
		object.addPacket('Y');
		objects1.add(object);
		con.bulkInsert("temp", "temp2",objects1);
		System.out.println("Time for Non-indexed Inserts = "+(System.currentTimeMillis()-sti));
		
		sti = System.currentTimeMillis();
		objects1.clear();
		for(int i=0;i<100;i++)
		{
			object = new JDBObject();
			object.addPacket(i+1);
			object.addPacket("namsfsfe");
			object.addPacket("11122010 121212");
			object.addPacket(true);
			object.addPacket('Y');
			objects1.add(object);
		}
		object = new JDBObject();
		object.addPacket(101);
		object.addPacket("namsfsfe2");
		object.addPacket("11122010 121212");
		object.addPacket(true);
		object.addPacket('Y');
		objects1.add(object);
		con.bulkInsert("temp", "temp3",objects1);
		System.out.println("Time for Non-indexed Inserts = "+(System.currentTimeMillis()-sti));
		objects1 = null;
		/*object = new JDBObject();
		object.addPacket(13);
		object.addPacket("saumil");
		object.addPacket("11-12-2010 12:12:12");
		object.addPacket(true);
		object.addPacket('Y');
		objects1.add(object);
		
		object = new JDBObject();
		object.addPacket(11);
		object.addPacket("saumil");
		object.addPacket("11-12-2010 12:12:12");
		object.addPacket(true);
		object.addPacket('Y');
		objects1.add(object);
		
		object = new JDBObject();
		object.addPacket(14);
		object.addPacket("saumil1");
		object.addPacket("11-12-2010 12:12:12");
		object.addPacket(true);
		object.addPacket('Y');
		objects1.add(object);*/
		/*System.out.println("Inserts start");
		long st = System.currentTimeMillis();
		for(int i=0;i<100000;i++)
		{
			con.insert("temp", "temp",object);
		}
		con.commit();
		System.out.println("Time reqd for inserts = "+(System.currentTimeMillis()-st));
		*/
		//System.out.println("Bulk Inserts start");
		//Thread.sleep(10000);
		DBManager.getDBManager();
		Queue<Object> q = new ConcurrentLinkedQueue<Object>();
		/*long st = System.currentTimeMillis();
		con.selectAMEFObjectsb("temp", "temp1", new String[]{"*"}, q , "", false);
		long st1 = System.currentTimeMillis();
		System.out.println("\nTime required to fetch indexed data = "+(st1-st)+" "+(q.size()-3));
				
		q = new ConcurrentLinkedQueue<Object>();
		st = System.currentTimeMillis();
		con.selectAMEFObjectsb("temp", "temp2", new String[]{"*"}, q , "", false);
		st1 = System.currentTimeMillis();
		System.out.println("Time required to fetch non-indexed data = "+(st1-st)+" "+(q.size()-3));*/
				
		/*q = new ConcurrentLinkedQueue<Object>();		
		long st = System.currentTimeMillis();
		con.selectQuery(q,"select * from temp1,temp2 group by temp1.name,temp2.id");
		long st1 = System.currentTimeMillis();
		System.out.println("Time required to fetch distinct data for temp1 = "+(st1-st)+" "+(q.size()));
				
		q = new ConcurrentLinkedQueue<Object>();
		st = System.currentTimeMillis();
		con.selectQuery(q,"select ucase(name),lcase(name),mid(name,1,4),len(name),round(id,1),now(),sysdate(),format(date1,'dd-mm-yyyy') from temp1");
		st1 = System.currentTimeMillis();
		System.out.println("Time required to fetch distinct data for temp1 = "+(st1-st)+" "+(q.size()));
		
		q = new ConcurrentLinkedQueue<Object>();
		st = System.currentTimeMillis();
		con.selectQuery(q,"select first(name),last(id),sum(id),count(name),max(id),min(id),avg(id),max(name),min(name),name,ucase(name) from temp1 group by name,id");
		st1 = System.currentTimeMillis();
		System.out.println("Time required to fetch distinct data for temp1 = "+(st1-st)+" "+(q.size()));
		
		q = new ConcurrentLinkedQueue<Object>();
		st = System.currentTimeMillis();
		con.selectQuery(q,"select first(name),last(id),sum(id),count(name),max(id),min(id),avg(id),max(name),min(name) from temp1");
		st1 = System.currentTimeMillis();
		System.out.println("Time required to fetch distinct data for temp1 = "+(st1-st)+" "+(q.size()));
		
		q = new ConcurrentLinkedQueue<Object>();
		st = System.currentTimeMillis();
		con.selectQuery(q,"select * from temp1,temp2");
		st1 = System.currentTimeMillis();
		System.out.println("Time required to fetch joined data = "+(st1-st)+" "+(q.size()));		
		
		q = new ConcurrentLinkedQueue<Object>();
		st = System.currentTimeMillis();
		con.selectQuery(q,"select * from temp1 a,temp2 b");
		st1 = System.currentTimeMillis();
		System.out.println("Time required to fetch aliased joined data = "+(st1-st)+" "+(q.size()));
				
		q = new ConcurrentLinkedQueue<Object>();
		st = System.currentTimeMillis();
		con.selectQuery(q,"select a.id,b.name from temp1 a,temp2 b");
		st1 = System.currentTimeMillis();
		System.out.println("Time required to fetch aliased column joined data = "+(st1-st)+" "+(q.size()));
				
		q = new ConcurrentLinkedQueue<Object>();
		st = System.currentTimeMillis();
		con.selectQuery(q,"select a.id,b.name from temp1 a,temp2 b where a.name=b.name");
		long st2 = System.currentTimeMillis();
		System.out.println("Time required to fetch filtered aliased column joined data = "+(st2-st)+" "+(q.size()));
		
		q = new ConcurrentLinkedQueue<Object>();
		st = System.currentTimeMillis();
		con.selectQuery(q,"select a.id,b.name from temp1 a inner join temp2 b on a.name=b.name");
		st2 = System.currentTimeMillis();
		System.out.println("Time required to fetch inner joined filtered aliased column joined data = "+(st2-st)+" "+(q.size()));
		
		q = new ConcurrentLinkedQueue<Object>();
		st = System.currentTimeMillis();
		con.selectQuery(q,"select a.id,b.name from temp1 a full join temp2 b on a.name=b.name");
		st2 = System.currentTimeMillis();
		System.out.println("Time required to fetch full joined filtered aliased column joined data = "+(st2-st)+" "+(q.size()));
		
		q = new ConcurrentLinkedQueue<Object>();
		st = System.currentTimeMillis();
		con.selectQuery(q,"select a.id,b.name from temp1 a left join temp2 b on a.name=b.name");
		st2 = System.currentTimeMillis();
		System.out.println("Time required to fetch left joined filtered aliased column joined data = "+(st2-st)+" "+(q.size()));
		
		q = new ConcurrentLinkedQueue<Object>();
		st = System.currentTimeMillis();
		con.selectQuery(q,"select a.id,b.name from temp1 a right join temp2 b on a.name=b.name");
		st2 = System.currentTimeMillis();
		System.out.println("Time required to fetch right joined filtered aliased column joined data = "+(st2-st)+" "+(q.size()));
		
		q = new ConcurrentLinkedQueue<Object>();
		st = System.currentTimeMillis();
		con.selectQuery(q,"select count(temp1.name),count(temp2.id) from temp1,temp2");
		st2 = System.currentTimeMillis();
		System.out.println("Time required to fetch right joined filtered aliased column joined data = "+(st2-st)+" "+(q.size()));
		
		q = new ConcurrentLinkedQueue<Object>();
		st = System.currentTimeMillis();
		con.selectQuery(q,"select * from temp1,temp2,temp3 order by temp1.id,temp2.ind asc");
		long st3 = System.currentTimeMillis();
		System.out.println("Time required to fetch right joined filtered aliased column joined data = "+(st3-st)+" "+(q.size()));*/
				
		q = new ConcurrentLinkedQueue<Object>();
		long st5 = System.currentTimeMillis();
		con.selectQuery(q,"select count(temp1.name),max(temp1.name),last(temp2.id),count(temp2.id),temp1.name,temp3.id from temp1,temp2,temp3 group by temp1.name,temp3.id having temp1.name='namsfsfe'");
		long st34 = System.currentTimeMillis();
		System.out.println("Time required to fetch right joined filtered aliased column joined data = "+(st34-st5)+" "+(q.size()));
		
		/*q = new ConcurrentLinkedQueue<Object>();
		st = System.currentTimeMillis();
		con.selectQuery(q,"select * from temp1 left join temp2 on temp1.name=temp2.name left join temp3 on temp1.name=temp3.name");
		//con.selectQuery(q,"select count(temp1.name),count(temp2.id),temp1.name,temp2.id from temp1,temp2,temp3 group by temp1.name,temp2.id");
		st3 = System.currentTimeMillis();
		System.out.println("Time required to fetch right joined filtered aliased column joined data = "+(st3-st)+" "+(q.size()));
			
		//con.insert("temp", "temp",object);
		//con.bulkInsert("temp", "temp1",objects1);
		/*for(int i=0;i<100000;i++)
		{
			con.insert("temp", "temp",object);
		}*/
		//con.bulkInsert("temp", "temp1",objects1);
		//con.commit();
		//System.out.println("Time reqd for bulkinserts = "+(System.currentTimeMillis()-st1));		
		
		//System.out.println("Selects start");
		//long st2 = System.currentTimeMillis();
		//List<JDBObject> objectsa = con.selectAMEFObject("temp", "temp", null);
		//System.out.println(objectsa.size()+"\nTime reqd for selects = "+(System.currentTimeMillis()-st2));
		
		DBManager.shutdown();		
}

}
