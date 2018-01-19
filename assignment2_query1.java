package assignment2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class assignment2_query1 extends Thread {
	static int c_nextId =1;
	 static Connection s_conn = null;
	 static boolean greenLight = false;
	 static int count = 0;
	 private final String workWith;
	 
	 static synchronized void setGreenLight(){
		 greenLight = true;
	 }
	 
	 synchronized boolean getGreenLight(){
		 return greenLight;
	 }
	 
	 public assignment2_query1(String query){
		 super();
		 workWith = query;
	 }
	 
	 public static void main(String args[]){
		    try{
		       long startTime = System.currentTimeMillis();
		       Class.forName("org.postgresql.Driver");		       
		       int threadNumber = 6;
		       int queryNumber = 6;
		       
		       //create queries
		       String[] queries = new String[queryNumber];
		       queries[0] = "SELECT b.branch, r.lastname, count(*) from Reader r, Borrow_APurdy b where r.rid=b.rid GROUP by b.branch, r.lastname HAVING count(*) > 1000";
		       queries[1] = "SELECT b.branch, r.lastname, count(*) from Reader r, Borrow_APurdy b where r.rid=b.rid GROUP by b.branch, r.lastname HAVING count(*) > 1000";
		       queries[2] = "SELECT b.branch, r.lastname, count(*) from Reader r, Borrow_FMowat b where r.rid=b.rid GROUP by b.branch, r.lastname HAVING count(*) > 1000";
		       queries[3] = "SELECT b.branch, r.lastname, count(*) from Reader r, Borrow_LCohen b where r.rid=b.rid GROUP by b.branch, r.lastname HAVING count(*) > 1000";
		       queries[4] = "SELECT b.branch, r.lastname, count(*) from Reader r, Borrow_MAtwoord b where r.rid=b.rid GROUP by b.branch, r.lastname HAVING count(*) > 1000";
		       queries[5] = "SELECT b.branch, r.lastname, count(*) from Reader r, Borrow_MOndaatje b where r.rid=b.rid GROUP by b.branch, r.lastname HAVING count(*) > 1000";
		              
		       //create threads
		       Thread[] threadList = new Thread[threadNumber];
		       
		       //spawn threads
		       for(int i =0; i < threadNumber; i++){
		    	   threadList[i] = new assignment2_query1(queries[i]);
		    	   threadList[i].start();
		       }
		       //Start everyone at the same time
		       setGreenLight();
		       
		       // wait for all threads to end
		       for(int i=0; i < threadNumber; i++){
		    	   threadList[i].join();
		       }
		       
		       //show the results
		       long endTime = System.currentTimeMillis();
			   System.out.println(count);
			   System.out.println(startTime);
			   System.out.println(endTime);
			   System.out.println(endTime - startTime);
			} catch(Exception e) {
			  System.err.println(e); 
			  }
		    }  
	 public void run(){
		 try{
			 //get a connection
			 Connection   con  = DriverManager.getConnection("jdbc:postgresql://localhost:5432/dbgeo", "bigdata", "");
			 
			 //create a statement
			 Statement stmt = con.createStatement();
			 while(!getGreenLight()){
				 yield();
			 }
			 
			 //execute the query
			 ResultSet rs = stmt.executeQuery(workWith);
			 while(rs.next()){
				   count += rs.getInt(3);
				   System.out.println(rs.getString(1) + " " + rs.getString(2) + " " +rs.getString(3));
				   yield();
			 }
			 
			 //close the connections
			 rs.close();
			 stmt.close();
			 con.close();
		 }
		 catch(Exception e){
			 System.out.println(e);
		 }
	 }
}
