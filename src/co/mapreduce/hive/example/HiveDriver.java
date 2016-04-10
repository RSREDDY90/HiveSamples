package co.mapreduce.hive.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class HiveDriver extends Configured implements Tool{

	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	
	@Override
	public int run(String[] args) throws Exception {
		return 0;
		
		
	}
	
	public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new HiveDriver(),
                     args);
        
        createHiveTable();
        System.exit(res);
 }

	private static void createHiveTable() throws SQLException, ClassNotFoundException {
		 Class.forName(driverName);
	      // get connection
	      Connection con = DriverManager.getConnection("jdbc:hive2://quickstart.cloudera:10000/default", "", "");
	      
	      // create statement
	      Statement stmt = con.createStatement();
	      
	      // execute statement
	      stmt.execute("CREATE TABLE IF NOT EXISTS "+" wordcount ( word String, count int)"+" COMMENT 'Word Count details'"+" ROW FORMAT DELIMITED"+" FIELDS TERMINATED BY '\t'"+" LINES TERMINATED BY '\n'"+" STORED AS TEXTFILE");
	         
	      System.out.println("Table employee created.");
	      con.close();
	      
	}
		

}
