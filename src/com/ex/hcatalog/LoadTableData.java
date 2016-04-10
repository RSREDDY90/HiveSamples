package com.ex.hcatalog;

import java.util.Iterator;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hive.hcatalog.data.transfer.HCatReader;
import org.apache.hive.hcatalog.data.transfer.HCatWriter;
import org.apache.hive.hcatalog.data.transfer.ReadEntity;
import org.apache.hive.hcatalog.data.transfer.ReaderContext;
import org.apache.hive.hcatalog.data.transfer.WriteEntity;
import org.apache.hive.hcatalog.data.transfer.WriterContext;
import org.junit.Assert;
import org.junit.Test;
public class LoadTableData {

	/**
	 * @param args
	 * @throws HCatException 
	 */
	public static void main(String[] args) throws HCatException {

		Map<String, String> map = new HashMap<String, String>();
		ReadEntity.Builder builder = new ReadEntity.Builder();
		ReadEntity entity = builder.withDatabase("jdbc:hive2://quickstart.cloudera:10000/default").withTable("employee").build();
		HCatReader reader = DataTransferFactory.getHCatReader(entity, map);
	    ReaderContext cntxt = reader.prepareRead();
	    for(int i=0;i<cntxt.numSplits();i++){

	    	runsInSlave(cntxt,i);
	    }
	    
	    readTableData();
	    
	}
	private static void readTableData() throws HCatException {

		ReadEntity.Builder builder=new ReadEntity.Builder();
		  ReadEntity entity=builder.withDatabase("default").withTable("tbl_spark_out").build();
		  Map<String,String> config=new HashMap<String,String>();
		  HCatReader reader=DataTransferFactory.getHCatReader(entity,config);
		  ReaderContext cntxt=reader.prepareRead();
		  for (int i=0; i < cntxt.numSplits(); ++i) {
		    HCatReader splitReader=DataTransferFactory.getHCatReader(cntxt,i);
		    Iterator<HCatRecord> itr1=splitReader.read();
		    while (itr1.hasNext()) {
		      HCatRecord record=itr1.next();
		      System.out.println(record.getAll());
		    }
		  }
	}
	private static void runsInSlave(ReaderContext cntxt, int slaveNum) throws  HCatException {

	    HCatReader reader = DataTransferFactory.getHCatReader(cntxt, slaveNum);
	    Iterator<HCatRecord> itr = reader.read();
	    int i = 1;
	    while (itr.hasNext()) {
	      HCatRecord read = itr.next();
	      HCatRecord written = getRecord(i++);
	      System.out.println("data is:"+read.getAll());
	      
	      
	      /*// Argh, HCatRecord doesnt implement equals()
	      Assert.assertTrue("Read: " + read.get(0) + "Written: " + written.get(0),
	        written.get(0).equals(read.get(0)));
	      Assert.assertTrue("Read: " + read.get(1) + "Written: " + written.get(1),
	        written.get(1).equals(read.get(1)));
	      Assert.assertEquals(2, read.size());*/
	    }
	    
	    

}
	
	 private static HCatRecord getRecord(int i) {
		    List<Object> list = new ArrayList<Object>(2);
		    list.add("Row #: " + i);
		    list.add(i);
		    return new DefaultHCatRecord(list);
		  }
}