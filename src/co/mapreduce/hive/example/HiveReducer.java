package co.mapreduce.hive.example;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;

public class HiveReducer extends Reducer<Text, IntWritable, WritableComparable, HCatRecord> {
	private IntWritable result = new IntWritable();
	@Override
    protected void reduce(Text key, Iterable<IntWritable> values,org.apache.hadoop.mapreduce.Reducer<Text, IntWritable,WritableComparable, HCatRecord>.Context context){
		
		
		int sum = 0;
		
		for (IntWritable val : values) {
			sum += val.get();
		}
	result.set(sum);
	//context.write(key, result);
	
	HCatRecord record = new DefaultHCatRecord(2);
    record.set(0, key);
    record.set(1, result);

    try {
		context.write(null, record);
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
	
	
	}
	
}
