
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.util.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Main {
  public static class MyMapper extends Mapper<LongWritable, Text, Text, Text>{
	  double MAX_DIST = 5.0;
	  int MIN_POS = 4;
	  List<String[]> data = new ArrayList<String[]>();
	  List<int[]> result = new ArrayList<int[]>();// index of data , number of neighbors, class 
	  List<ArrayList<Integer>> neigh = new ArrayList<ArrayList<Integer>>();
	  public static int class_index  = 0;
	  interface toDouble {
		  double apply(String a);
	  }
	  // distance
	  double cal_dist(String x1,String y1,String x2,String y2) {
		  toDouble func = Double::parseDouble;
		  double dist = Math.pow(func.apply(x1)-func.apply(x2),2)+Math.pow(func.apply(y1)-func.apply(y2),2);
		  dist = Math.sqrt(dist);
		  return dist;
	  }
	  public void map(LongWritable key, Text value, Context context) 
           throws IOException, InterruptedException {
    	String[] stringArr = value.toString().split(",");
        data.add(stringArr);
        int res[] = new int[4];
        res[0] = data.size()-1;
        res[1] = 0;  
        res[2] = 0;  
        res[3] = -1; 
        ArrayList<Integer> nei = new ArrayList<Integer>(); 
        for(int j=0; j<result.size(); j++) {
        	int i[] = result.get(j);
        	double dist = cal_dist(data.get(i[0])[1],data.get(i[0])[2],stringArr[1],stringArr[2]);
        	if(dist<=MAX_DIST) {
        		i[1]++;
        		res[1]++;
        		neigh.get(j).add(data.size()-1);
        		nei.add(j);
        	}
        }
        neigh.add(nei);
        result.add(res);           
    }
    
    @Override
    protected void cleanup(Context ctx) throws IOException, InterruptedException {
        try {
    	HashMap<Integer,Integer> class_mapping = new HashMap<Integer,Integer>();
    	
        for(int j=0; j<result.size(); j++) {
        	int i[] = result.get(j);
        	if(i[1]>=MIN_POS) {
        		int c;
        		if(i[3]==-1) {
        			c = class_index;
        			class_index++;
        		}
        		else c = i[3];
        		class_mapping.put(c, c);
        		
        		for(int a: neigh.get(j)) {
        			int border_points[] = result.get(a);
        			if(border_points[2]<MIN_POS) {
        				if(border_points[3]==-1) border_points[3] = c;
        				else {
        					class_mapping.put(border_points[3],c);
        				}
        			}
        			else {
        				if(border_points[3]==-1) border_points[3] = c;
        				else {
        					class_mapping.put(border_points[3],c);
        				}
        			}
        		}
        	}
        }
    	for(int[] i: result) {
    		int c = i[3];
    		if(c==-1) {
    			ctx.write(new Text("-1"), new Text(data.get(i[0])[0]+','+data.get(i[0])[1]+","+data.get(i[0])[2]));
    		}
    		else {
    		while(class_mapping.get(c)!=c) c=class_mapping.get(c);
    		ctx.write(new Text(c+""), new Text(data.get(i[0])[0]+','+data.get(i[0])[1]+","+data.get(i[0])[2]));
    	
    		}
  
    	}
        }
        catch(Exception e) {
        	e.printStackTrace();
        }
    }
  public static void main(String[] args)  throws Exception{
    Configuration conf = new Configuration();
    
    Job job = Job.getInstance(conf, "WC");
    job.setJarByClass(Main.class);
    job.setMapperClass(MyMapper.class);
    job.setNumReduceTasks(0);
    //job.setReducerClass(MyReducer.class);
    //job.setPartitionerClass(MyPartitioner.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
}
