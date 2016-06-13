import java.io.IOException;
import java.util.*;
 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
 
public class sorting {
 
   
	  static ArrayList<Integer> bubbleSort(ArrayList <Integer> list)
      {
            int count = 0;
            for (int outer = 0; outer < list.size() - 1; outer++)
            {
                for (int inner = 0; inner < list.size()-outer-1; inner++)
                {
                  if (list.get(inner) > list.get(inner + 1))
                  {
                    swapEm(list, inner);
                    count = count + 1;
                  }
                }
              }
            return list;
      }
      static void swapEm(ArrayList<Integer>list, int inner)
      {
            int temp = list.get(inner);
            list.set(inner, list.get(inner + 1));
            list.set(inner + 1, temp);
      }
      
      
	 public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		 

		   public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		      
			

		        context.write(value, value);
		      
		    }
		  }

		  public static class Reduce extends Reducer<Text, Text, Text, Text> {

		  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			  ArrayList<Integer> result = new ArrayList<Integer>();
			  ArrayList<Integer> result_sorted = new ArrayList<Integer>();
			  
		      for (Text value : values){
		         result.add (Integer.parseInt(value.toString().trim()));
		      }
		      result_sorted = bubbleSort(result);
		      
		      StringBuilder out = new StringBuilder();
		      for (Object o : result_sorted)
		      {
		        out.append(o.toString());
		        out.append("\n");
		      }
		      
		      context.write(new Text(""), new Text(out.toString()));
		    }
		  }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
 
        Job job = new Job(conf, "sorting");
        job.setJarByClass(sorting.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
 
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
 
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
 
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
 
        job.waitForCompletion(true);
    }        
}