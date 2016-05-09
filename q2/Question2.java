// Contributors
// Avijeet Mishra avijeetm@buffalo.edu 50169242
// Suramrit Singh suramrit@buffalo.edu 50169918
// Insight 2: For each hall at the university, finding the number of years
// it took to get a 10% in capacity. 
// input file: classchedule.csv

import java.io.IOException;
import java.util.Iterator;
import java.lang.String;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Question2 {

  public static class Mapper1
       extends Mapper<Object, Text, Text, IntWritable>{

    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	String[] fields=value.toString().split(",");
    	if(fields[2].equals("Unknown")||fields[7].equals("")||!StringUtils.isNumeric(fields[7]))
		return;
	String wrd = fields[2].split(" ")[0]+" "+fields[1].split(" ")[1];
    	word.set(wrd);
    	context.write(word,new IntWritable(Integer.parseInt(fields[7])));
    }
  }

  public static class Reducer1
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	int sum=0;
    	for(IntWritable val:values){
    		sum+=val.get();
    	}
    	result.set(sum);
    	context.write(key,result);
    }
  }


//same as q1


  public static class Mapper2
  extends Mapper<Object, Text, Text, IntWritable>{
  	Text word=new Text();

public void map(Object key, Text value, Context context
               ) throws IOException, InterruptedException {
	String[] fields=value.toString().split("\\t"); 
	String year=fields[0].split(" ")[1];
	int yr = Integer.parseInt(year);
	if(yr >= 1995){
	
	String wrd = fields[0].split(" ")[0]+" 1995:2015";
 	word.set(wrd);
    	context.write(word,new IntWritable(Integer.parseInt(fields[1])));


	}
	
	

}
}

public static class Reducer2
  extends Reducer<Text,IntWritable,Text,IntWritable> {

public void reduce(Text key, Iterable<IntWritable> values,
                  Context context
                  ) throws IOException, InterruptedException {
	String ke = (key.toString()).split(" ")[0];
	Text key_text = new Text(ke);
	Iterator<IntWritable> iterator=values.iterator();
	int value1 = 0;

	if(iterator.hasNext()==true)
		value1=iterator.next().get();
	else
		return;
	
	int count = 0;
	while(iterator.hasNext()){

        int value2=iterator.next().get();
	
		count++;
		if(value2 >= (int) ((0.1*value1) + value1)){

		context.write(key_text,new IntWritable(count));
		break; 
		}

	}
}
}
  public static void main(String[] args) throws Exception {
	String temp="Temp";
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "job1");
    job.setJarByClass(Question2.class);
    job.setMapperClass(Mapper1.class);
    job.setCombinerClass(Reducer1.class);
    job.setReducerClass(Reducer1.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(temp));
    job.waitForCompletion(true);
    Configuration conf2 = new Configuration();
     Job job2 = Job.getInstance(conf2, "job2");
    job2.setJarByClass(Question2.class);
    job2.setMapperClass(Mapper2.class);
    job2.setReducerClass(Reducer2.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job2, new Path("Temp"));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
    
  }
}
