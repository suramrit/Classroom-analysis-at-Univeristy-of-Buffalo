// Contributors
// Avijeet Mishra avijeetm@buffalo.edu 50169242
// Suramrit Singh suramrit@buffalo.edu 50169918
// Insight 4: Finding the total number of exams given in each time slot
// of 1 hour and categorize it with the number of exams in each department. 
// input file: bina_examschedule.tsv

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
import java.util.regex.Pattern;

public class Question4{

  public static class Mapper1
       extends Mapper<Object, Text, Text, IntWritable>{

    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	String[] fields=value.toString().split(","); 
    	String dept = fields[4].trim();
      String start_time = fields[8];
      String end_time = fields[9];
      int start_hr = Integer.parseInt(fields[8].split(":")[0]);
      int start_min = Integer.parseInt(fields[8].split(":")[1]);
      int end_hr = Integer.parseInt(fields[9].split(":")[0]);
      String end_min = fields[9].split(":")[1];
      
      if(fields[3].contains("Fall 2015") && Integer.parseInt(fields[12])>0 ){
        if(end_min != "00")
          end_hr++;
        while(start_hr < end_hr){

        String wrd = dept+"||"+start_hr + "_" + ++start_hr; 
        word.set(wrd);
        context.write(word,new IntWritable(Integer.parseInt(fields[12])));
      }
      }
      
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
  extends Mapper<Object, Text, Text, Text>{
  	Text word=new Text();
    Text val2=new Text();

public void map(Object key, Text value, Context context
               ) throws IOException, InterruptedException {
	String[] fields=value.toString().split("\\t"); 

	String dept=fields[0].split(Pattern.quote("||"))[0];
  String interval = fields[0].split(Pattern.quote("||"))[1];
  String val = new String();
    val = fields[1]+"_"+dept;
  word.set(interval);
  val2.set(val);
  context.write(word,val2);
}
}


public static class Reducer2
  extends Reducer<Text,Text,Text,Text> {

public void reduce(Text key, Iterable<Text> values,
                  Context context
                  ) throws IOException, InterruptedException {



	String ke = (key.toString()).split(" ")[0];
	Text key_text = new Text(ke);
  Text val = new Text();
	Iterator<Text> iterator=values.iterator();
	String value1 = new String();
  int sum = 0;
  String dept_list = new String();
	if(iterator.hasNext()==true)
  {
		value1=iterator.next().toString();
    sum = Integer.parseInt(value1.split("_")[0]);
    
    dept_list = sum +"_"+ value1.split("_")[1];  
  }
	else
    {
      return;
    }
	while(iterator.hasNext()){

        String value2=iterator.next().toString();
	      sum = sum+Integer.parseInt(value2.split("_")[0]);
        dept_list = dept_list + ","+value2.split("_")[0]+"_"+ value2.split("_")[1];  

	}
  dept_list = sum +":"+ dept_list;
  val.set(dept_list);
  context.write(key_text, val);
  



}
}
  public static void main(String[] args) throws Exception {
	String temp="Temp";
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "job1");
    job.setJarByClass(Question4.class);
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
    job2.setJarByClass(Question4.class);
    job2.setMapperClass(Mapper2.class);
    //job2.setCombinerClass(FindIncreaseReducer.class);
    job2.setReducerClass(Reducer2.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job2, new Path("Temp"));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
    System.exit(job2.waitForCompletion(true) ? 0 : 1); 
    
  }
}
