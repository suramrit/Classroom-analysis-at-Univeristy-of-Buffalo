// Contributors
// Avijeet Mishra avijeetm@buffalo.edu 50169242
// Suramrit Singh suramrit@buffalo.edu 50169918
// Insight 5: Find the hall and classroom for each department that has 
// maximum number of students enrolled in Spring 2016. 
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

public class Question5{

  public static class Mapper1
       extends Mapper<Object, Text, Text, IntWritable>{

    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	String[] fields=value.toString().split(","); 
    	String dept = fields[4].trim();
    	String class_no = new String();
    	if(fields[5].split(" ").length == 1){

    		class_no = fields[5].split(" ")[0];
   		}
   		else 
   			class_no = fields[5].split(" ")[1];
      	String hall = fields[5].split(" ")[0];
      if(fields[3].contains("Spring 2016") && Integer.parseInt(fields[12])>0 ){
        
        String wrd = dept+"_"+ hall + "_" +class_no; 
        word.set(wrd);
        context.write(word,new IntWritable(Integer.parseInt(fields[12])));
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
	String dept=fields[0].split("_")[0];
  	String hall = fields[0].split("_")[1];
  	String class_no = fields[0].split("_")[2];
    
    String val = class_no+"_"+fields[1];
    String ke = dept+"_"+hall;
 	word.set(ke);
 	val2.set(val);
 	context.write(word,val2);
}
}


public static class Reducer2
  extends Reducer<Text,Text,Text,Text> {

public void reduce(Text key, Iterable<Text> values,
                  Context context
                  ) throws IOException, InterruptedException {



	String ke = (key.toString()).split("_")[0];
	Text key_text = new Text(ke);
  	Text val = new Text();
	Iterator<Text> iterator=values.iterator();
	String value1 = new String();
  	int max = 0;
  	String class_list = new String();
  	String hall = (key.toString()).split("_")[1];
	if(iterator.hasNext()==true)
	  {
		value1=iterator.next().toString();
    	max = Integer.parseInt(value1.split("_")[1]);
    
   	 	class_list = hall +":"+"Room number-"+ value1.split("_")[0]+","+"Enrolled students-"+max;  
  	}
	else
    {
      return;
    }
	while(iterator.hasNext()){

        String value2=iterator.next().toString();
	     int max_temp = Integer.parseInt(value2.split("_")[1]);
	     if(max_temp> max)
		{    
   	 		class_list = hall +":"+"Room number-"+ value2.split("_")[0]+","+"Enrolled students-"+max_temp;  
   	 	}
	}
  val.set(class_list);
  context.write(key_text, val);
  


}
}
  public static void main(String[] args) throws Exception {
	String temp="Temp";
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "job1");
    job.setJarByClass(Question5.class);
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
    job2.setJarByClass(Question5.class);
    job2.setMapperClass(Mapper2.class);
    job2.setReducerClass(Reducer2.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job2, new Path("Temp"));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
    System.exit(job2.waitForCompletion(true) ? 0 : 1); 
    
  }
}
