package csx55.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


//TODO: need to combine both datasets into one big one!

public class MapReduce {

    private Configuration conf = new Configuration();
    private Job job;

    public Configuration getConfiguration(){
        return conf;
    }

    public Job getJob(){
        return job;
    }

    public void setJob(String jobName){
        try{
            this.job = Job.getInstance(getConfiguration(), jobName);
        } catch(IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public void taskOne(String input, String output){
        setJob("QuestionOneJob");
        getJob().setJarByClass(MapReduce.class);
        getJob().setMapperClass(Task1Mapper.class);
        //getJob().setCombinerClass(TaskOneReducer.class); //only add a combiner if needed
        getJob().setReducerClass(Task1Reducer.class);
        getJob().setOutputKeyClass(Text.class); //the text is the dataset we are reading in?
        getJob().setOutputValueClass(IntWritable.class);
        try{
            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path(output));
        } catch(IOException e){
            System.out.println(e.getMessage());
        }
        //System.exit(job.waitForCompletion(true) ? 0 : 1); // do this after all tasks have completed from main?
    }

    public void taskTwo(String input, String output) {
        setJob("QuestionTwoJob");
        getJob().setJarByClass(MapReduce.class);
        getJob().setMapperClass(Task2Mapper.class);
        //getJob().setCombinerClass(TaskOneReducer.class); //only add a combiner if needed
        getJob().setReducerClass(Task2Reducer.class);
        getJob().setOutputKeyClass(Text.class); //the text is the dataset we are reading in?
        getJob().setOutputValueClass(IntWritable.class);
        try{
            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path(output));
        } catch(IOException e){
            System.out.println(e.getMessage());
        }
        //System.exit(job.waitForCompletion(true) ? 0 : 1); // do this after all tasks have completed from main?
    }

    public static void main(String[] args) throws Exception{

        if(args.length != 2){
            System.exit(0); 
        }

        String input = args[0]; //this is the path to where to find the dataset
        String output = args[1]; //this is the path to where to store the output

        MapReduce mapReduce = new MapReduce();

        try{
            mapReduce.taskOne(input + "/metadata.txt", output + "/q1");
            mapReduce.taskTwo(input + "/analysis.txt", output + "/q2"); //need to grab the songID from analysis then find the associated artistID from metadata.txt
        } catch(Exception e){
            System.out.println(e.getMessage());
        }


    }

}