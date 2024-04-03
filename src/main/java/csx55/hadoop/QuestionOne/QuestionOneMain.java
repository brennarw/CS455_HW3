package csx55.hadoop.QuestionOne;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import csx55.hadoop.MapReduce;

public class QuestionOneMain {
    
    private Configuration conf = new Configuration();
    private Job job;
    private String input;
    private String output;

    public QuestionOneMain(Job job, String input, String output){
        this.job = job;
        this.input = input;
        this.output = output;
    }

    public void setJob(String jobName){
        try{
            this.job = Job.getInstance(conf, jobName);
        } catch(IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public void answerIntermediateQuestion(){
        job.setJarByClass(MapReduce.class);
        job.setMapperClass(MapperOne.class);
        //getJob().setCombinerClass(TaskOneReducer.class); //only add a combiner if needed
        job.setReducerClass(ReducerOne.class);
        job.setOutputKeyClass(Text.class); //the text is the dataset we are reading in?
        job.setOutputValueClass(IntWritable.class);
        try{
            FileInputFormat.addInputPath(job, new Path(input + "/metadata.txt"));
            FileOutputFormat.setOutputPath(job, new Path(output + "/q1/intermediate"));
            job.waitForCompletion(true); // do this after all tasks have completed from main?
        } catch(InterruptedException | ClassNotFoundException | IOException e){
            System.out.println(e.getMessage());
        }
    }

    public void answerFinalQuestion(){
        job.setJarByClass(MapReduce.class);
        job.setMapperClass(MapperTwo.class);
        //getJob().setCombinerClass(TaskOneReducer.class); //only add a combiner if needed
        job.setReducerClass(ReducerTwo.class);
        job.setOutputKeyClass(Text.class); //the text is the dataset we are reading in?
        job.setOutputValueClass(IntWritable.class);
        try{
            FileInputFormat.addInputPath(job, new Path("/hw3Output/q1/intermediate"));
            FileOutputFormat.setOutputPath(job, new Path(output + "/q1/final"));
            job.waitForCompletion(true); // do this after all tasks have completed from main?
        } catch(InterruptedException | ClassNotFoundException | IOException e){
            System.out.println(e.getMessage());
        }
    }
}
