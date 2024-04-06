package csx55.hadoop.QuestionSeven;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import csx55.hadoop.Driver;

//Create segment data for the average song. Include start time, pitch, timbre, max loudness, max loudness time, and start loudness
public class QuestionFiveMain {
    
    private Job job;
    private String input;
    private String output;

    public QuestionFiveMain(Job job, String input, String output){
        this.job = job;
        this.input = input;
        this.output = output;
    }

    public void answerQuestion(){
        job.setJarByClass(Driver.class);
        job.setMapperClass(MapperOne.class);
        //getJob().setCombinerClass(TaskOneReducer.class); //only add a combiner if needed
        job.setReducerClass(ReducerOne.class);
        job.setOutputKeyClass(IntWritable.class); //expected key output of the reducer
        job.setOutputValueClass(Text.class); //TODO: check why this doesn't compile when set to FloatWritable.class
        try{
            FileInputFormat.addInputPath(job, new Path(input + "/combined_data.txt"));
            FileOutputFormat.setOutputPath(job, new Path(output + "/q7"));
            job.waitForCompletion(true); // do this after all tasks have completed from main?
        } catch(InterruptedException | ClassNotFoundException | IOException e){
            System.out.println(e.getMessage());
        }
    }

}
