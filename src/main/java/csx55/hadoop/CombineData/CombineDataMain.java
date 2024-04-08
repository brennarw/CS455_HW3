package csx55.hadoop.CombineData;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import csx55.hadoop.Driver;

public class CombineDataMain {
    
    private Job job;
    private String input;
    private String output;

    public CombineDataMain(Job job, String input, String output){
        this.job = job;
        this.input = input;
        this.output = output;
    }

    public void combine(){
        job.setJarByClass(Driver.class);
        //job.setMapperClass(MapperOne.class);
        //getJob().setCombinerClass(TaskOneReducer.class); //only add a combiner if needed
        job.setReducerClass(ReducerOne.class);
        job.setOutputKeyClass(Text.class); //expected key output of the reducer
        job.setOutputValueClass(Text.class); //TODO: check why this doesn't compile when set to FloatWritable.class
        try{
            MultipleInputs.addInputPath(job, new Path(input + "/analysis.txt"), TextInputFormat.class , MapperOne.class);
            MultipleInputs.addInputPath(job, new Path(input + "/metadata.txt"), TextInputFormat.class , MapperTwo.class);
            FileOutputFormat.setOutputPath(job, new Path("/hw3DataSets/combined"));
            job.waitForCompletion(true); // do this after all tasks have completed from main?
        } catch(InterruptedException | ClassNotFoundException | IOException e){
            System.out.println(e.getMessage());
        }
    }

}
