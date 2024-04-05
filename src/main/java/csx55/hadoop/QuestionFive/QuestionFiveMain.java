package csx55.hadoop.QuestionFive;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import csx55.hadoop.Driver;

//what is the longest song(s)? The shortest song(s)? The song(s) of median length?
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
        job.setOutputKeyClass(Text.class); //the text is the dataset we are reading in?
        job.setOutputValueClass(FloatWritable.class);
        try{
            FileInputFormat.addInputPath(job, new Path(input + "/combined_data.txt"));
            FileOutputFormat.setOutputPath(job, new Path(output + "/q5"));
            job.waitForCompletion(true); // do this after all tasks have completed from main?
        } catch(InterruptedException | ClassNotFoundException | IOException e){
            System.out.println(e.getMessage());
        }
    }

}
