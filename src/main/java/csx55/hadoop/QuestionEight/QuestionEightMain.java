package csx55.hadoop.QuestionEight;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import csx55.hadoop.Driver;

//Which artist is the most generic? Which artist is the most unique?
//approach: use the attributes similar_artists, artist_terms_freq, artist_familiarity, and artist_hottness

//if the artist is very familiar and has a high hottness, but a low number of similar artists and artist_terms_freq -> unique
//if the artist has a high number of similar artists and artist_terms_freq and is not familiar and has a low hottness -> generic

public class QuestionEightMain {
    
    private Job job;
    private String input;
    private String output;

    public QuestionEightMain(Job job, String input, String output){
        this.job = job;
        this.input = input;
        this.output = output;
    }

    public void answerQuestion(){
        job.setJarByClass(Driver.class);
        job.setMapperClass(MapperOne.class);
        //getJob().setCombinerClass(TaskOneReducer.class); //only add a combiner if needed
        job.setReducerClass(ReducerOne.class);
        job.setOutputKeyClass(Text.class); //expected key output of the reducer
        job.setOutputValueClass(Text.class); //TODO: check why this doesn't compile when set to FloatWritable.class
        try{
            FileInputFormat.addInputPath(job, new Path(input + "/metadata.txt"));
            FileOutputFormat.setOutputPath(job, new Path(output + "/q8"));
            job.waitForCompletion(true); // do this after all tasks have completed from main?
        } catch(InterruptedException | ClassNotFoundException | IOException e){
            System.out.println(e.getMessage());
        }
    }

}
