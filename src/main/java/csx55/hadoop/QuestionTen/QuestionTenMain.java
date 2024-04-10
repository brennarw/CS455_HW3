package csx55.hadoop.QuestionTen;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import csx55.hadoop.Driver;

// min year:       1926
// max year:       2010
//84 years - 8 subsets of years: ~10 years each
/*have 9 reducers
 * reducer 1 key: 1926
 * reducer 2 key: 1936
 * reducer 3 key: 1946
 * reducer 4 key: 1956
 * reducer 5 key: 1966
 * reducer 6 key: 1976
 * reducer 7 key: 1986
 * reducer 8 key: 1996
 * reducer 9 key: 2006
*/

// How do musical characteristics, such as tempo, key signature, and energy level, vary across different   
//     decades, and are there any discernible trends or shifts in musical styles over time?
//     -use year, song hottness, pitch, tempo, and artist terms

public class QuestionTenMain {
    
    private Job job;
    private String input;
    private String output;

    public QuestionTenMain(Job job, String input, String output){
        this.job = job;
        this.input = input;
        this.output = output;
    }

    public void answerQuestion(){
        job.setJarByClass(Driver.class);
        job.setMapperClass(MapperOne.class);
        //getJob().setCombinerClass(TaskOneReducer.class); //only add a combiner if needed
        job.setReducerClass(ReducerOne.class);
        job.setOutputKeyClass(Text.class); 
        job.setOutputValueClass(Text.class); 
        try{
            FileInputFormat.addInputPath(job, new Path(input + "/combined_data.txt"));
            FileOutputFormat.setOutputPath(job, new Path(output + "/q10"));
            job.waitForCompletion(true); // do this after all tasks have completed from main?
        } catch(InterruptedException | ClassNotFoundException | IOException e){
            System.out.println(e.getMessage());
        }
    }

}
