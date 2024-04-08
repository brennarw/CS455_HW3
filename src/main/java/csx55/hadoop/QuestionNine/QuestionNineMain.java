package csx55.hadoop.QuestionNine;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import csx55.hadoop.Driver;

/* Imagine a song with a higher hotness score than the song in your answer to Q3. List this song's tempo, time signature, danceability, duration, 
    mode, energy, key, loudness, when it stops fading in, when it starts fading out, and which terms describe the artist who made it. Give both the song and artist
    who made it unique names.

    answer to question 3: SOAAXAK12A8C13C030

    hottness: 1.0

    tempo: 150.569

    time signature: 3

    danceability: 0.0

    duration: 145.05751

    mode: 1

    energy: 0.0

    key: 11

    loudness: -10.544

    stops fading in at: 2.223

    starts fading out: 140.132

    artist terms: 'blues-rock' 'heavy metal' 'hard rock' 'classic rock' 'psychedelic rock' 'progressive rock' 'southern rock' 'blues' 'oldies' 'stoner rock' 
            'ballad' 'soft rock' 'rock' 'garage rock' 'jam band' 'alternative rock' 'progressive metal' 'grunge' 'male vocalist' '70s' 'pop rock' 'british' 
            'guitar' 'funk' 'psychedelic' 'reggae' '60s' 'metal' 'alternative' 'soundtrack' 'jazz' 'folk' 'pop' 'experimental' 'indie rock' 'punk' 'indie'
*/

//approach: have a song that with the same exact variables but increase danceability and energy from 0.0 to 1.0
//include any songs that have a hottness, energy, and danceability of 1.0

public class QuestionNineMain {
    
    private Job job;
    private String input;
    private String output;

    public QuestionNineMain(Job job, String input, String output){
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
            FileInputFormat.addInputPath(job, new Path(input + "/combined_data.txt"));
            FileOutputFormat.setOutputPath(job, new Path(output + "/q9"));
            job.waitForCompletion(true); // do this after all tasks have completed from main?
        } catch(InterruptedException | ClassNotFoundException | IOException e){
            System.out.println(e.getMessage());
        }
    }

}
