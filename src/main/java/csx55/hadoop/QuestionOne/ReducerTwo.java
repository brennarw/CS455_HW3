package csx55.hadoop.QuestionOne;

import java.io.IOException;

import javax.naming.Context;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerTwo extends Reducer<IntWritable, Text, Text, IntWritable> {

    private Text artistID = new Text();
    private IntWritable maxSongs = new IntWritable();
    
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text val : values) {
            String[] pair = val.toString().split("\\t");
            String tempArtistID = pair[0];
            int songCount = Integer.parseInt(pair[1]);

            if(songCount > maxSongs.get()){
                artistID.set(tempArtistID);
                maxSongs.set(songCount);
                //context.write(artistID, maxSongs);
            }
        }

    context.write(artistID, maxSongs);
    
    }

    //cleanup runs only once after all reduce tasks have completed
    // protected void cleanup(Context context) throws IOException, InterruptedException {
    // }
}