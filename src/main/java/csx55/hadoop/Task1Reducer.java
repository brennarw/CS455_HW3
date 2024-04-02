package csx55.hadoop;

import java.io.IOException;

import javax.naming.Context;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task1Reducer extends Reducer<Text,IntWritable,Text,IntWritable> {

    private Text artistID = new Text();
    private IntWritable maxSongs = new IntWritable();
    
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      if(sum > maxSongs.get()){
        artistID.set(key);
        maxSongs.set(sum);
      }
    }

    //cleanup runs only once after all reduce tasks have completed
    protected void cleanup(Context context) throws IOException, InterruptedException {
      context.write(artistID, maxSongs);
    }
}