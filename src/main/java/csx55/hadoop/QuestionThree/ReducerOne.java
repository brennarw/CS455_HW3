package csx55.hadoop.QuestionThree;

import java.io.IOException;

import javax.naming.Context;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerOne extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    private Text songID = new Text();
    private FloatWritable maxSongHottness = new FloatWritable();
    
    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (FloatWritable val : values) {
        sum += val.get();
      }
      if(sum > maxSongHottness.get()){
        songID.set(key);
        maxSongHottness.set(sum);
        //context.write(songID, maxSongHottness);
      }
    }

    //cleanup runs only once after all reduce tasks have completed
    protected void cleanup(Context context) throws IOException, InterruptedException {
      context.write(songID, maxSongHottness);
    }
}