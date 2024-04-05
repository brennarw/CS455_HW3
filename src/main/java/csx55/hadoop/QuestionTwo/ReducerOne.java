package csx55.hadoop.QuestionTwo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerOne extends Reducer<Text, IntWritable[], Text, IntWritable> {

    private Text artistID = new Text();
    private IntWritable maxAverageLoudness = new IntWritable();
    
    @Override
    protected void reduce(Text key, Iterable<IntWritable[]> values, Context context) throws IOException, InterruptedException {

        int loudness = 0;
        int songCount = 0;

        for (IntWritable[] val : values) {
            songCount += val[0].get();
            loudness += val[1].get();
        }

        int loudnessAverage = loudness/songCount;
        
        if(loudnessAverage > maxAverageLoudness.get()){
            artistID.set(key);
            maxAverageLoudness.set(loudnessAverage);
        }
        
    }
    
    //cleanup runs only once after all reduce tasks have completed
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(artistID, maxAverageLoudness);
    }
}