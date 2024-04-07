package csx55.hadoop.QuestionSeven;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ReducerOne extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        DoubleWritable average = new DoubleWritable();
        double sum = 0;
        double count = 0;

        for(Text val : values){
            String[] segmentValues = val.toString().split(" ");
            for(int i = 1; i < segmentValues.length - 1; i++){ //when split, segmentValues[0] = "[" and segmentValues[length - 1] = "]"
                sum += Double.parseDouble(segmentValues[i]);
                count ++;
            }

        }
        average.set(sum/count);

        context.write(key, new Text(String.valueOf(average.get())));
    }
}