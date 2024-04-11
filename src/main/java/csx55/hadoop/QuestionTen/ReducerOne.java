package csx55.hadoop.QuestionTen;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.Hashtable;


public class ReducerOne extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        double songCount = 0.0;
        double tempoSum = 0;
        double durationSum = 0;

        for(Text val : values){

            String[] songSpecifics = val.toString().split("\\|");
            songCount += Double.parseDouble(songSpecifics[0]);
            tempoSum += Double.parseDouble(songSpecifics[1]);
            durationSum += Double.parseDouble(songSpecifics[2]);

        }

        double tempoAverage = tempoSum / songCount;
        double durationAverage = durationSum / songCount;

        String yearValue = "Average tempo: " + String.valueOf(tempoAverage) + " | Average duration: " + String.valueOf(durationAverage);

        context.write(key, new Text(yearValue)); 


    }
}