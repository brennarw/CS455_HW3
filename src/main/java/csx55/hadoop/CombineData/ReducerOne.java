package csx55.hadoop.CombineData;

import java.io.IOException;

import javax.management.RuntimeErrorException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ReducerOne extends Reducer<Text, Text, Text, Text> {

    //values has 
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String[] combinedData = new String[2];
        int count = 0;
        
        for(Text val : values){
            count++;
            if(count > 2){
                throw new RuntimeException("Error: too many elements");
            } else{
                if(val.toString().substring(0, 1).equals("A")){
                    combinedData[0] = val.toString();
                }else{
                    combinedData[1] = val.toString();
                }
            } 
        }

        String finalValue = key.toString() + combinedData[0].substring(1) + combinedData[1].substring(1);
        // System.out.println(finalValue);
        // System.exit(0);
        context.write(null, new Text(finalValue));

    }

    //cleanup runs only once after all reduce tasks have completed
    protected void cleanup(Context context) throws IOException, InterruptedException {

    }
}