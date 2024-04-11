package csx55.hadoop.QuestionThree;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MapperOne extends Mapper<Object, Text, Text, FloatWritable> {

    private static FloatWritable songHottness = new FloatWritable();
    private Text songID = new Text();

    //this mapper needs to find each unique artistID and write each instance found to the reducer
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] songAttributes = value.toString().split("\\|");
            songID.set(songAttributes[0]); //artistID is the third attribute in the line

            if(songAttributes[1].equals("nan")){
                songHottness.set(0);
            }
            else{
                songHottness.set(Float.parseFloat(songAttributes[1]));
            }
            
            context.write(songID, songHottness); //this is what writes each <key, value> pair to the reducer
    }
}