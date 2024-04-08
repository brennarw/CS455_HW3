package csx55.hadoop.QuestionEight;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperOne extends Mapper<Object, Text, Text, Text> {


    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] attributes = value.toString().split("\\|");

        if(attributes[0].equals("nan")){
            attributes[0] = "0.0";
        }
        if(attributes[1].equals("nan")){
            attributes[1] = "0.0";
        }

        String songValue = attributes[0] + "|" + attributes[1] + "|" + attributes[9] + "|" + attributes[11]; //familiarity|hottness|similarity|termFreq

        //System.out.println("familiarity: " + attributes[1]);
        // System.exit(0);

        context.write(new Text(attributes[2]), new Text(songValue));

    }
}

