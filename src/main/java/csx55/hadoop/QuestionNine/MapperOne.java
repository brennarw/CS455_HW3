package csx55.hadoop.QuestionNine;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperOne extends Mapper<Object, Text, Text, Text> {

    private final static IntWritable one = new IntWritable(1); //key

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] attributes = value.toString().split("\\|", 2);

        if(attributes[0].equals("SOAAXAK12A8C13C030")){
            context.write(new Text(attributes[0]), new Text(attributes[1]));
        //     System.out.println("hottness: " + attributes[1]);
        //     System.out.println();
        //     System.out.println("tempo: " + attributes[13]);
        //     System.out.println();
        //     System.out.println("time signature: " + attributes[14]);
        //     System.out.println();
        //     System.out.println("danceability: " + attributes[3]);
        //     System.out.println();
        //     System.out.println("duration: " + attributes[4]);
        //     System.out.println();
        //     System.out.println("mode: " + attributes[10]);
        //     System.out.println();
        //     System.out.println("energy: " + attributes[6]);
        //     System.out.println();
        //     System.out.println("key: " + attributes[7]);
        //     System.out.println();
        //     System.out.println("loudness: " + attributes[9]);
        //     System.out.println();
        //     System.out.println("stops fading in at: " + attributes[5]);
        //     System.out.println();
        //     System.out.println("starts fading out: " + attributes[12]);
        //     System.out.println();
        //     System.out.println("artist terms: " + attributes[41]);
        //     System.exit(0);
        }

        //context.write(new Text(attributes[0]), new Text(value));

    }
}

