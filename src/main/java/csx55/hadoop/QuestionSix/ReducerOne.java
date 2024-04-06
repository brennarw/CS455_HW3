package csx55.hadoop.QuestionSix;

import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerOne extends Reducer<IntWritable, Text, Text, Text>  {

    private PriorityQueue<SongObject> pq = new PriorityQueue<>();
    // private Text songID = new Text();
    // private FloatWritable[] maxFadingTime = new FloatWritable[2];

    class SongObject implements Comparable<SongObject>{
        private String songID;
        private float danceability;
        private float energy;

        public SongObject(String songID, float danceability, float energy){
            this.songID = songID;
            this.danceability = danceability;
            this.energy = energy;
        }
        
        @Override
        public int compareTo(SongObject other){
            int result = Float.compare(other.danceability, this.danceability);
    
            if(result == 0) { 
                return Float.compare(other.energy, this.energy);
            }
            return result;
            
        }
    }
    
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for(Text val : values){
            String[] song = val.toString().split("\\|");
            SongObject songObj = new SongObject(song[0], Float.parseFloat(song[1]), Float.parseFloat(song[2]));
            pq.add(songObj);
        }

        for(int i = 0; i < 10; i++){
            SongObject tempSong = pq.poll();
            String valueOut = String.valueOf(tempSong.danceability) + " " + String.valueOf(tempSong.energy);
            context.write(new Text(tempSong.songID), new Text(valueOut));
        }
        
    }
    
    //cleanup runs only once after all reduce tasks have completed
    // protected void cleanup(Context context) throws IOException, InterruptedException {
    // }
}