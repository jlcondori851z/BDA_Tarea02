import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountByFile {

  public static class WordCountMapper extends Mapper < Object, Text, Text, IntWritable > {

    private final static IntWritable one = new IntWritable(1);
    private Text wordKey = new Text();

    // MapReduce invocará a este método una vez por cada línea del fichero
    public void map(Object key, Text value, Context context) throws IOException,
    InterruptedException {

      // Obtener el nombre del fichero (para contar por fichero)
      String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

      // Leer la línea
      String line = value.toString();

      // Ignorar cabecera del CSV
      if (line.startsWith("game_id,type,player_id")) {
        return;
      }

      // Normalizar
      line = line.toLowerCase();

      // Reemplazar separadores por espacios (para poder usar StringTokenizer)
      line = line.replaceAll("[^a-z0-9]+", " ");

      // Tokenizar en palabras
      StringTokenizer itr = new StringTokenizer(line);

      while (itr.hasMoreTokens()) {
        String token = itr.nextToken();

        // Clave: "fichero \t palabra"
        wordKey.set(fileName + "\t" + token);
        context.write(wordKey, one);
      }
    }
  }

  public static class IntSumReducer extends Reducer < Text, IntWritable, Text, IntWritable > {

    private IntWritable result = new IntWritable();

    // MapReduce invocará a este método una vez por cada clave,
    // pasando todos los valores asociados generados en map.
    public void reduce(Text key, Iterable < IntWritable > values, Context context)
    throws IOException,
    InterruptedException {

      int sum = 0;
      for (IntWritable val: values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "WordCountByFile");

    job.setJarByClass(WordCountByFile.class);
    job.setMapperClass(WordCountMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
