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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class QuotationAnalyzer {

  public static class IncreaseQuotationFilterMapper extends Mapper < Object, Text, Text, IntWritable > {

    // MapReduce invocará a este método una vez por cada línea del fichero
    public void map(Object key, Text value, Context context) throws IOException,
    InterruptedException {

      // Cogemos la línea que llega como parámetro, la convertimos a String
      // y la dividimos en los distintos bloques de información
      String valueString = value.toString();
      String[] dataOfTheQuotation = valueString.split(";");

      // Tomamos los valores que nos interesan: nombre de la empresa y cotizaciones
      float currentQuotation = Float.parseFloat(dataOfTheQuotation[2]);
      float lastQuotation = Float.parseFloat(dataOfTheQuotation[3]);
      String companyName = dataOfTheQuotation[1];

      // Realizamos el filtro: si la cotización crece, enviamos una pareja (nombre de la empresa, 1)
      if (currentQuotation > lastQuotation)
        context.write(new Text(SingleCountryData[companyName]), new IntWritable(1));
    }
  }
}

public static class IntSumReducer extends Reducer < Text, IntWritable, Text, IntWritable > {

  private IntWritable result = new IntWritable();

  // MapReduce invocará a este método una vez por cada empresa, 
  // pasando como parámetro todos los valores asociados generados en map.
  public void reduce(Text key, Iterable values, Context context) throws IOException,
  InterruptedException {
    int sum = 0;

    // Simplemente sumamos los valores, y la suma será el resultado de esa empresa
    for (IntWritable val: values) {
      sum += val.get();
    }
    result.set(sum);
    context.write(key, result);
  }
}

public static void main(String[] args) throws Exception {
  Configuration conf = new Configuration();
  Job job = Job.getInstance(conf, "QuotationAnalyzer");
  job.setJarByClass(QuotationAnalyzer.class);
  job.setMapperClass(IncreaseQuotationFilterMapper.class);
  job.setCombinerClass(IntSumReducer.class);
  job.setReducerClass(IntSumReducer.class);
  job.setOutputKeyClass(Text.class);
  job.setOutputValueClass(IntWritable.class);
  FileInputFormat.addInputPath(job, new Path(args[0]));
  FileOutputFormat.setOutputPath(job, new Path(args[1]));
  System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}
