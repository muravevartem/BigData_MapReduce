package org.example;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.text.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {

  public static class MappingResult implements Writable {

    long countSymbols;
    long countA;
    long countB;
    String word;

    public MappingResult() {
    }

    public MappingResult(long countSymbols, long countA, long countB, String word) {
      this.countSymbols = countSymbols;
      this.countA = countA;
      this.countB = countB;
      this.word = word;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      dataOutput.writeLong(countSymbols);
      dataOutput.writeLong(countA);
      dataOutput.writeLong(countB);
      dataOutput.writeChars(word);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      this.countSymbols = dataInput.readLong();
      this.countA = dataInput.readLong();
      this.countB = dataInput.readLong();
      this.word = dataInput.readLine();
    }
  }

  public static class SMapper extends Mapper<Object, Text, Text, MappingResult> {

    private static final Set<String> A = new HashSet<>(
        Arrays.asList("А", "И", "О", "У", "Ы", "Э"));
    private static final Set<String> B = new HashSet<>(
        Arrays.asList("Б", "В", "Г", "Д", "Ж", "З", "Й", "К", "Л",
            "М", "Н", "П", "Р", "С", "Т", "Ф", "Х", "Ц", "Ч", "Ш", "Щ"));

    @Override
    protected void map(Object key, Text value,
        Mapper<Object, Text, Text, MappingResult>.Context context)
        throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), " ");
      while (itr.hasNext()) {
        String word = itr.next()
            .replace(".", "")
            .replace(",", "")
            .replace(":", "");

        System.out.println("Hello");
        if (!"".equals(word)) {
          context.write(
              new Text("Mapping result"),
              new MappingResult(
                  word.length(),
                  Arrays.stream(word.split(""))
                      .peek(System.out::println)
                      .filter(s -> A.contains(s.toUpperCase()))
                      .count(),
                  Arrays.stream(word.split(""))
                      .filter(s -> B.contains(s.toUpperCase()))
                      .count(),
                  word
              ));
        }
      }
    }
  }

  public static class ReduceResult implements Writable{

    double avgCount;
    double avgA;
    double avgB;
    String maxString;

    public ReduceResult(double avgCount, double avgA, double avgB, String maxString) {
      this.avgCount = avgCount;
      this.avgA = avgA;
      this.avgB = avgB;
      this.maxString = maxString;
    }

    public ReduceResult() {
    }

    @Override
    public String toString() {
      return "ReduceResult{" +
          "avgCount=" + avgCount +
          ", avgA=" + avgA +
          ", avgB=" + avgB +
          ", maxString='" + maxString + '\'' +
          '}';
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      dataOutput.writeDouble(avgCount);
      dataOutput.writeDouble(avgA);
      dataOutput.writeDouble(avgB);
      dataOutput.writeChars(maxString);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      this.avgCount = dataInput.readDouble();
      this.avgA = dataInput.readDouble();
      this.avgB = dataInput.readDouble();
      this.maxString = dataInput.readLine();
    }
  }

  public static class SReducer extends Reducer<Text, MappingResult, Text, ReduceResult> {


    @Override
    protected void reduce(Text key, Iterable<MappingResult> values,
        Reducer<Text, MappingResult, Text, ReduceResult>.Context context)
        throws IOException, InterruptedException {
      long count = 0;
      long sumCount = 0;
      long sumA = 0;
      long sumB = 0;
      String maxString = "";
      for (MappingResult mappingResult : values) {
        count++;
        sumCount += mappingResult.countSymbols;
        sumA += mappingResult.countA;
        sumB += mappingResult.countB;
        if (mappingResult.word.length() > maxString.length()) {
          maxString = mappingResult.word;
        }
      }
      System.out.println(sumA + " " + sumB);
      context.write(new Text("Result"), new ReduceResult((double) sumCount / count,
          (double) sumA / count, (double) sumB / count, maxString));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "job");
    job.setJarByClass(Main.class);
    job.setMapperClass(SMapper.class);
    job.setReducerClass(SReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(MappingResult.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(ReduceResult.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    System.out.println(Arrays.stream("АБВГ".split("")).filter(s -> Arrays.asList("А", "Б").contains(s)).count());
  }
}