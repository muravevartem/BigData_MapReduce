package org.example;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.text.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import static java.lang.String.format;

public class Main {

    public static class MapperResult implements Writable {
        String route;

        long interval;

        public MapperResult(String route, long interval) {
            this.route = route;
            this.interval = interval;
        }

        public MapperResult() {
        }

        @Override
        public String toString() {
            return "MapperResult{" +
                    "route='" + route + '\'' +
                    ", interval=" + interval +
                    '}';
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeLong(interval);
            dataOutput.writeChars(route);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.interval = dataInput.readLong();
            this.route = dataInput.readLine();
        }
    }

    public static class ReduceResult implements Writable, Comparable<ReduceResult> {

        String route;

        double interval;

        public ReduceResult() {
        }

        public ReduceResult(String route, double interval) {
            this.route = route;
            this.interval = interval;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeDouble(interval);
            dataOutput.writeChars(route);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.interval = dataInput.readDouble();
            this.route = dataInput.readLine();
        }

        @Override
        public String toString() {
            return format("%s\t%s\n", route, interval);
        }

        @Override
        public int compareTo(ReduceResult o) {
            return Double.compare(this.interval, o.interval);
        }
    }

    public static class SMapper extends Mapper<Object, Text, Text, MapperResult> {

        @Override
        protected void map(Object key, Text value,
                           Mapper<Object, Text, Text, MapperResult>.Context context)
                throws IOException, InterruptedException {
            try {
                String[] split = value.toString().split(";");

                MapperResult mapperResult = new MapperResult(
                        split[0],
                        Long.parseLong(split[1])
                );
                System.out.println("OK");
                System.out.println(mapperResult);

                context.write(
                        new Text("Result"),
                        mapperResult
                );

            } catch (NumberFormatException e) {

            }
        }
    }

    public static class SReducer extends Reducer<Text, MapperResult, Text, String> {


        @Override
        protected void reduce(Text key, Iterable<MapperResult> values,
                              Reducer<Text, MapperResult, Text, String>.Context context)
                throws IOException, InterruptedException {

            Map<String, Long> intervals = new HashMap<>();
            Map<String, Long> count = new HashMap<>();

            for (MapperResult mapperResult : values) {
                long interval = intervals.getOrDefault(mapperResult.route, 0L) + mapperResult.interval;
                intervals.put(mapperResult.route, interval);
                long c = count.getOrDefault(mapperResult.route, 0L) + 1;
                count.put(mapperResult.route, c);
            }

            System.out.println(values);

            List<ReduceResult> collect = intervals.keySet().stream()
                    .map(route -> new ReduceResult(route, (double) intervals.get(route) / count.get(route)))
                    .sorted()
                    .collect(Collectors.toList());

            String result = collect.stream()
                    .map(ReduceResult::toString)
                    .collect(Collectors.joining());

            context.write(
                    new Text("Result"),
                    result
            );
        }

    }

    public static void main(String[] args) throws Exception {

        class Data {
            String route;
            Long interval;

            public Data(String route, Long interval) {
                this.route = route;
                this.interval = interval;
            }
        }
        Configuration conf = new Configuration();
        System.out.println("start");
        Job job = Job.getInstance(conf, "job");
        job.setJarByClass(Main.class);
        job.setMapperClass(SMapper.class);
        job.setReducerClass(SReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapperResult.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(String.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}