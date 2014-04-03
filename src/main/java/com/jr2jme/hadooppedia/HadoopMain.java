package com.jr2jme.hadooppedia;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
/**
 * Created by Hirotaka on 2014/04/03.
 */
public class HadoopMain extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int r = ToolRunner.run(new HadoopMain(), args);
        System.exit(r);
    }

    @Override
    public int run(String[] args) throws Exception {
        Path input = new Path("sum100/input");
        Path output = new Path("sum100/output");
        init(input);

        FileSystem fs = output.getFileSystem(getConf());
        try {
            fs.delete(output, true);

            return submit(fs, input, output);
        } finally {
            fs.close();
        }
    }

    void init(Path input) throws IOException {
        FileSystem fs = input.getFileSystem(getConf());
        try {
            SequenceFile.Writer writer = SequenceFile.createWriter(fs,
                    getConf(), input, NullWritable.class, IntWritable.class,
                    CompressionType.NONE);
            try {
                NullWritable key = NullWritable.get();
                IntWritable val = new IntWritable();

                for (int x = 1; x <= 100; x++) {
                    val.set(x);
                    writer.append(key, val);
                }
            } finally {
                writer.close();
            }
        } finally {
            fs.close();
        }
    }

    int submit(FileSystem fs, Path input, Path output) throws IOException,
            InterruptedException, ClassNotFoundException {
        Job job = new Job(getConf(), "sum100");
        job.setJarByClass(HadoopMain.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Mapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        boolean succeeded = job.waitForCompletion(true);
        if (succeeded) {
            print(fs, output);
            return 0;
        } else {
            return 1;
        }
    }

    void print(FileSystem fs, Path output) throws IOException {
        PathFilter filter = new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().startsWith("part-");
            }
        };
        for (FileStatus s : fs.listStatus(output, filter)) {
            SequenceFile.Reader reader = new SequenceFile.Reader(fs,
                    s.getPath(), getConf());
            try {
                NullWritable key = NullWritable.get();
                IntWritable val = new IntWritable();

                while (reader.next(key, val)) {
                    System.out.println(val.get());
                }
            } finally {
                reader.close();
            }
        }
    }
}
