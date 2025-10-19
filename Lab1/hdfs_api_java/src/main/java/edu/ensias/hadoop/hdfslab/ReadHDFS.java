package edu.ensias.hadoop.hdfslab;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.io.*;

public class ReadHDFS {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) { System.err.println("Usage: <hdfs_file>"); System.exit(1); }
        FileSystem fs = FileSystem.get(new Configuration());
        try (FSDataInputStream in = fs.open(new Path(args[0]));
             BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
            String line; while ((line = br.readLine()) != null) System.out.println(line);
        }
    }
}
