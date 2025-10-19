package edu.ensias.hadoop.hdfslab;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.io.IOException;

public class HDFSWrite {
    public static void main(String[] args) throws IOException {
        if (args.length < 2) { System.err.println("Usage: <target_file> <text>"); System.exit(1); }
        FileSystem fs = FileSystem.get(new Configuration());
        Path t = new Path(args[0]);
        if (!fs.exists(t)) try (FSDataOutputStream out = fs.create(t)) { out.writeUTF(args[1] + "\n"); }
        else try (FSDataOutputStream out = fs.append(t)) { out.writeUTF(args[1] + "\n"); }
        fs.close();
    }
}
