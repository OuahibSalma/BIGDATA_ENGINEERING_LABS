package edu.ensias.hadoop.hdfslab;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.io.IOException;

public class HadoopFileStatus {
    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.err.println("Usage: <hdfs_dir> <file_name> <new_name>");
            System.exit(1);
        }
        String dir = args[0], file = args[1], newName = args[2];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path p = new Path(dir, file);
        if (!fs.exists(p)) { System.err.println("Not found: " + p); System.exit(1); }
        FileStatus st = fs.getFileStatus(p);
        System.out.println("Name: " + p.getName());
        System.out.println("Size: " + st.getLen());
        System.out.println("Owner: " + st.getOwner());
        System.out.println("Perm: " + st.getPermission());
        System.out.println("Replication: " + st.getReplication());
        System.out.println("BlockSize: " + st.getBlockSize());
        boolean ok = fs.rename(p, new Path(dir, newName));
        System.out.println(ok ? "Renamed" : "Rename failed");
        fs.close();
    }
}
