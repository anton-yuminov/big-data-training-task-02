
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class HdfsTask {
    public HdfsTask() {

    }

    /**
     * delete a directory in hdfs
     *
     * @param file
     * @throws IOException
     */
    public void deleteFile(String file, Configuration conf) throws IOException {
        FileSystem fileSystem = FileSystem.get(conf);

        Path path = new Path(file);
        if (!fileSystem.exists(path)) {
            System.out.println("File " + file + " does not exists");
            return;
        }

        fileSystem.delete(new Path(file), true);

        fileSystem.close();
    }

    /**
     * create directory in hdfs
     *
     * @param dir
     * @throws IOException
     */
    public void mkdir(String dir, Configuration conf) throws IOException {
        FileSystem fileSystem = FileSystem.get(conf);

        Path path = new Path(dir);
        if (fileSystem.exists(path)) {
            System.out.println("Dir " + dir + " already not exists");
            return;
        }

        fileSystem.mkdirs(path);

        fileSystem.close();
    }

    public static void main(String[] args) throws IOException {

        HdfsTask client = new HdfsTask();
        String hdfsPath = "hdfs://localhost:8020"; // for simplicity

        Configuration conf = new Configuration();
        conf.addResource(new Path("/home/hadoop/hadoop/conf/core-site.xml"));
        conf.addResource(new Path("/home/hadoop/hadoop/conf/hdfs-site.xml"));
        conf.addResource(new Path("/home/hadoop/hadoop/conf/mapred-site.xml"));
        FileSystem fileSystem = FileSystem.get(conf);

        String hdfsInPathString = args[0];
        String hdfsOutPathString = args[1];

        Path path = new Path(hdfsInPathString);
        if (!fileSystem.exists(path)) {
            System.out.println("File " + hdfsInPathString + " does not exists");
            System.exit(1);
        }



        try (FSDataInputStream in = fileSystem.open(path);
             BufferedReader reader = new BufferedReader(new InputStreamReader(in));) {
            String line;
            while((line = reader.readLine()) != null) {
                System.out.println(line);
            }
        }


        System.out.println("Done!");
    }
}