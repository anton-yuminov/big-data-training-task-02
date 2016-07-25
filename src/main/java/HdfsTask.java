
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.epam.bigdata.utils.TopLList;
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

    public static Map<String, Integer> extractCountMap(BufferedReader reader) throws IOException {
        Map<String, Integer> countMap = new HashMap<>();
        String line;
        while ((line = reader.readLine()) != null) {
            String[] values = line.split("\\t");
            String id = values[2];
            Integer count = countMap.get(id);
            if (count == null) {
                countMap.put(id, 1);
            } else {
                countMap.put(id, count + 1);
            }
        }
        return countMap;
    }

    public static void fillTop100Desc(Map<String, Integer> countMap, List<String> ids, List<Integer> counts) {
        TopLList<Integer, String> top = new TopLList<>(100);
        for(Map.Entry<String, Integer> e : countMap.entrySet()) {
            top.add(-e.getValue(), e.getKey());
        }

        for (int i = 0; i < top.getLList().size(); i++) {
            ids.add(top.getRList().get(i));
            counts.add(-top.getLList().get(i));
        }
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

        Map<String, Integer> countMap;

        try (FSDataInputStream in = fileSystem.open(path);
             BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
            countMap = extractCountMap(reader);
        }

        List<String> outIds = new ArrayList<>();
        List<Integer> outCounts = new ArrayList<>();

        fillTop100Desc(countMap, outIds, outCounts);

        System.out.println("Out ids: " + outIds);
        System.out.println("Out counts: " + outCounts);

        System.out.println("Done!");
    }
}