
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.epam.bigdata.utils.FileProcessor;
import com.epam.bigdata.utils.TopLList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HdfsTask {

    private static final Logger logger = LoggerFactory.getLogger(HdfsTask.class);

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
        for (Map.Entry<String, Integer> e : countMap.entrySet()) {
            top.add(-e.getValue(), e.getKey());
        }

        for (int i = 0; i < top.getLList().size(); i++) {
            ids.add(top.getRList().get(i));
            counts.add(-top.getLList().get(i));
        }
    }

    private static void processHdfsFile(Path pathIn, Configuration conf, FileSystem fileSystem, FileProcessor fileProcessor) throws IOException {
        logger.info("Starting file: {}", pathIn);
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec codec = factory.getCodec(pathIn);

        if (codec == null) {
            System.err.println("No codec found for " + pathIn);
            System.exit(1);
        }

        try (InputStream in = codec.createInputStream(fileSystem.open(pathIn));
             BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
            fileProcessor.processFile(reader);
            System.out.println("Complete: " + pathIn.toString() + ". Status: " + fileProcessor.getStatusInfo());
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

        Path pathIn = new Path(hdfsInPathString);
        if (!fileSystem.exists(pathIn)) {
            System.out.println("File " + hdfsInPathString + " does not exists");
            System.exit(1);
        }

        FileProcessor fileProcessor = new FileProcessor();
        if (fileSystem.isDirectory(pathIn)) {
            FileStatus[] statuses = fileSystem.listStatus(pathIn);
            for (FileStatus fileStatus : statuses) {
                processHdfsFile(fileStatus.getPath(), conf, fileSystem, fileProcessor);
            }
        } else {
            processHdfsFile(pathIn, conf, fileSystem, fileProcessor);
        }

        List<String> outIds = fileProcessor.getSortedIds();
        List<Integer> outCounts = fileProcessor.getSortedCounts();

        System.out.println("Out ids: " + outIds);
        System.out.println("Out counts: " + outCounts);

        Path pathOut = new Path(hdfsOutPathString);
        try (FSDataOutputStream out = fileSystem.create(pathOut);
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out))) {
            for (int i = 0; i < outIds.size(); i++) {
                String s = outIds.get(i) + "\t" + outCounts.get(i);
                writer.write(s);
                writer.newLine();
            }
        }

        System.out.println("Done!");
    }
}