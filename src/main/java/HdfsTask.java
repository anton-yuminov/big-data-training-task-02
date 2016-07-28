
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

    private static Configuration conf;
    private static FileSystem fileSystem;
    private static FileProcessor fileProcessor;

    private static void processHdfsFile(Path pathIn) throws IOException {
        logger.info("Starting file: {}", pathIn);
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec codec = factory.getCodec(pathIn);

        if (codec == null) {
            logger.error("No codec found for {}", pathIn);
            System.exit(1);
        }

        try (InputStream in = codec.createInputStream(fileSystem.open(pathIn));
             BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
            fileProcessor.processFile(reader);
            logger.warn("Complete: {}. Status: {}", pathIn, fileProcessor.getStatusInfo());
        }
    }

    private static void init() throws IOException {

        conf = new Configuration();
        conf.addResource(new Path("/home/hadoop/hadoop/conf/core-site.xml"));
        conf.addResource(new Path("/home/hadoop/hadoop/conf/hdfs-site.xml"));
        conf.addResource(new Path("/home/hadoop/hadoop/conf/mapred-site.xml"));
        fileSystem = FileSystem.get(conf);
        fileProcessor = new FileProcessor();
    }

    public static void main(String[] args) throws IOException {

        init();

        String hdfsInPathString = args[0];
        String hdfsOutPathString = args[1];

        Path pathIn = new Path(hdfsInPathString);
        if (!fileSystem.exists(pathIn)) {
            logger.error("File {} does not exist", hdfsInPathString);
            System.exit(1);
        }


        if (fileSystem.isDirectory(pathIn)) {
            FileStatus[] statuses = fileSystem.listStatus(pathIn);
            for (FileStatus fileStatus : statuses) {
                processHdfsFile(fileStatus.getPath());
            }
        } else {
            processHdfsFile(pathIn);
        }

        List<String> outIds = fileProcessor.getSortedIds();
        List<Integer> outCounts = fileProcessor.getSortedCounts();

        logger.info("Out ids: {}", outIds);
        logger.info("Out counts: {}", outCounts);

        Path pathOut = new Path(hdfsOutPathString);
        try (FSDataOutputStream out = fileSystem.create(pathOut);
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out))) {
            for (int i = 0; i < outIds.size(); i++) {
                String s = outIds.get(i) + "\t" + outCounts.get(i);
                writer.write(s);
                writer.newLine();
            }
        }

        logger.warn("Done!");
    }
}