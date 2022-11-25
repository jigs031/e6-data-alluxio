package reader;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Path;
import alluxio.hadoop.FileSystem;
import org.apache.parquet.column.page.PageReadStore;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;

class AlluxioFileReader implements Runnable {
    FileSystem fs=null;
    Path filePath;
    public AlluxioFileReader(FileSystem fs , Path filePath){
        this.fs=fs;
        this.filePath=filePath;
    }

    @Override
    public void run() {

        FSDataInputStream in = null;
        try {
                ParquetFileReader reader = ParquetFileReader.open(this.fs.getConf(),this.filePath);
                PageReadStore pages;
                while ((pages = reader.readNextRowGroup()) != null) {
                    long rows = pages.getRowCount();
                    System.out.println(rows);
                }

            System.out.println("File Size: "+this.filePath.toString()+":");

        } catch (IOException e) {
           System.err.println(e);
        }
    }
}

public class AReader {
    public  static  void main(String[] args) throws IOException {

        if (args.length<1){
            System.out.println("Error:");
            System.out.println("USAGE: <JAVA CLASS NAME> <ALLUXIO_PATH> <THREADS>");
        }


        String pathStr=args[0];
        int MAX_T=Integer.parseInt(args[1]);
        System.out.println("Reading File : "+pathStr+" with N Threads:"+MAX_T);


        //part-00000-e7d59cb6-125c-436e-a76a-f2d604e401ae-c000.snappy.parquet
        FileSystem fs = new alluxio.hadoop.FileSystem();
        Path path=new Path(pathStr);
        Configuration conf=new Configuration();
        conf.set("io.file.buffer.size","12000");
        fs.initialize(path.toUri(),conf);


        ExecutorService pool = Executors.newFixedThreadPool(MAX_T);

        RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(
                path, true);
        long start = System.currentTimeMillis();
        long totalFiles=0;
        while(fileStatusListIterator.hasNext()){

            LocatedFileStatus fileStatus = fileStatusListIterator.next();
            //System.out.println(fileStatus.getPath());
            if (!fileStatus.getPath().toString().contains("_SUCCESS")){
                AlluxioFileReader reader=new AlluxioFileReader(fs,fileStatus.getPath());
                pool.execute(reader);
                totalFiles++;
            }
        }

        System.out.println("File Submitted: "+totalFiles+" ");
        pool.shutdown();
        while(!pool.isTerminated()){}
        long elapsedTime = System.currentTimeMillis() - start;
        System.out.println("Total Time Taken: "+elapsedTime+" to read "+totalFiles+" files.");

    }
}


