package reader;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class S3FileReader implements Runnable {

    S3AFileSystem fs=null;


    Path filePath;
    public S3FileReader(S3AFileSystem fs , Path filePath){
        this.fs=fs;
        this.filePath=filePath;
    }

    @Override
    public void run() {


        try {
                ParquetFileReader reader = ParquetFileReader.open(this.fs.getConf(),this.filePath);
                PageReadStore pages;
                while ((pages = reader.readNextRowGroup()) != null) {
                    long rows = pages.getRowCount();
                    System.out.println(rows);
                }
                reader.close();

            System.out.println("File Size: "+this.filePath.toString()+":");

        } catch (IOException e) {
           System.err.println(e);
        }
    }
}

public class S3Reader {
    public  static  void main(String[] args) throws IOException {

        if (args.length<1){
            System.out.println("Error:");
            System.out.println("USAGE: <JAVA CLASS NAME> <ALLUXIO_PATH> <THREADS>");
        }


        String pathStr=args[0];
        int MAX_T=Integer.parseInt(args[1]);
        System.out.println("Reading File : "+pathStr+" with N Threads:"+MAX_T);


        //part-00000-e7d59cb6-125c-436e-a76a-f2d604e401ae-c000.snappy.parquet
        S3AFileSystem fs = new S3AFileSystem();
        Path path=new Path(pathStr);
        Configuration conf=new Configuration();
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set("fs.s3a.connection.maximum","36");
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
                S3FileReader reader=new S3FileReader(fs,fileStatus.getPath());
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


