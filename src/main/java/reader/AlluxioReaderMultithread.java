package reader;
import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.List;


public class AlluxioReaderMultithread {

    public static void alluxio_read_performance(String filePath) throws IOException {
        long startTime = System.currentTimeMillis();
        ParquetFileReader reader = null; // ParquetFileReader.open(HadoopInputFile.fromPath(new Path(filePath), new Configuration()));
        MessageType schema = reader.getFooter().getFileMetaData().getSchema();
        List<Type> fields = schema.getFields();
        PageReadStore pages;
        while ((pages = reader.readNextRowGroup()) != null) {
            long rows = pages.getRowCount();
            //MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
            //RecordReader recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
            /*
            for (int i = 0; i < rows; i++) {
                SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();

            }*/
        }
        long endTime   = System.currentTimeMillis();
        float totalTime = (endTime - startTime)/1000F;
        System.out.println("Time Taken to read "+filePath + totalTime);
        reader.close();
    }


    public static void s3_read_performance(String s3_path){
    }

    public static void main(String[] file) throws IOException {
        String filePath="alluxio://ec2-18-207-158-136.compute-1.amazonaws.com:19998/mnt/s3/catalog_sales/cs_sold_date_sk=2451171/part-00156-494ff6a6-303b-4823-8f4c-9bfd3346acb8.c000.snappy.parquet";
        AlluxioReaderMultithread.alluxio_read_performance(filePath);
        //String s3_path="s3://tpcds-datagen/tpcds-1000/customer/part-00051-4d61603d-b59e-48e5-a1c2-7c5d059da309-c000.snappy.parquet"
        //Main.s3_read_performance(s3_path);
    }
}