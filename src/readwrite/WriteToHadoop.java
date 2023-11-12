package readwrite;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
//takes a Path object for the file to be created and returns an output stream to write to
public class WriteToHadoop {
    public void writeToHadoop(String uri) throws Exception {
        Configuration conf = new Configuration();
        String file = uri + "/test/mytest.txt";
        FileSystem fs = FileSystem.get(URI.create(file), conf);

        Path filepath = new Path(file);

        String s = "I am streaming this sentence to hadoop file \n";

        //ByteArrayInputStream is composed of two words: ByteArray and InputStream.
        // As the name suggests, it can be used to read byte array as input stream.
        //Java ByteArrayInputStream class contains an internal buffer which is used
        // to read byte array as stream. In this stream, the data is read from a byte array.
        InputStream in = new ByteArrayInputStream(s.getBytes());
        /*creating the output filepath and printing the arguments passed (-> folding)*/
        FSDataOutputStream out = fs.create(filepath, () -> System.out.println("/"));
         /*copying the in to out (buffsize is 4096 bytes at a time, normallly a multiple of 1024 byts=1kb)
       then closing streams*/
        IOUtils.copyBytes(in, out, 4096, true);
        in.close();
        out.close();
    }

}
