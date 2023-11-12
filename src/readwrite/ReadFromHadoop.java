package readwrite;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

public class ReadFromHadoop {
    public void readFromFile(String uri) throws Exception{
         /*A Configuration object encapsulates a client or
         server’s configuration, which is set  using configuration files read
         from the classpath, such as etc/hadoop/core-site.xml*/
        Configuration conf=new Configuration();
        String file=uri+"/test/mytest.txt";
        FileSystem fs=  FileSystem.get(URI.create(file),conf);
          /*FileSystem is a general filesystem API, so the first step is to
           retrieve an instance for the filesystem we want to use—HDFS, in this case.*/
        /*uses the given URI’s scheme and
           authority to determine the filesystem to use,falling
           back to the default filesystem if no
           scheme is specified in the given URI.*/
        Path filePath=new Path(file);
        /*A file in a Hadoop filesystem is represented by a Hadoop Path
         object (and not a java.io.File object, since its semantics are too
         closely tied to the local filesystem).
          You can think of a Path as a Hadoop filesystem URI, such as
          hdfs://localhost/user/test/mytest.txt"   */
        InputStream in=fs.open(filePath);
        OutputStream out=System.out;
        /*The open() method on FileSystem actually returns an FSDataInputStream
        rather than a standard java.io class. This class is a specialization of
        java.io.DataInputStream */
        IOUtils.copyBytes(in,out,4096,true);
        /*Then we call the utility method copyBytes()
        on IOUtils to copy the input to the output byte by byte
        The last two arguments to the copyBytes()
        method are the buffer size used for copying and whether to close
        the streams when the copy is complete.*/
        in.close();
        out.close();
    }
}
