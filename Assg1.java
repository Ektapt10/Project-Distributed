import java.lang.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.*;
import java.io.*;

public class Assg1{

public static void main(String []args) throws IOException
{
System.out.println("Enter the filename1 path: ");
InputStreamReader in = new InputStreamReader(System.in);
BufferedReader reader = new BufferedReader(in);
String f1 = reader.readLine();


System.out.println("Enter the filename2 path: ");
String f2 = reader.readLine();

System.out.println("Enter the output filename: ");
String f3 = reader.readLine();

Configuration conf = new Configuration();
FileSystem hdfs = FileSystem.get(conf);
FileSystem localfs = FileSystem.getLocal(conf);

Path inFile1 = new Path(f1);
if(!localfs.exists(inFile1))
{
System.out.println("File1 not found");
return;
}

Path inFile2 = new Path(f2);
if(!localfs.exists(inFile2))
{
System.out.println("File2 not found");
return;
}

Path outFile = new Path(f3);
if(!hdfs.exists(outFile))
{
 outFile = new Path("/user/dir1/default-assign1");
 FSDataOutputStream out=null;
 if(!hdfs.exists(outFile))
 {
  out = hdfs.create(hdfs,outFile,FsPermission.valueOf("-rwxrw-rw-"));
 }
 else
 {
  out = hdfs.create(outFile);
 }
 WriteDataToFile(inFile1,out,localfs);
 WriteDataToFile(inFile2,out,localfs);
 out.close();
}

}

public static void WriteDataToFile(Path inFile, FSDataOutputStream out,FileSystem localfs) throws IOException
{
 FSDataInputStream in = localfs.open(inFile);
 int bytesRead = 0;
 byte []buffer = new byte[256];
 while((bytesRead=in.read(buffer))>0)
 {
  out.write(buffer,0,bytesRead);
 }
 in.close();
}

}
