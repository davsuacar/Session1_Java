package utad.hdfs.utad_sequenciator_sample;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;

public class SequenciatorSample {
	
	public static final String DEFAULT_FS = "hdfs://quickstart.cloudera:8020/";

	public static void main(String[] args) throws Exception {
		
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);

		 Configuration conf = new Configuration();
		 FileSystem hdfs = FileSystem.get(new URI(DEFAULT_FS), conf);
		 
		 Boolean exists = hdfs.exists(outputPath);
		 
		 if (exists) {
			 System.out.print("File already exists");
			 hdfs.close();
			 return;
		 }
		 
		 SequenceFile.Writer outSeq = SequenceFile.createWriter(conf,
				 SequenceFile.Writer.file(hdfs.makeQualified(outputPath)),
				 SequenceFile.Writer.compression(CompressionType.BLOCK, new GzipCodec()),
				 SequenceFile.Writer.keyClass(IntWritable.class),
				 SequenceFile.Writer.valueClass(Text.class));
		 
		 FileStatus[] list = hdfs.listStatus(inputPath);
		 for (FileStatus fileStatus : list) {
			// Take data from input files.
			 
			 FSDataInputStream in = hdfs.open(fileStatus.getPath());	
			 
			 byte[] content = new byte[(int)hdfs.getFileStatus(fileStatus.getPath()).getLen()];
			 in.readFully(content);
			 
			 in.close();	
			 outSeq.append(new IntWritable(1), new Text(new String(content)));
		 }
		 
		 outSeq.close();
		 hdfs.close();
	}
}
