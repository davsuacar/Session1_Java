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
		
		// We take the input <glob­ficheros­orig> and the output <fichero­dest> from arguments.
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		 // Configuration Object creation.
		 Configuration conf = new Configuration();
			
		 // FileSystem Object from our hdfs home "hdfs://quickstart.cloudera:8020/  
		 FileSystem hdfs = FileSystem.get(new URI(DEFAULT_FS), conf);
		 
		 // Boolean to check the file does not exists.
		 Boolean exists = hdfs.exists(outputPath);
		 
		 // If file already exists then print a warning and close the FileSystem.
		 if (exists) {
			 System.out.print("File already exists");
			 hdfs.close();
			 return;
		 }
		 
		 // SequenceFile creation from Configuration Object and setting the Gzip compression.
		 SequenceFile.Writer outSeq = SequenceFile.createWriter(conf,
				 SequenceFile.Writer.file(hdfs.makeQualified(outputPath)),
				 SequenceFile.Writer.compression(CompressionType.BLOCK, new GzipCodec()),
				 SequenceFile.Writer.keyClass(IntWritable.class),
				 SequenceFile.Writer.valueClass(Text.class));
		 
		 // Take the files from the input folder. 
		 FileStatus[] list = hdfs.listStatus(inputPath);
		 for (FileStatus fileStatus : list) {

  			 // Open file and take data from input files.			 
			 FSDataInputStream in = hdfs.open(fileStatus.getPath());	
			 
			 byte[] content = new byte[(int)hdfs.getFileStatus(fileStatus.getPath()).getLen()];
			 in.readFully(content);
			 
			 in.close();	
			 // Append the file to the SequenceFile.
			 outSeq.append(new IntWritable(1), new Text(new String(content)));
		 }
	
		 // Close the SequenceFile and the FileSystem.	 
		 outSeq.close();
		 hdfs.close();
	}
}
