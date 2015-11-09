package utad.hdfs.utad_sequenciator_sample;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class SequenciatorSample {
	
	public static final String DEFAULT_FS = "hdfs://quickstart.cloudera:8020/";

	public static void main(String[] args) {

		 Configuration conf = new Configuration();
		 FileSystem hdfs = FileSystem.get(new URI(DEFAULT_FS), conf);
		 
		 Path fichero = new Path("data/quijote.txt");
		 
		 FSDataInputStream in = hdfs.open(fichero);
		 
		 byte[] content = new byte[(int)hdfs.getFileStatus(fichero).getLen()];
		 in.readFully(content);
		 in.close();
		 
		 SequenceFile.Writer outSeq = SequenceFile.createWriter(conf,
				 SequenceFile.Writer.file(hdfs.makeQualified(new Path("data/quijote.seq"))),
				 SequenceFile.Writer.keyClass(IntWritable.class),
				 SequenceFile.Writer.valueClass(Text.class));
		 
		 outSeq.append(new IntWritable(1), new Text(new String(content)));
		 outSeq.close();
		 hdfs.close();

	}

}
