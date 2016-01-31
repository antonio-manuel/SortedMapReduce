package eu.antoniolopez.mapreduce.io;

import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Unziper {

	private final static int BUFFER = 64*1024;

	public static void unzip(String fileName){
		try {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			
			FSDataInputStream fis = fs.open(new Path(fileName));
			ZipInputStream zis = new ZipInputStream(fis);
			ZipEntry entry;
			String path = Paths.get(fileName).getParent().toString()+System.getProperty("file.separator");
			while((entry = zis.getNextEntry()) != null) {				
				Path outFile = new Path(path+entry.getName());
				FSDataOutputStream fsdos = fs.create(outFile);
				byte []buffer = new byte [BUFFER];
				int bytesRead  = -1;
				while ((bytesRead = zis.read(buffer)) > 0) {
					fsdos.write(buffer, 0, bytesRead);
				}
				fsdos.flush();
				fsdos.close();
			}
			zis.close();
			fis.close();
		} catch(Exception e) {
			e.printStackTrace();
		}
		try {
			Files.delete(Paths.get(fileName));
		} catch (NoSuchFileException x) {
			System.err.format("%s: no such" + " file or directory%n", fileName);
		} catch (DirectoryNotEmptyException x) {
			System.err.format("%s not empty%n", fileName);
		} catch (IOException x) {
			// File permission problems are caught here.
			System.err.println(x);
		}
	}
}
