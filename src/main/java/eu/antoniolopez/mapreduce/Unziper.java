package eu.antoniolopez.mapreduce;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class Unziper {

	private final static int BUFFER = 2048;

	public static void unzip(String fileName){
		try {
			BufferedOutputStream bos = null;
			FileInputStream fis = new FileInputStream(fileName);
			ZipInputStream zis = new ZipInputStream(new BufferedInputStream(fis));
			ZipEntry entry;
			String path = Paths.get(fileName).getParent().toString()+System.getProperty("file.separator");
			while((entry = zis.getNextEntry()) != null) {
				int count;
				byte data[] = new byte[BUFFER];
				FileOutputStream fos = new FileOutputStream(path+entry.getName());
				bos = new BufferedOutputStream(fos, BUFFER);
				while ((count = zis.read(data, 0, BUFFER))!= -1) {
					bos.write(data, 0, count);
				}
				bos.flush();
				bos.close();
			}
			zis.close();
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
