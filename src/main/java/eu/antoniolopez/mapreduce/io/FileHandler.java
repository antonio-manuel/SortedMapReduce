package eu.antoniolopez.mapreduce.io;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileHandler {

	private static final String ZIP_EXTENSION = "zip";
	private static final int BUFFER = 64*1024;
	private static final int BUFFER_FTP = 4*1024;
	private static final String REGEXP_CHARS_NUMS = "[^\\p{L}\\p{Nd}]+";
	
	private static String getfileName(String URL){
		int position = 0;
		Pattern pattern = Pattern.compile(REGEXP_CHARS_NUMS);
	    Matcher matcher = pattern.matcher(URL.substring(0,URL.lastIndexOf(".")));
	    while (matcher.find()) {
	        position = matcher.end();
	    }
		return URL.substring(position);
	}
	
	public static boolean isZip(String fileName){
		String extension = fileName.substring(fileName.lastIndexOf(".") + 1, fileName.length());
		return extension.equalsIgnoreCase(ZIP_EXTENSION);
	}
	
	public static InputStream getReader(String url) throws IOException{
		URL aURL = new URL(url);
		InputStream is;
		if(aURL.getProtocol().equals("file")){
			is = Files.newInputStream(Paths.get(URI.create(url)));
		}else{
			URLConnection urlc = aURL.openConnection();
			is = urlc.getInputStream();
		}
		return is;
	}
	
	public static String copyFile(String url, String path){
		String fileName = getfileName(url);
		String exit = path;
		if(!exit.endsWith(System.getProperty("file.separator"))){
			exit+=System.getProperty("file.separator");
		}
		exit+=fileName;
		try{
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			fs.delete(new Path(path), true);
			fs.mkdirs(new Path(path));
			
			BufferedInputStream in = new BufferedInputStream(getReader(url));

			Path outFile = new Path(exit);
			FSDataOutputStream fsdos = fs.create(outFile);
			byte []buffer = new byte [BUFFER];
			int bytesRead  = -1;
			while ((bytesRead = in.read(buffer)) > 0) {
				fsdos.write(buffer, 0, bytesRead);
			}
			fsdos.close();
			in.close();
			
		} catch (MalformedURLException e) {
			System.err.format("%s: no such" + " file or directory%n", url);
			e.printStackTrace();
			exit = null;
		} catch (IOException e) {
			System.err.println(e);
			exit = null;
		}
		return exit;
	}
	
	public static void uploadFile(String file, String ftpUrl){

		try {
		    URL url = new URL(ftpUrl);
		    URLConnection urlConn = url.openConnection();
		    OutputStream os = urlConn.getOutputStream();
		    
		    Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
		    FSDataInputStream fis = fs.open(new Path(file));

		    byte[] buffer = new byte[BUFFER_FTP];
		    int bytesRead = -1;
		    while ((bytesRead = fis.read(buffer)) != -1) {
		    	os.write(buffer, 0, bytesRead);
		    }

		    fis.close();
		    os.close();

		} catch (IOException ex) {
		    ex.printStackTrace();
		}
	}
}
