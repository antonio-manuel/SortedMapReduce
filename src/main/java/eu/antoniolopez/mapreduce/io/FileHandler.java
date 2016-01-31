package eu.antoniolopez.mapreduce.io;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileHandler {

	private static String ZIP_EXTENSION = "zip";
	private static int BUFFER = 64*1024;
	
	private static String getfileName(String URL){
		int position = 0;
		Pattern pattern = Pattern.compile("[^\\p{L}\\p{Nd}]+");
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
	
	public static void deleteFolder(File file) throws IOException{
    	if(file.isDirectory()){
    		if(file.list().length==0){    			
    		   file.delete();
    		}else{
        	   String files[] = file.list();     
        	   for (String temp : files) {
        	      File fileDelete = new File(file, temp);
        	      deleteFolder(fileDelete);
        	   }
        	   if(file.list().length==0){
           	     file.delete();
        	   }
    		}    		
    	}else{
    		file.delete();
    	}
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
			
//			File tmp = new File(path);
//			deleteFolder(tmp);
//			tmp.mkdir();
//		    Files.createFile(Paths.get(exit));		    
		    
//		    ReadableByteChannel rbc = Channels.newChannel(getReader(url));
//		    FileOutputStream fos = new FileOutputStream(exit);
//		    fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
//		    fos.close();
//		    rbc.close();
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
}
