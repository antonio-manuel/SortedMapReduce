package eu.antoniolopez.mapreduce;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FileHandler {

	private static String ZIP_EXTENSION = "zip";
	private final static int BUFFER = 64*1024;
	
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
	
	public static BufferedInputStream getReader(String url) throws IOException{
		URL aURL = new URL(url);
		InputStream is;
		if(aURL.getProtocol().equals("file")){
			is = Files.newInputStream(Paths.get(URI.create(url)));
		}else{
			is = aURL.openStream();
		}
		return new BufferedInputStream(is);
	}
	
	public static String copyFile(String url, String path){
		String fileName = getfileName(url);
		String exit = path;
		if(!exit.endsWith(System.getProperty("file.separator"))){
			exit+=System.getProperty("file.separator");
		}
		exit+=fileName;
		try{
		    Files.deleteIfExists(Paths.get(exit));
		    Files.createFile(Paths.get(exit));		    
		    
		    BufferedInputStream bis = getReader(url);
		    FileOutputStream fos = new FileOutputStream(exit);
		    BufferedOutputStream bos = new BufferedOutputStream(fos, BUFFER);
		    int count;
			byte data[] = new byte[BUFFER];
		    while ((count = bis.read(data, 0, BUFFER))!= -1) {
		    	bos.write(data, 0, count);
			}
		    bos.close();
		    bis.close();
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
