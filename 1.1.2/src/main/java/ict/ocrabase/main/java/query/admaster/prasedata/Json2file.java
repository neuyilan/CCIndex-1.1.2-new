package ict.ocrabase.main.java.query.admaster.prasedata;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import com.google.gson.Gson;

public class Json2file {
	private Gson gson;
//	private String fileName = "jc-2016070300.log";
//	private String rFileName = "/home/qhl/test-data/"+fileName;
	
	private static String readPath="/home/qhl/test-data/";
	private String wFileName = "/home/qhl/result-data/ads.txt";

	

	public Json2file() {
		gson = new Gson();
	}
	
	public static void main(String[] args) {
		Json2file json2file = new Json2file();
		json2file.readPath(readPath);
	}
	
	


	public Ads json2Ads(String str) {
		Ads ads = gson.fromJson(str, Ads.class);
//		System.out.println(ads.toString());
		return ads;
	}
	
	
	
	public void readPath(String path){
		File file=new File(path);
		File [] files=file.listFiles();
		long count=1;
		
		BufferedReader reader = null;
		
		File wf = new File(wFileName);
		OutputStreamWriter osw=null;
		BufferedWriter bw =null;
		
		String tempStr = null;
		Ads ads=null;
		for(int i=0;i<files.length;i++){
			try {
				InputStreamReader isr=new InputStreamReader(new FileInputStream(files[i]),"UTF-8");
				reader = new BufferedReader(isr);
				
				osw=new OutputStreamWriter(new FileOutputStream(wf,true),"UTF-8");
				bw = new BufferedWriter(osw);
				
				while ((tempStr = reader.readLine()) != null) {
//					System.out.println("json str:"+tempStr.split(",").length);
					ads = json2Ads(tempStr);
					String wstr=count+";"+ads.toString()+"\n";
//					System.out.println("length: "+wstr.split(";").length);
					bw.write(wstr);
					count++;
				}
				reader.close();
				bw.close();

			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if (reader != null) {
					try {
						reader.close();
					} catch (Exception e2) {
						e2.printStackTrace();
					}
				}
			}
		}
		
	}
	

//	public void readFile() {
//		File rf = new File(rFileName);
//		BufferedReader reader = null;
//
//		try {
//			
//			InputStreamReader isr=new InputStreamReader(new FileInputStream(rf),"UTF-8");
//			reader = new BufferedReader(isr);
//
//			File wf = new File(wFileName);
//			OutputStreamWriter osw=new OutputStreamWriter(new FileOutputStream(wf),"UTF-8");
//
//			BufferedWriter bw = new BufferedWriter(osw);
//			String tempStr = null;
//			Ads ads=null;
//			
//			while ((tempStr = reader.readLine()) != null) {
//				System.out.println("json str:"+tempStr.split(",").length);
//				ads = json2Ads(tempStr);
//				String wstr=count+";"+ads.toString()+"\n";
//				System.out.println("length: "+wstr.split(";").length);
////				bw.write(wstr);
//				count++;
//			}
//			reader.close();
//			bw.close();
//
//		} catch (Exception e) {
//			e.printStackTrace();
//		} finally {
//			if (reader != null) {
//				try {
//					reader.close();
//				} catch (Exception e2) {
//					e2.printStackTrace();
//				}
//			}
//		}
//	}
//


}
