package ict.ocrabase.main.java.query.all;

import java.io.IOException;

public class OrderQuery {
	public static void main(String[] args) {
		if (args.length != 7) {
			System.out.println("wrong parameter");
			return;
		}
		String query=args[args.length-1];
		if("q3".equals(query)){
			try {
				OrderQ3Util.process(args);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}else if("q4".equals(query)){
			try {
				OrderQ4Util.process(args);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
	}
}
