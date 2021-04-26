import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.Date;
import java.util.Hashtable;
import java.util.Vector;

import org.junit.jupiter.api.Assertions;

public class test {
	public static void main(String[] args) throws DBAppException {
//		try {
////			String wawa="lol";
////			Integer mama=2;
////			Object lol=(Object)2.4657;
////			System.out.println(lol.getClass());
////			System.out.println(Class.forName("java.lang.Double").isInstance(lol));
//			
//		} catch (IllegalArgumentException | SecurityException | ClassNotFoundException e) {
			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
//		DBApp dbApp=new DBApp();
//		
//		String tableName = "transcripts";
//
//        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
//        htblColNameType.put("gpa", "java.lang.Double");
//        htblColNameType.put("student_id", "java.lang.String");
//        htblColNameType.put("course_name", "java.lang.String");
//        htblColNameType.put("date_passed", "java.util.Date");
//
//        Hashtable<String, String> minValues = new Hashtable<>();
//        minValues.put("gpa", "0.7");
//        minValues.put("student_id", "43-0000");
//        minValues.put("course_name", "AAAAAA");
//        minValues.put("date_passed", "1990-01-01");
//
//        Hashtable<String, String> maxValues = new Hashtable<>();
//        maxValues.put("gpa", "5.0");
//        maxValues.put("student_id", "99-9999");
//        maxValues.put("course_name", "zzzzzz");
//        maxValues.put("date_passed", "2020-12-31");
//
//        dbApp.createTable(tableName, "gpa", htblColNameType, minValues, maxValues);
        
		
		
//		System.out.println(getFileOverflowNumber("Employee[243](1423)"));
		
//		File dir = new File("src\\main\\resources");
//		File[] directoryListing = dir.listFiles();
//		Boolean found=false;
//		if (directoryListing != null) {
//			boolean foundPage=false;
//			for (File page : directoryListing) {
//				String name=page.getName();
//				System.out.println(name);
//			}
//		}
		
		
		DBApp dbApp = new DBApp();
//		String tableName = "courses";
//
//        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
//        htblColNameType.put("date_added", "java.util.Date");
//        htblColNameType.put("course_id", "java.lang.String");
//        htblColNameType.put("course_name", "java.lang.String");
//        htblColNameType.put("hours", "java.lang.Integer");
//
//
//        Hashtable<String, String> minValues = new Hashtable<>();
//        minValues.put("date_added", "1990-01-01");
//        minValues.put("course_id", "100");
//        minValues.put("course_name", "AAAAAA");
//        minValues.put("hours", "1");
//
//        Hashtable<String, String> maxValues = new Hashtable<>();
//        maxValues.put("date_added", "2000-12-31");
//        maxValues.put("course_id", "2000");
//        maxValues.put("course_name", "zzzzzz");
//        maxValues.put("hours", "24");
//
//        dbApp.createTable(tableName, "date_added", htblColNameType, minValues, maxValues);
		
//        dbApp.init();
//
//        String table = "courses";
//        Hashtable<String, Object> row = new Hashtable();
//
//        Date date_added = new Date(2011 - 1900, 4 - 1, 1);
//        row.put("date_added", date_added);
//
//        row.put("course_id", "foo");
//        row.put("course_name", "bar");
//        row.put("hours", 13);
//
//        dbApp.insertIntoTable(table, row);
		Vector<Hashtable<String,Object>> v = null;
	      try {
	         FileInputStream fileIn = new FileInputStream("src\\main\\resources\\data\\courses[1](0).class");
	         ObjectInputStream in = new ObjectInputStream(fileIn);
	         v = (Vector) in.readObject();
	         in.close();
	         fileIn.close();
	      } catch (IOException i) {
	         i.printStackTrace();
	         return;
	      } catch (ClassNotFoundException c) {
	         System.out.println("Employee class not found");
	         c.printStackTrace();
	         return;
	      }
	      
	      System.out.println(v.get(0).toString());
		
		
	}
	
	
	
	
	
	
	public static String getFileTableName(String fileName) {
		String tableName="";
		int c=0;
		while(fileName.charAt(c)!='[') {
			tableName+=fileName.charAt(c);
			c++;
		}
		return tableName;
		
	}
	
	public static int getFilePageNumber(String fileName) {
		String pageNumber="";
		int c=0;
		while(fileName.charAt(c)!='[') {
			c++;
		}
		c++;
		while(fileName.charAt(c)!=']') {
			pageNumber+=fileName.charAt(c);
			c++;
		}
		
		return Integer.parseInt(pageNumber);
	}
	
	public static int getFileOverflowNumber(String fileName) {
		String overflowNumber="";
		int c=0;
		while(fileName.charAt(c)!='(') {
			c++;
		}
		c++;
		while(fileName.charAt(c)!=')') {
			overflowNumber+=fileName.charAt(c);
			c++;
		}
		
		return Integer.parseInt(overflowNumber);
	}
}