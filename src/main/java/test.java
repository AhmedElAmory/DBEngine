import java.lang.reflect.InvocationTargetException;
import java.util.Hashtable;

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
        
		
		
		System.out.println(getFilePageNumber("Employee[2](1)"));
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
			System.out.println("lol");
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