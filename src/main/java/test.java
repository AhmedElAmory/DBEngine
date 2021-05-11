import javax.management.ObjectName;
import java.io.*;
import java.lang.reflect.Array;
import java.time.Duration;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

public class test {

	Hashtable<String,ArrayList<Grid>> allIndexes;

	public test(){
		allIndexes =  new Hashtable<String,ArrayList<Grid>>();
	}

	public static void main(String[] args) throws DBAppException {



//		Hashtable<String,Object> a = new Hashtable<String,Object>();
//		a.put("id","1");
//		a.put("gpa",1.0);
//		a.put("first_name","mohamed");
//		page.add(a);
//
//		try {
//			FileOutputStream fileOut = new FileOutputStream("src\\main\\resources\\data\\students[1](0).class" );
//			ObjectOutputStream out = new ObjectOutputStream(fileOut);
//			out.writeObject(page);
//			out.close();
//			fileOut.close();
//		} catch (IOException i) {
//			i.printStackTrace();
//		}

		test t1 = new test();
		String[] arr = {"gpa","first_name"};
		t1.createIndex("students",arr);

		Vector<BucketItem> page = new Vector<BucketItem>();
		page=DBApp.readBucketIntoVector("Bstudents[0][8](0).class");
		System.out.println(page);

		System.out.println(t1.allIndexes);




		//DBApp x = new DBApp();

//		ArrayList<String> a = new ArrayList<String>();
//		a.add("aa");
//		a.add("bb");
//
//		System.out.println(a.contains("aa")? "sdsds":"asasaasas");

//		Hashtable<String, ArrayList<String>> a = new Hashtable<String, ArrayList<String>>();
//
//		if(a.get("asas")==null){
//			System.out.println("dssd");
//		}



//		Vector<Hashtable<String, Object>> page = new Vector<Hashtable<String,Object>>();
//		
//		Hashtable<String, Object> a = new Hashtable<String, Object>();
//		a.put("name", "mohamed");
//		a.put("age", 15);
//		
//		page.add(a);
//		
//		
//		try {
//			FileOutputStream fileOut = new FileOutputStream("src\\main\\resources\\data\\tessst.class");
//			ObjectOutputStream out = new ObjectOutputStream(fileOut);
//			out.writeObject(page);
//			out.close();
//			fileOut.close();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
		
		//Vector<Hashtable<String, Object>> page2 = DBApp.readPageIntoVector("tessst");
		
		
		

		//System.out.println(page);

		
//		Object[] arr= new Object[10];
//
//		int x=2;
//
//		goDeeper(arr,x-1);
//
//		goDeeperCheck(arr,x-1);
//
//
//		String mind="2003-02-28";
//		String maxd="2003-03-01";
//
//		String minDates[] =mind.split("-");
//		String maxDates[] =maxd.split("-");
//
//		LocalDate minDate= LocalDate.of(Integer.parseInt(minDates[0]), Integer.parseInt(minDates[1]),
//				Integer.parseInt(minDates[2]));
//
//		LocalDate maxDate= LocalDate.of(Integer.parseInt(maxDates[0]), Integer.parseInt(maxDates[1]),
//				Integer.parseInt(maxDates[2]));
//
//		long daysBetween = ChronoUnit.DAYS.between(minDate, maxDate);
//
//		minDate=minDate.plusDays(20);
//		System.out.println(minDate.toString() );
//
//		System.out.println("Days: " + daysBetween);
//
//
//		Range ranges[][]=new Range[1][10];
//
//		String min="aaaaa";
//		String max="zzzzz";
//
//
//		String standard="";
//		for(int i=0;i<Math.max(min.length(), max.length());i++) {
//			if(i<min.length()&&min.charAt(i)==max.charAt(i)) {
//				standard+=min.charAt(i);
//			}else if(i<min.length()&&min.charAt(i)!=max.charAt(i)) {
//				int dif=max.charAt(i)-min.charAt(i);
//				int range=dif/10;
//
//				boolean flag=false;
//
//				ranges[0][0]=new Range(min,standard+(char)(min.charAt(i)+range));
//				range+=dif/10;
//				for(int j=1;j<9;j++) {
//					ranges[0][j]=new Range(ranges[0][j-1].max,standard+(char)(min.charAt(i)+range));
//					range+=dif/10;
//				}
//				ranges[0][9]=new Range(ranges[0][8].max,max);
//				break;
//			}else if(i==min.length()) {
//				int dif=max.charAt(i);
//				int range=dif/10;
//
//				ranges[0][0]=new Range(min,standard+(char)(min.charAt(i)+range));
//				range+=dif/10;
//				for(int j=1;j<9;j++) {
//					ranges[0][j]=new Range(ranges[0][j-1].max,standard+(char)(min.charAt(i)+range));
//					range+=dif/10;
//				}
//				ranges[0][9]=new Range(ranges[0][8].max,max);
//				break;
//			}
//		}
//
//		for(int i=0;i<10;i++) {
//			System.out.println(ranges[0][i].min+"     "+ranges[0][i].max);
//		}

		
	}


	// following method creates one index – either multidimensional
	// or single dimension depending on the count of column names passed.
	public void createIndex(String strTableName, String[] strarrColName) throws DBAppException {
		String primarycolAndDataType = checkCreateIndexExceptions(strTableName,strarrColName);
		String[] x = primarycolAndDataType.split(",");
		String primaryCol = x[0];
		String primaryDataType = x[1];
		Grid index = new Grid(strTableName,strarrColName,primaryCol,primaryDataType);
		ArrayList<Grid> a = allIndexes.get(strTableName);
		if(a==null){
			a =new ArrayList<Grid>();
			a.add(index);
			allIndexes.put(strTableName,a);
		}else{
			a.add(index);
		}

		try {
			FileOutputStream fileOut = new FileOutputStream("src\\main\\resources\\indicesAndBuckets\\indices.class");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(allIndexes);
			out.close();
			fileOut.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}


	public static String checkCreateIndexExceptions(String strTableName, String[] strarrColName) throws DBAppException {

		String primaryColumn="";
		String primaryDataType="";
		try {
			BufferedReader br = new BufferedReader(new FileReader("src\\main\\resources\\metadata.csv"));
			String current = br.readLine();

			// check if column names in hashtable exist in metadata
			int countCorrectColumns = 0;
			boolean tableExist = false;
			ArrayList<String> listOfCol = new ArrayList<String>();
			for(int i=0; i<strarrColName.length ;i++){
				listOfCol.add(strarrColName[i]);
			}

			while (current != null) {
				String arr[] = current.split(",");
				// check if metadata row has same table name as input
				if (arr[0].equals(strTableName)) {
					tableExist=true;
					// check if hashtable contains same column name as metadata
					if (listOfCol.contains(arr[1])) {
						countCorrectColumns++;
					}
					if(arr[3].equals("true")){
						primaryColumn=arr[1];
						primaryDataType=arr[2];
					}

				}
				current = br.readLine();
			}
			br.close();
			if (!tableExist) {
				throw new DBAppException("Table does not exist!");
			}
			if (!(countCorrectColumns == listOfCol.size())) {
				throw new DBAppException("Array Columns are not in metadata!");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return primaryColumn+","+primaryDataType;
	}
	
	public static void goDeeper(Object[] array,int n) {
		if(n==0) {
			for(int i=0;i<10;i++) {
				array[i]=i;
			}
			return;
		}
		
		for(int i=0;i<10;i++) {
			array[i]=new Object[10];
			goDeeper((Object[])array[i],n-1);
		}
		
	}
	//goDeeperCheck has array, level, hashtable with level,value
	public static void goDeeperCheck(Object[] array,int n) {
		if(n==0) {
			for(int i=0;i<10;i++) {
//				System.out.println((int)array[i]);
			}
		}else {
			for(int i=0;i<10;i++) {
				goDeeperCheck((Object[])array[i],n-1);
			}
		}
	}
}
