import java.time.Duration;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Hashtable;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

public class test {
	public static void main(String[] args) {


		Vector<Hashtable<String, Object>> page = DBApp.readPageIntoVector("students[1](0).class");

		System.out.println(page);

		
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
