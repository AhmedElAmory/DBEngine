import java.time.Duration;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class test {
	public static void main(String[] args) {
		
		Object[] arr= new Object[10];
		
		int x=2;
		
		goDeeper(arr,x-1);
		
		goDeeperCheck(arr,x-1);
		
		
		String min="2003-02-28";
		String max="2003-03-01";
		
		String minDates[] =min.split("-");
		String maxDates[] =max.split("-");
		
		LocalDate minDate= LocalDate.of(Integer.parseInt(minDates[0]), Integer.parseInt(minDates[1]),
				Integer.parseInt(minDates[2]));
		
		LocalDate maxDate= LocalDate.of(Integer.parseInt(maxDates[0]), Integer.parseInt(maxDates[1]),
				Integer.parseInt(maxDates[2]));
		
		long daysBetween = ChronoUnit.DAYS.between(minDate, maxDate);
		
		minDate=minDate.plusDays(20);
		System.out.println(minDate.toString() );
		
		System.out.println("Days: " + daysBetween);
		
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
