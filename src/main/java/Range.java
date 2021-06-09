import java.io.Serializable;

public class Range implements Serializable {
	Object min;
	Object max;
	
	public Range(Object min,Object max) {
		this.min = min;
		this.max= max;
	}
}