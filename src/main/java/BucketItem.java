import java.util.Hashtable;

public class BucketItem {
	
	Hashtable<String,Object> colNamesAndValues;
	String pageName;
	//int row;
	public BucketItem(Hashtable<String,Object> colNamesAndValues, String pageName) {
		
		this.colNamesAndValues=colNamesAndValues;
		this.pageName=pageName;
		//this.row=row;
	}

}
