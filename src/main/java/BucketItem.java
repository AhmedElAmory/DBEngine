import java.io.Serializable;
import java.util.Hashtable;

public class BucketItem implements Serializable {

	 Object primaryKeyValue;
	 Hashtable<String,Object> colNamesAndValues;
	 String pageName;


	public BucketItem(Hashtable<String,Object> colNamesAndValues, String pageName,Object primaryKeyValye) {
		
		this.colNamesAndValues=colNamesAndValues;
		this.pageName=pageName;
		this.primaryKeyValue=primaryKeyValye;
	}

	@Override
	public String toString() {
		return "BucketItem{" +
				"primaryKeyValue=" + primaryKeyValue +
				", colNamesAndValues=" + colNamesAndValues +
				", pageName='" + pageName + '\'' +
				'}'+"\n";
	}
}
