import java.lang.reflect.InvocationTargetException;
import java.util.Hashtable;

public class test {
	public static void main(String[] args) throws DBAppException {
		try {
			String wawa="lol";
			Integer mama=2;
			Object lol=(Object)2.4657;
			System.out.println(lol.getClass());
			System.out.println(Class.forName("java.lang.Double").isInstance(lol));
			
		} catch (IllegalArgumentException | SecurityException | ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}