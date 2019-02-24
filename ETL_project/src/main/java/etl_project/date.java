
package etl_project;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class date {
	public static void main(String[] args) {
		Date date = new Date();
		
//		12/Dec/2017:00:17:34
		SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss",Locale.US);
		
		String time = sdf.format(date);
		System.out.println(time);
	}
}
