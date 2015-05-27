import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import com.es.offline.util.Pattern;


public class Test1 {

	public static void main(String[] args) throws IOException {
		String strs="atstorm-[yyyy.MM.dd],bjdx-user-new-2014.03.12,msg-test2*";
		
		String[] arr=strs.split(",");
		
		
		for (int i = 0; i < arr.length; i++) {
		 
			String str=arr[i];
			
			if(str.indexOf("[")!=-1 && str.indexOf("]")!=-1){
				String template=StringUtils.substringBetween(str, "[", "]");
				
				String service=str.substring(0, str.indexOf("["));
				if(service.endsWith("-")){
					service=StringUtils.removeEnd(service, "-");
				}
				System.out.println(template+"  "+service);
			}
			else if(StringUtils.endsWith(str, "*")){
				System.out.println("* ---"+str+"  "+StringUtils.removeEnd(str, "*"));
				
			}
			else{
				String service=Pattern.parsing(str);
				System.out.println("service "+service);
			}
			
		}
		
	
	}
	
	
	
}
