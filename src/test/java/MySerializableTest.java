import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.DeflateSerializer;

public class MySerializableTest {  
  
    public static void main(String[] args) throws IOException {  
    	 File f=new File("test.bin");
         System.out.println(f.delete());
        long start =  System.currentTimeMillis();  
        setSerializableObject();  
        System.out.println("Kryo Serializable writeObject time:" + (System.currentTimeMillis() - start) + " ms" );  
        start =  System.currentTimeMillis();  
        getSerializableObject();  
        System.out.println("Kryo Serializable readObject time:" + (System.currentTimeMillis() - start) + " ms");  
       
    }  
  
    public static void setSerializableObject() throws FileNotFoundException{  
  
        Kryo kryo = new Kryo();  
  
        kryo.setReferences(false);  
  
  
       // kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());  
  
        kryo.register(Simple.class);  

  
     
        Output output = new Output(new FileOutputStream("test.bin"));  
        try {
        	 for (int i = 0; i < 100000; i++) {  
                 Map<String,Integer> map = new HashMap<String, Integer>(2);  
                 map.put("zhang0", i);  
                 map.put("zhang1", i);  
                 kryo.writeObject(output, new Simple("zhang"+i,(i+1),map));  
             }  
             output.flush();  
          
             output.close();  
		} catch (Exception e) {
			e.printStackTrace();
		}
       
    }  
  
  
    public static void getSerializableObject(){  
        Kryo kryo = new Kryo();  
  
        kryo.setReferences(false);  
  
        kryo.register(Simple.class);  

      //  kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());  
  
        Input input;  
        try {  
            input = new Input(new FileInputStream("test.bin"));  
       
            Simple simple =null;  
            while(!input.eof()){  
            	simple=kryo.readObject(input, Simple.class);
               // System.out.println(simple.getAge() + "  " + simple.getName() + "  " + simple.getMap().toString());  
            }  
  
            input.close();  
        } catch (Exception e) {  
            e.printStackTrace();  
        } 
    }  
  
}  