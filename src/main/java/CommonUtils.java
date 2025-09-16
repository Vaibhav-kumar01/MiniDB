import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class CommonUtils {

    public static String getString(byte[] bytes){
        //convert this into a string
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
