package Rec.utils;

import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import java.text.SimpleDateFormat;

public class JsonUtil {
    private static SimpleDateFormat fullsdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static ObjectMapper objectMapper = new ObjectMapper();
    public JsonUtil(){
    }

    public static <T> T json2Obj(String json,Class<T> cls){
        try {
            return objectMapper.readValue(json,cls);
        }catch (Exception e){
            return null;
        }
    }

    public static <T> String objTojson(T o){
        try {
            return objectMapper.writeValueAsString(o);
        }catch (Exception e){
            return null;
        }
    }

    static {
        objectMapper.configure( Feature.ALLOW_UNQUOTED_FIELD_NAMES,true);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.setSerializationInclusion(Include.NON_NULL);
    }
}
