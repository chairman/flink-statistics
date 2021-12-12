package Rec.utils;

import java.util.Locale;
import java.util.ResourceBundle;

public class BasePropertiesUtils {
    static String path = "base";
    static ResourceBundle resourceBundle = null;
    static {
        resourceBundle = java.util.ResourceBundle.getBundle(path, Locale.ENGLISH);
    }

    public static String getProperty(String name){
        String result = "";
        try {
            result = resourceBundle.getString(name).trim();
        }catch (Exception e){

        }
        return result;
    }
}
