package Rec.utils;

public class StringUtil {
    public static final String EMPY_STRING = "";

    private final static String AS = "abcdefghijklmnopqrstuvwxyz1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    public static boolean isNumeric(String str){
        if(str==null) return false;
        int length = str.length();
        for (int i=0;i<length;i++){
            if(!Character.isDigit(str.charAt(i))) return false;
        }
        return true;
    }
}
