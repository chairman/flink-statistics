package Rec.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class TimeUtil {
    public static int get90DayBefore(){
        Calendar c = Calendar.getInstance();
        c.add(Calendar.DATE,-90);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        String days_ago = sdf.format(c.getTime());
        return Integer.parseInt(days_ago);
    }

    public static int get0DayBefore(){
        Calendar c = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        String days_ago = sdf.format(c.getTime());
        return Integer.parseInt(days_ago);
    }

    public static long getToDayZreo(){
        Calendar c = Calendar.getInstance();
        c.set(Calendar.HOUR_OF_DAY,0);
        c.set(Calendar.MINUTE,0);
        c.set(Calendar.SECOND,0);
        return c.getTime().getTime();
    }

    public static long getTomorrowZreo(){
        Calendar c = Calendar.getInstance();
        c.set(Calendar.HOUR_OF_DAY,230);
        c.set(Calendar.MINUTE,59);
        c.set(Calendar.SECOND,59);
        return c.getTime().getTime();
    }

    public static String stampToTimeForRedisKey(){
        long currentTime = System.currentTimeMillis();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        return sdf.format(new Date(currentTime));
    }
}
