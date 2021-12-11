package Rec.model;

import java.io.Serializable;
import java.sql.Date;

public class ExpoStat implements Serializable {
    private int group;
    private Date day;
    private int hour;
    private int is_free;
    private int expoNum;
    private int income;
    private int chapterNum;
    private long wordsNum;

    public ExpoStat() {
    }

    public ExpoStat(int group, Date day, int hour, int is_free, int expoNum, int income, int chapterNum, long wordsNum) {
        this.group = group;
        this.day = day;
        this.hour = hour;
        this.is_free = is_free;
        this.expoNum = expoNum;
        this.income = income;
        this.chapterNum = chapterNum;
        this.wordsNum = wordsNum;
    }

    public int getGroup() {
        return group;
    }

    public void setGroup(int group) {
        this.group = group;
    }

    public Date getDay() {
        return day;
    }

    public void setDay(Date day) {
        this.day = day;
    }

    public int getHour() {
        return hour;
    }

    public void setHour(int hour) {
        this.hour = hour;
    }

    public int getIs_free() {
        return is_free;
    }

    public void setIs_free(int is_free) {
        this.is_free = is_free;
    }

    public int getExpoNum() {
        return expoNum;
    }

    public void setExpoNum(int expoNum) {
        this.expoNum = expoNum;
    }

    public int getIncome() {
        return income;
    }

    public void setIncome(int income) {
        this.income = income;
    }

    public int getChapterNum() {
        return chapterNum;
    }

    public void setChapterNum(int chapterNum) {
        this.chapterNum = chapterNum;
    }

    public long getWordsNum() {
        return wordsNum;
    }

    public void setWordsNum(long wordsNum) {
        this.wordsNum = wordsNum;
    }
}
