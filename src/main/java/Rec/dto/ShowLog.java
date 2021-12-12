package Rec.dto;

public class ShowLog {
    private String userid;
    private String bookid;
    private long servertime;
    private Integer chapterid;
    private String nowchannel;

    public String getUserid() {
        return userid;
    }

    public void setUserid(String userid) {
        this.userid = userid;
    }

    public String getBookid() {
        return bookid;
    }

    public void setBookid(String bookid) {
        this.bookid = bookid;
    }

    public long getServertime() {
        return servertime;
    }

    public void setServertime(long servertime) {
        this.servertime = servertime;
    }

    public Integer getChapterid() {
        return chapterid;
    }

    public void setChapterid(Integer chapterid) {
        this.chapterid = chapterid;
    }

    public String getNowchannel() {
        return nowchannel;
    }

    public void setNowchannel(String nowchannel) {
        this.nowchannel = nowchannel;
    }
}
