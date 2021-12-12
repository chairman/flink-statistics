package Rec.hbasedao;

import Rec.utils.TimeUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.InputStream;
import java.util.Properties;

public class HbaseOperator {
    private static Logger LOG = LoggerFactory.getLogger(HbaseOperator.class);
    private static String hbaseZookeeperQuorum = "";
    private static String hbaseZookeeperPropertyClientPort = "";
    private static HbasePool pool = null;
    private static String user_cost =  "user_cost";
    private static String chapters =  "chapters";
    private static String user_book_chapter_read =  "user_book_chapter_read";
    static {
        try {
            InputStream in = HbaseOperator.class.getClassLoader().getResourceAsStream("hbase.properties");
            Properties props = new Properties();
            props.load(in);
            hbaseZookeeperQuorum = props.getProperty("hbase.zookeeper.quorum").trim();
            hbaseZookeeperPropertyClientPort = props.getProperty("hbase.zookeeper.property.clientPort").trim();
            pool = new Hbase(hbaseZookeeperQuorum,hbaseZookeeperPropertyClientPort);
            Runtime.getRuntime().addShutdownHook(new Thread((Runnable) ()->{pool.destory();}));
        }catch (Exception e){
            LOG.error("init err",e.getMessage());
        }
    }

    public boolean exitsUserBookReadByKey(String userid,String book_id){
        String columnFamily = "info";
        try {
            Table table = pool.getTable(user_book_chapter_read);
            try {
                Get g = new Get(Bytes.fromHex(MD5Hash.getMD5AsHex(Bytes.toBytes(userid+"_"+book_id))));
                table.get(g);
                Result rs = table.get(g);
                byte[] v = rs.getValue(Bytes.toBytes(columnFamily),Bytes.toBytes("pt"));
                if(v != null){
                    return Bytes.toInt(v) >= TimeUtil.get90DayBefore();
                }
            }finally {
                table.close();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return false;
    }

    public void addUserBookRead(String userid,String book_id,int pt){
        String columnFamily = "info";
        try {
            Table table = pool.getTable(user_book_chapter_read);
            try {
                Put put = new Put(Bytes.fromHex(MD5Hash.getMD5AsHex(Bytes.toBytes(userid+"_"+book_id))));
                put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes("pt"),Bytes.toBytes(pt));
                table.put(put);
            }finally {
                table.close();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public long getChaptersByKey(String book_id,String chapter_id){
        String columnFamily = "info";
        try {
            Table table = pool.getTable(user_cost);
            try {
                Get g = new Get(Bytes.fromHex(MD5Hash.getMD5AsHex(Bytes.toBytes(book_id+"_"+chapter_id))));
                Result rs = table.get(g);
                byte[] v = rs.getValue(Bytes.toBytes(columnFamily),Bytes.toBytes("word_count"));
                if(v!=null){
                    return Bytes.toLong(v);
                }
            }finally {
                table.close();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return 0;
    }

    public int getUserCostByKey(String userid,String bookid){
        String columnFamily = "info";
        try {
            Table table = pool.getTable(user_cost);
            try {
                Get g = new Get(Bytes.fromHex(MD5Hash.getMD5AsHex(Bytes.toBytes(userid+"_"+bookid))));
                Result rs = table.get(g);
                byte[] v = rs.getValue(Bytes.toBytes(columnFamily),Bytes.toBytes("amount"));
                if(v!=null){
                    return Bytes.toInt(v);
                }
            }catch (Exception e){
                table.close();
            }
        }catch (Exception e){
            LOG.error(e.getMessage());
        }
        return 0;
    }

}
