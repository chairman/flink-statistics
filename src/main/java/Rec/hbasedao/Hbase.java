package Rec.hbasedao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.TableName;
import java.io.IOException;

public class Hbase implements HbasePool{
    Configuration conf;
    Connection conn;

    public Hbase(String hbaseZookeeperQurum,String hbaseZookeeperPropertyClientPort) throws Exception{
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum",hbaseZookeeperQurum);
        conf.set("hbase.zookeeper.property.clientPort",hbaseZookeeperPropertyClientPort);
        conn = ConnectionFactory.createConnection(conf);
    }

    public Hbase() throws Exception{
        conf = HBaseConfiguration.create();
        conf.addResource("hbase-site.xml");
        conn = ConnectionFactory.createConnection(conf);
    }

    @Override
    public Table getTable(String name) throws IOException {
        return conn.getTable(TableName.valueOf(name));
    }

    @Override
    public void destory() {
        if(conn!=null){
            try {
                conn.close();
            }catch (IOException e){
                e.printStackTrace();
            }
        }
    }
}
