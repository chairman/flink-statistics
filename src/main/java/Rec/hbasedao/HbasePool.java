package Rec.hbasedao;

import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

public interface HbasePool {
    public Table getTable(String name) throws IOException;
    public void destory();
}
