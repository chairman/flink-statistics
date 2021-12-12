package Rec;

import Rec.dao.ExpoStatDao;
import Rec.dao.ExpoStatDaoImpl;
import Rec.dto.ShowLog;
import Rec.dto.TraceLog;
import Rec.hbasedao.HbaseOperator;
import Rec.model.ExpoStat;
import Rec.serializer.KafkaSerializer;
import Rec.utils.BasePropertiesUtils;
import Rec.utils.JsonUtil;
import Rec.utils.RedisConfig;
import Rec.utils.TimeUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Main {
    private static Logger LOG = LoggerFactory.getLogger(Main.class);
    private static HbaseOperator hbaseOperator = new HbaseOperator();
    private static ExpoStatDao expoStatDao = new ExpoStatDaoImpl();
    private static RedisConfig redisConfig = new RedisConfig();

    public static void main(String[] args) {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.enableCheckpointing(3000, CheckpointingMode.EXACTLY_ONCE);
            env.setStateBackend((StateBackend)new FsStateBackend(BasePropertiesUtils.getProperty("state.backend")));
            env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

            String showtopic = BasePropertiesUtils.getProperty("show.kafka.topic");
            String tracetopic = BasePropertiesUtils.getProperty("trace.kafka.topic");

            String[] topics = {showtopic,tracetopic};
            Properties properties = new Properties();
            properties.put("bootstrap.servers",BasePropertiesUtils.getProperty("bootstrap.servers"));
            properties.setProperty("group.id",BasePropertiesUtils.getProperty("group.id"));

            DataStream<KafkaMessage> lines = env.addSource(
                    new FlinkKafkaConsumer010<KafkaMessage>(
                            Arrays.asList(topics),
                            new KafkaSerializer(),
                            properties
                    )
            );

            //处理阅读
            lines.filter(record -> tracetopic.equals(record.getTopic()))
                    .map(KafkaMessage::getMessageBody)
                    .map(msg->{
                        TraceLog traceLog = JsonUtil.json2Obj(msg,TraceLog.class);
                        return traceLog;
                    })
                    .filter(info ->info!=null)
                    .map(new TraceLogConsumerMapper());

            //处理曝光
            lines.filter(record -> showtopic.equals(record.getTopic()))
                    .map(KafkaMessage::getMessageBody)
                    .map(msg->{
                        ShowLog log = JsonUtil.json2Obj(msg,ShowLog.class);
                        return log;
                    })
                    .filter(info ->info!=null)
                    .map(new ShowLogConsumerMapper())
                    .windowAll(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                    .apply(new MyReduceWindowAllFunction());

            env.execute("app flink start");
        }catch (Exception e){
            LOG.error("main loop err",e.getMessage());
        }
    }

    private static class ShowLogConsumerMapper extends RichMapFunction<ShowLog,ExpoStat>{
        private Logger logger;
        @Override
        public ExpoStat map(ShowLog showLog) throws Exception {
            logger = LoggerFactory.getLogger(this.getClass());
            ExpoStat expoStat = getInitExpoStat(showLog);
            getDetailsExpoStat(showLog,expoStat);
            return expoStat;
        }

        private ExpoStat getDetailsExpoStat(ShowLog showLog,ExpoStat expoStat){
            int expoNum = 1;
            int chapterNum = 0;
            long wordsNum = 0;
            int income = 0;
            boolean isRead = hbaseOperator.exitsUserBookReadByKey(showLog.getUserid(),showLog.getBookid());
            chapterNum = getChapterNum(showLog,isRead);
            wordsNum = getWordsNum(showLog,isRead);
            if(expoStat.getIs_free() == 0){
                //免费统计
                expoStat.setExpoNum(expoNum);
                expoStat.setChapterNum(chapterNum);
                expoStat.setWordsNum(wordsNum);
                expoStat.setIncome(income);
            }

            if(expoStat.getIs_free() == 1){
                //收费的统计
                return getIncomeDetail(showLog,expoStat,expoNum,chapterNum,wordsNum,income,isRead);
            }
            return null;
        }

        private ExpoStat getIncomeDetail(ShowLog showLog,ExpoStat expoStat,int expoNum
                ,int chapterNum,long wordsNum,int income,boolean isRead){
            String sdaystr = new SimpleDateFormat("yyyyMMdd", Locale.CHINA).format(Calendar.getInstance().getTime());
            String expoNumKey = String.format("expoNum:%d:%s:%d",expoStat.getGroup(),sdaystr,expoStat.getHour());
            String chapterNumKey = String.format("chapterNum:%d:%s:%d",expoStat.getGroup(),sdaystr,expoStat.getHour());
            String wordsNumKey = String.format("wordsNum:%d:%s:%d",expoStat.getGroup(),sdaystr,expoStat.getHour());

            expoNum += getFromRedis(expoNumKey);
            chapterNum += getFromRedis(chapterNumKey);
            wordsNum += getFromRedis(wordsNumKey);
            income += getIncome(showLog,isRead);
            if(income>0){
                del2Redis(expoNumKey);
                del2Redis(chapterNumKey);
                del2Redis(wordsNumKey);
                expoStat.setExpoNum(expoNum);
                expoStat.setChapterNum(chapterNum);
                expoStat.setIncome(income);
                expoStat.setWordsNum(wordsNum);
            }else {
                set2Redis(expoNumKey,String.valueOf(expoNum));
                set2Redis(chapterNumKey,String.valueOf(chapterNum));
                set2Redis(wordsNumKey,String.valueOf(wordsNum));
                expoStat = null;
            }
            return expoStat;
        }

        public void set2Redis(String key,String value){
            Jedis jedis = null;
            try {
                JedisSentinelPool pool = redisConfig.getPool(key);
                jedis = pool.getResource();
                jedis.set(key,value);
                jedis.expire(key,24*60*60);
            }catch (Exception e){
                logger.error(e.getMessage());
            }finally {
                if(jedis!=null) jedis.close();
            }
        }

        public void del2Redis(String key){
            Jedis jedis = null;
            try {
                JedisSentinelPool pool = redisConfig.getPool(key);
                jedis = pool.getResource();
                jedis.del(key);
            }catch (Exception e){
                logger.error("redis del err",e.getMessage());
            }finally {
                if(jedis!=null) jedis.close();
            }
        }

        private int getIncome(ShowLog showLog,boolean isRead){
            int income = 0;
            if(isRead) return income;
            int newincome = hbaseOperator.getUserCostByKey(showLog.getUserid(),showLog.getBookid());
            income = income + newincome;
            return income;
        }

        public Integer getFromRedis(String key){
            Jedis jedis = null;
            try {
                JedisSentinelPool pool = redisConfig.getPool(key);
                jedis = pool.getResource();
                String str = jedis.get(key);
                if(str!=null) return Integer.parseInt(str);
            }catch (Exception e){
                logger.error("get from redis err:",e.getMessage());
            }finally {
                if(jedis!=null) jedis.close();
            }
            return 0;
        }

        public long getWordsNum(ShowLog showLog,boolean isRead){
            long wordsNum = 0;
            try {
                if(isRead) return wordsNum;
                if(showLog.getChapterid() != null && showLog.getChapterid() > 0) {
                    long orinwordsNum = hbaseOperator.getChaptersByKey(showLog.getBookid(), String.valueOf(showLog.getChapterid()));
                    wordsNum = wordsNum + orinwordsNum;
                }
            }catch (Exception e){

            }
            return wordsNum;
        }

        private int getChapterNum(ShowLog showLog,boolean isRead){
            int chapterNum = 0;
            try {
                if(isRead) return chapterNum;

                if(showLog.getChapterid()!=null&&showLog.getChapterid()>0){
                    String key = String.format("%s:%d:%s",showLog.getUserid(),showLog.getBookid(),TimeUtil.stampToTimeForRedisKey());
                    RedisConfig redisConfig = new RedisConfig();
                    Jedis jedis = null;
                    try {
                        JedisSentinelPool pool = redisConfig.getPool(key);
                        jedis = pool.getResource();
                        Map<String,String> uidtimemap0 = jedis.hgetAll(key);
                        String var2 = String.valueOf(showLog.getChapterid());
                        jedis.hincrBy(key,var2,1);
                        jedis.expire(key,60*60*24);
                        Map<String,String> uidtimemap1 = jedis.hgetAll(key);
                        if(uidtimemap1.size() > uidtimemap0.size()) chapterNum++;
                    }catch (Exception e){
                        logger.error("redis deal err:{}",e.getMessage());
                    }finally {
                        if(jedis!=null){
                            jedis.close();
                        }
                    }
                }
            }catch (Exception e){

            }
            return chapterNum;
        }

        private ExpoStat getInitExpoStat(ShowLog showLog){
            ExpoStat expoStat = new ExpoStat();
            try {
                Integer group = (Integer) (Integer.parseInt(showLog.getUserid()) % 100);
                Date day = new Date(Calendar.getInstance().getTime().getTime());
                int hour = 0;
                if( showLog.getServertime() >= TimeUtil.getToDayZreo() && showLog.getServertime() < TimeUtil.getTomorrowZreo()){
                    day = new Date(showLog.getServertime());
                    hour = new Date(showLog.getServertime()).getHours();
                }
                int is_free = 1;
                if(showLog.getNowchannel().startsWith("f_")) is_free = 0;
                expoStat.setGroup(group);
                expoStat.setDay(day);
                expoStat.setHour(hour);
                expoStat.setIs_free(is_free);
            }catch (Exception e){

            }
            return expoStat;
        }
    }

    private static class MyReduceWindowAllFunction implements AllWindowFunction<ExpoStat,String, TimeWindow>{
        private Logger logger;

        @Override
        public void apply(TimeWindow timeWindow, Iterable<ExpoStat> iterable, Collector<String> collector) throws Exception {
            logger = LoggerFactory.getLogger(this.getClass());
            Connection c = null;
            try {
                c = expoStatDao.getConnection();
                String sql = "insert into expo_stat (group,day,hour,is_free,expoNum,income,chapterNum,wordsNum) value (?,?,?,?,?,?,?,?)";
                PreparedStatement ps = c.prepareStatement(sql);
                int count = 0;
                for (ExpoStat expoStat:iterable){
                    ps.setInt(1,expoStat.getGroup());
                    ps.setDate(2,expoStat.getDay());
                    ps.setInt(3,expoStat.getHour());
                    ps.setInt(4,expoStat.getIs_free());
                    ps.setInt(5,expoStat.getExpoNum());
                    ps.setInt(6,expoStat.getIncome());
                    ps.setInt(7,expoStat.getChapterNum());
                    ps.setLong(8,expoStat.getWordsNum());
                    ps.addBatch();
                    count++;
                }
                ps.executeBatch();
                collector.collect("mps count"+count);
            }catch (Exception e){
                logger.error(e.getMessage());
            }finally {
                try {
                    if(c!=null) c.close();
                }catch (SQLException e){
                    logger.error(e.getMessage());
                }
            }
        }
    }

    private static class TraceLogConsumerMapper extends RichMapFunction<TraceLog,String>{
        private Logger logger;

        @Override
        public String map(TraceLog traceLog) throws Exception {
            logger = LoggerFactory.getLogger(this.getClass());
            int pt = TimeUtil.get0DayBefore();
            hbaseOperator.addUserBookRead(traceLog.getUserid(),traceLog.getBookid(),pt);
            return null;
        }
    }
}
