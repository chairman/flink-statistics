package Rec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestLog4j {
    private static Logger logger = LoggerFactory.getLogger(TestLog4j.class);
    public static void main(String[] args) {
        // 插入记录信息（格式化日志信息）
        // 记录debug级别的信息
        logger.debug("调试信息.");
        // 记录info级别的信息
        logger.info("输出信息.");
        // 记录error级别的信息
        logger.error("错误信息.");
        // 记录warn级别的信息
        logger.warn("警告信息.");
        logger.trace("跟踪信息");
    }
}
