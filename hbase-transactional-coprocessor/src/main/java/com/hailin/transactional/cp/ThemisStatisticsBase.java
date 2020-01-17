package com.hailin.transactional.cp;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public class ThemisStatisticsBase {

    private static final Log LOG = LogFactory.getLog(ThemisStatisticsBase.class);
    public static final String THEMIS_SLOW_OPERATION_CUTOFF_KEY = "themis.slow.operation.cutoff";
    public static final long DEFAULT_THEMIS_SLOW_OPERATION_CUTOFF = 100;
    protected static long slowCutoff = DEFAULT_THEMIS_SLOW_OPERATION_CUTOFF * 1000; // in us
    private static final String EmptySlowOperationMsg = "";

    public static void init(Configuration conf) {
        slowCutoff = conf.getLong(ThemisCpStatistics.THEMIS_SLOW_OPERATION_CUTOFF_KEY,
                ThemisCpStatistics.DEFAULT_THEMIS_SLOW_OPERATION_CUTOFF) * 1000;
    }
}
