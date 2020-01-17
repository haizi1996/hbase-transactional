package com.hailin.hbase.themis;

import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableSet;

public abstract class ThemisRead extends ThemisRequest {

    protected boolean hasColumn(){
        return MapUtils.isNotEmpty(getFamilyMap());

    }

    public abstract Map<byte [], NavigableSet<byte []>> getFamilyMap();

    public abstract ThemisRead addFamily(byte[] family , byte[] qualifier) throws IOException;

    public abstract void setCacheBlocks(boolean cacheBlocks);

    public abstract boolean getCacheBlocks();

    public ThemisRead setFilter(Filter filter) throws IOException{
        ThemisCpUtil.processFilters(filter,

    }


    protected abstract ThemisRead setFilterWithoutCheck(org.apache.hadoop.hbase.filter.Filter filter);

    public abstract Filter getFilter();


}
