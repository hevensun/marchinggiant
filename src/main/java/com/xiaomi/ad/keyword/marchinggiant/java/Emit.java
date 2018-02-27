package com.xiaomi.ad.keyword.marchinggiant.java;

import java.io.Serializable;

/**
 * Created by cailiming on 17-5-23.
 */
public class Emit implements Serializable {
    private final String keyword;//匹配到的模式串
    private final int start;
    private final int end;

    /**
     * 构造一个模式串匹配结果
     * @param start   起点
     * @param end     重点
     * @param keyword 模式串
     */
    public Emit(final int start, final int end, final String keyword) {
        this.start = start;
        this.end = end;
        this.keyword = keyword;
    }

    /**
     * 获取对应的模式串
     * @return 模式串
     */
    public String getKeyword() {
        return this.keyword;
    }

    public int getStart() {
        return this.start;
    }

    public int getEnd() {
        return this.end;
    }

    @Override
    public String toString() {
        return super.toString() + "=" + this.keyword;
    }
}
