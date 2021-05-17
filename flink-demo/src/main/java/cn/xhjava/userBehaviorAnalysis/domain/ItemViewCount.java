package cn.xhjava.userBehaviorAnalysis.domain;


import lombok.Data;

/**
 * 商品页面
 */
@Data
public class ItemViewCount {
    private Long itemId;
    private Long windowEnd;
    private Long windowStart;
    private Long count;

    public ItemViewCount() {
    }

    public ItemViewCount(Long itemId, Long windowEnd, Long windowStart, Long count) {
        this.itemId = itemId;
        this.windowEnd = windowEnd;
        this.windowStart = windowStart;
        this.count = count;
    }

    public Long getItemId() {
        return itemId;
    }

    public void setItemId(Long itemId) {
        this.itemId = itemId;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }
}
