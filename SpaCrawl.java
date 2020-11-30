import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.math.RandomUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author xinrd.xu
 * @since 2019/12/19
 */
@Slf4j
public class SpaCrawl {


    public static void main(String[] args) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        SpaCrawl spaCrawl = new SpaCrawl();
        int i = 0;
        while (true) {
            int n = ++i;
            spaCrawl.request(new HotelPriceQuery(), new ResultHandler<Result>() {
                @Override
                public void done(Result priceWrappers) {
                    try {
                        log.info("第{}次请求返回 {} :{}", n, (System.currentTimeMillis() - priceWrappers.getCacheQueryTime()), mapper.writeValueAsString(priceWrappers));
                    } catch (JsonProcessingException e) {
                    }
                }
            });
            if (System.in.read() == '9') {
                break;
            }
        }

        executor.shutdownNow();
    }


    private static final double THRESHOLD_SCORE = 0.9f;

    private static final Map<String, CrawlJob> CRAWL_JOBS = Maps.newConcurrentMap();

    /**
     * @param hQuery  酒店查询
     * @param handler 查询完成后回调
     */
    public void request(HotelPriceQuery hQuery, ResultHandler<Result> handler) {
        List<Wrapper> wrappers = queryOnSaleWrappers(hQuery);
        Map<String, Price> prices = queryPricesFromCache(hQuery, wrappers);
        Result result = new Result(prices);
        result.setOnSaleWrappers(wrappers);
        result.setRelativeWeights(calculateRelativeWrapperWeights(wrappers));
        result.setScore(result.calculateFreshScore()); // init score
        if (result.isFreshEnough()) {
            log.info("match score after query redis, fresh enough {}", result.getScore());
            crawlPricesAsyncIfNeed(hQuery, wrappers, result, null);
            handler.done(result);
        } else {
            log.info("score not enough : {}", result.getScore());
            crawlPricesAsyncIfNeed(hQuery, wrappers, result, new LiveQueryResultUpdater(result, handler, 3000));
        }
    }

    private List<Wrapper> queryOnSaleWrappers(HotelPriceQuery hQuery) {
        List<Wrapper> rst = Lists.newArrayList();
        rst.add(new Wrapper("gctrip", 1000L, 6));
        rst.add(new Wrapper("booking", 2000L, 0.3));
        rst.add(new Wrapper("expedia", 3000L, 1));
        rst.add(new Wrapper("agoda", 4000L, 0.5));
        rst.add(new Wrapper("elong", 5000L, 0.2));
        rst.add(new Wrapper("wrapper1", 10000L, 0.01));
        rst.add(new Wrapper("wrapper2", 10000L, 0.01));
        rst.add(new Wrapper("wrapper3", 10000L, 0.01));
        rst.add(new Wrapper("wrapper4", 10000L, 0.01));
        rst.add(new Wrapper("wrapper5", 10000L, 0.01));
        return rst;
    }

    /**
     *
     */
    private void crawlPricesAsyncIfNeed(HotelPriceQuery hQuery, List<Wrapper> wrappers, Result result, LiveQueryResultUpdater resultUpdater) {
        StringBuilder attachLog = new StringBuilder(), crawlLog = new StringBuilder(), inCacheLog = new StringBuilder();
        for (Wrapper wrapper : wrappers) {
            Price price = result.prices.get(wrapper.wrapperId);
            if (price != null && (result.cacheQueryTime - price.crawlTs) < wrapper.cacheTime) {
                // this wrapper price is fresh enough, ignore crawl
                inCacheLog.append(wrapper.wrapperId).append(",");
                continue;
            }
            String key = generateJobKey(hQuery, wrapper);
            CrawlJob newJob = new CrawlJob(hQuery, wrapper, resultUpdater);
            CrawlJob crawlJob = CRAWL_JOBS.computeIfAbsent(key, s -> newJob);
            if (crawlJob != newJob) { // old job, attach Updater if not null
                attachLog.append(wrapper.wrapperId).append(",");
                if (resultUpdater != null) { // live query may have return, null
                    if (!crawlJob.attachLiveQuery(resultUpdater)) {
                        // ? 在发起一次抓取?, 先看监控吧,概率太小
                        log.error("#### try attaching but the crawl job is just over");
                    }
                }
            } else { // new job
                crawlLog.append(wrapper.wrapperId).append(",");
                crawlJob.startCrawl(); // do crawl
            }
        }
        log.info("fresh: " + inCacheLog);
        log.info("attach: " + attachLog);
        log.info("crawl: " + crawlLog);
    }

    private static String generateJobKey(HotelPriceQuery hQuery, Wrapper wrapper) {
        return "query_" + wrapper.getWrapperId();
    }

    // mock redis
    public static final Map<String, Price> redis = Maps.newConcurrentMap();

    private Map<String, Price> queryPricesFromCache(HotelPriceQuery hQuery, List<Wrapper> priceWrappers) {
        Map<String, Price> result = Maps.newHashMap();
        for (Wrapper wrapper : priceWrappers) {
            Price price = redis.get(wrapper.getWrapperId());
            if (price != null) {
                result.put(wrapper.getWrapperId(), price);
            }
        }
        return result;
    }

    private static class CrawlJob {

        private static final List<LiveQueryResultUpdater> DESTORY = new ArrayList<>();

        private HotelPriceQuery hQuery;
        private Wrapper wrapper;
        private volatile LiveQueryResultUpdater initUpdater;
        private List<LiveQueryResultUpdater> attachedUpdater; // 搭车,只增不减

        CrawlJob(HotelPriceQuery hQuery, Wrapper wrapper, LiveQueryResultUpdater initUpdater) {
            this.hQuery = hQuery;
            this.wrapper = wrapper;
            this.initUpdater = initUpdater;
        }

        public void startCrawl() {
            SpaUtil.sendAsync(hQuery, wrapper, wrapperPrice -> {
                dealWithCrawlPriceAsync(wrapper, wrapperPrice);
                CRAWL_JOBS.remove(generateJobKey(hQuery, wrapper)); // detach
                notifyLiveUpdater(wrapperPrice);
            });
        }

        /**
         * 处理抓取结果
         */
        private void dealWithCrawlPriceAsync(Wrapper wrapper, Price freshPrice) {
            redis.put(wrapper.getWrapperId(), freshPrice);
        }

        /**
         * 由抓取IO回调线程调用触发完成
         */
        public void notifyLiveUpdater(Price freshPrice) {
            if (initUpdater != null) {
                initUpdater.update(wrapper, freshPrice);
            }
            synchronized (this) {
                if (attachedUpdater != null) {
                    for (int i = 0; i < attachedUpdater.size(); i++) {
                        LiveQueryResultUpdater updater = attachedUpdater.get(i);
                        if (updater == null) {
                            log.error("### null updater {}, {}", i, attachedUpdater);
                        }
                        updater.update(wrapper, freshPrice);
                    }
                }
                attachedUpdater = DESTORY;
            }
        }

        /**
         * 由request主线程调用,添加updater
         */
        public synchronized boolean attachLiveQuery(LiveQueryResultUpdater liveQueryUpdater) {
            if (attachedUpdater == DESTORY) {
                return false;
            }
            if (attachedUpdater == null) {
                attachedUpdater = Lists.newArrayListWithCapacity(2);
            }
            attachedUpdater.add(liveQueryUpdater);
            log.error("### {} {}", attachedUpdater.size(), wrapper.wrapperId);
            return true;
        }
    }

    /**
     * wrapper info for cache & crawl
     */
    @Data
    @AllArgsConstructor
    private static class Wrapper {
        private String wrapperId;
        private long cacheTime;
        private double weight; // 绝对权重
    }

    @Data
    private static class HotelPriceQuery {
    }

    @Data
    @AllArgsConstructor
    private static class Price {
        private long crawlTs;
        private String priceContent;
    }

    /**
     * 更新用户请求的结果
     * 调用update更新最新的抓取结果,并在足够新鲜的时候返回
     */
    @Data
    private class LiveQueryResultUpdater {
        private List<Wrapper> wrappers;
        private Result result;
        private ResultHandler handler;
        private boolean handled;
        private Map<String, Double> wrapperWeights; // weight per wrapper in this query wrapper set


        public LiveQueryResultUpdater(Result result, ResultHandler callback, long timeout) {
            this.wrappers = result.getOnSaleWrappers();
            this.result = result;
            this.handler = callback;

            this.result.prices = new ConcurrentHashMap<>(result.prices);
            this.wrapperWeights = calculateRelativeWrapperWeights(wrappers);
        }

        /**
         * updated by crawl result handler
         *
         * @return true if score enough
         */
        public synchronized void update(Wrapper wrapper, Price freshPrice) {
            if (handled) {
                log.info("ignore update, 'cause live query has completed");
                return;
            }
            double score = result.getScore();
            double weight = wrapperWeights.get(wrapper.wrapperId);
            Double os = result.getScoresPerWrapper().get(wrapper.getWrapperId());
            double oldScore = os == null ? 0d : os;
            double newScore = weight * 1d; // new score
            score += (newScore - oldScore);

            // update result
            result.getPrices().put(wrapper.getWrapperId(), freshPrice);
            result.getScoresPerWrapper().put(wrapper.wrapperId, newScore);
            result.setScore(score);

            log.info("update after crawled, old={}, new={}, delta={}, total={}", oldScore, newScore, newScore - oldScore, score);
            if (score > THRESHOLD_SCORE) {
                handled = true;
                log.info("match score after update, eager return");
                handler.done(result);
                clean(); // help gc
            }
        }

        private void clean() {
            this.wrappers = null;
            this.result = null;
            this.handler = null;
            this.wrapperWeights = null;
        }

    }

    /**
     * 算单个报价的 新鲜度
     */
    private static double calculateFreshScoreWrapperByTime(Wrapper wrapper, long crawlTs, long cacheQueryTime) {
        long expired = cacheQueryTime - crawlTs;
        if (expired < wrapper.cacheTime) {
            return 1d;
        } else {
            return wrapper.cacheTime * 1.0f / expired;
        }
    }

    private Map<String, Double> calculateRelativeWrapperWeights(List<Wrapper> wrappers) {
        Map<String, Double> rst = Maps.newHashMap();
        double total = 0;
        for (Wrapper wrapper : wrappers) {
            total += wrapper.weight;
        }
        for (Wrapper wrapper : wrappers) {
            rst.put(wrapper.getWrapperId(), wrapper.getWeight() / total);
        }
        return rst;
    }

    private interface ResultHandler<T> {
        void done(T priceWrappers);
    }

    // mock async http
    private static final ExecutorService executor = Executors.newCachedThreadPool();
    private static final AtomicInteger inc = new AtomicInteger(0);

    private static class SpaUtil {
        public static void sendAsync(HotelPriceQuery hQuery, Wrapper wrapper, ResultHandler<Price> handler) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    long l = RandomUtils.nextInt((int) wrapper.cacheTime);
//                    log.info("start crawl {}, going sleep for {}ms", wrapper.getWrapperId(), l);
                    try {
                        Thread.sleep(l);
                    } catch (InterruptedException e) {
                    }
                    Price freshPrice = new Price(System.currentTimeMillis(), wrapper.getWrapperId() + "_" + System.currentTimeMillis() + "_" + inc.incrementAndGet());
//                    log.info("end   crawl {}, end   sleep for {}ms , crawl result : {}", wrapper.getWrapperId(), l, freshPrice);
                    handler.done(freshPrice);
                }
            });
        }
    }

    @Data
    private static class Result {
        private long cacheQueryTime;
        private double score;
        private Map<String, Price> prices;
        private Map<String, Double> scoresPerWrapper = Maps.newHashMap();
        private List<Wrapper> onSaleWrappers;
        private Map<String, Double> relativeWeights;

        public Result(Map<String, Price> prices) {
            this.cacheQueryTime = System.currentTimeMillis();
            this.prices = prices;
        }

        public boolean isFreshEnough() {
            return score > THRESHOLD_SCORE;
        }

        /**
         * 计算酒店报价新鲜度
         */
        private double calculateFreshScore() {
            double total = 0;
            for (Wrapper wrapper : onSaleWrappers) {
                total += wrapper.weight;
            }
            double score = 0d;
            for (Wrapper wrapper : onSaleWrappers) {
                double relativeWeight = wrapper.getWeight() / total;
                Price price = getPrices().get(wrapper.getWrapperId());
                if (price != null) {
                    double wrapperScore = relativeWeight * calculateFreshScoreWrapperByTime(wrapper, price.crawlTs, cacheQueryTime);
                    scoresPerWrapper.put(wrapper.getWrapperId(), wrapperScore);
                    score += wrapperScore;
                }
            }
            return score;
        }

    }
}
