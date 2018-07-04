/**
 * Copyright 2015 Netflix, Inc.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.hystrix.metric.consumer;

import com.netflix.hystrix.metric.CachedValuesHistogram;
import com.netflix.hystrix.metric.HystrixEvent;
import com.netflix.hystrix.metric.HystrixEventStream;
import org.HdrHistogram.Histogram;
import rx.Observable;
import rx.Subscription;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.subjects.BehaviorSubject;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Maintains a stream of distributions for a given Command.
 * There is a rolling window abstraction on this stream.
 * The latency distribution object is calculated over a window of t1 milliseconds.  This window has b buckets.
 * Therefore, a new set of counters is produced every t2 (=t1/b) milliseconds
 * t1 = metricsRollingPercentileWindowInMilliseconds()
 * b = metricsRollingPercentileBucketSize()
 *
 * These values are stable - there's no peeking into a bucket until it is emitted
 *
 * These values get produced and cached in this class.
 */
public class RollingDistributionStream<Event extends HystrixEvent> {
    private AtomicReference<Subscription> rollingDistributionSubscription = new AtomicReference<Subscription>(null);
    private final BehaviorSubject<CachedValuesHistogram> rollingDistribution = BehaviorSubject.create(CachedValuesHistogram.backedBy(CachedValuesHistogram.getNewHistogram()));
    private final Observable<CachedValuesHistogram> rollingDistributionStream;

    // Histogram(直方图)累加
    private static final Func2<Histogram, Histogram, Histogram> distributionAggregator = new Func2<Histogram, Histogram, Histogram>() {
        @Override
        public Histogram call(Histogram initialDistribution, Histogram distributionToAdd) {
            initialDistribution.add(distributionToAdd);
            return initialDistribution;
        }
    };

    // Reduce操作符应用一个函数接收Observable发射的数据和函数的计算结果作为下次计算的参数，输出最后的结果
    // 迭代累加Histogram(直方图)
    private static final Func1<Observable<Histogram>, Observable<Histogram>> reduceWindowToSingleDistribution = new Func1<Observable<Histogram>, Observable<Histogram>>() {
        @Override
        public Observable<Histogram> call(Observable<Histogram> window) {
            return window.reduce(distributionAggregator);
        }
    };

    // 根据当前Histogram获取CachedValues（getMean，getValueAtPercentile)
    private static final Func1<Histogram, CachedValuesHistogram> cacheHistogramValues = new Func1<Histogram, CachedValuesHistogram>() {
        @Override
        public CachedValuesHistogram call(Histogram histogram) {
            return CachedValuesHistogram.backedBy(histogram);
        }
    };

    // Observable<T> 转换成 Observable<List<T>>
    private static final Func1<Observable<CachedValuesHistogram>, Observable<List<CachedValuesHistogram>>> convertToList =
            new Func1<Observable<CachedValuesHistogram>, Observable<List<CachedValuesHistogram>>>() {
                @Override
                public Observable<List<CachedValuesHistogram>> call(Observable<CachedValuesHistogram> windowOf2) {
                    return windowOf2.toList();
                }
            };

    /**
     *
     * @param stream
     * @param numBuckets 统计周期内bucket个数
     * @param bucketSizeInMs 统计周期内每个bucket时长
     * @param addValuesToBucket 将event事件统计入Histogram
     */
    protected RollingDistributionStream(final HystrixEventStream<Event> stream, final int numBuckets, final int bucketSizeInMs,
                                        final Func2<Histogram, Event, Histogram> addValuesToBucket) {
        final List<Histogram> emptyDistributionsToStart = new ArrayList<Histogram>();
        for (int i = 0; i < numBuckets; i++) {
            emptyDistributionsToStart.add(CachedValuesHistogram.getNewHistogram());
        }

        final Func1<Observable<Event>, Observable<Histogram>> reduceBucketToSingleDistribution = new Func1<Observable<Event>, Observable<Histogram>>() {
            @Override
            public Observable<Histogram> call(Observable<Event> bucket) {
                return bucket.reduce(CachedValuesHistogram.getNewHistogram(), addValuesToBucket);
            }
        };

        // window 每间隔bucketSizeInMs毫秒，生成一个Observable
        // flatMap 将第一步window按时间分隔的结果聚合成一个Observable<Histograms>
        // startWith 在开始插入n个初始化的Histogram
        rollingDistributionStream = stream
                .observe()
                .window(bucketSizeInMs, TimeUnit.MILLISECONDS) //stream of unaggregated buckets
                .flatMap(reduceBucketToSingleDistribution)     //stream of aggregated Histograms
                .startWith(emptyDistributionsToStart)          //stream of aggregated Histograms that starts with n empty
                .window(numBuckets, 1)                         //windowed stream: each OnNext is a stream of n Histograms
                .flatMap(reduceWindowToSingleDistribution)     //reduced stream: each OnNext is a single Histogram
                .map(cacheHistogramValues)                     //convert to CachedValueHistogram (commonly-accessed values are cached)
                .share()
                .onBackpressureDrop();
    }

    public Observable<CachedValuesHistogram> observe() {
        return rollingDistributionStream;
    }

    public int getLatestMean() {
        CachedValuesHistogram latest = getLatest();
        if (latest != null) {
            return latest.getMean();
        } else {
            return 0;
        }
    }

    public int getLatestPercentile(double percentile) {
        CachedValuesHistogram latest = getLatest();
        if (latest != null) {
            return latest.getValueAtPercentile(percentile);
        } else {
            return 0;
        }
    }

    public void startCachingStreamValuesIfUnstarted() {
        if (rollingDistributionSubscription.get() == null) {
            //the stream is not yet started
            Subscription candidateSubscription = observe().subscribe(rollingDistribution);
            if (rollingDistributionSubscription.compareAndSet(null, candidateSubscription)) {
                //won the race to set the subscription
            } else {
                //lost the race to set the subscription, so we need to cancel this one
                candidateSubscription.unsubscribe();
            }
        }
    }

    CachedValuesHistogram getLatest() {
        startCachingStreamValuesIfUnstarted();
        if (rollingDistribution.hasValue()) {
            return rollingDistribution.getValue();
        } else {
            return null;
        }
    }

    public void unsubscribe() {
        Subscription s = rollingDistributionSubscription.get();
        if (s != null) {
            s.unsubscribe();
            rollingDistributionSubscription.compareAndSet(s, null);
        }
    }
}
