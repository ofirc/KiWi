package kiwi;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.Random;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Created by msulamy on 7/27/15.
 */
public class KiWiMap implements CompositionalMap<Integer,Integer>
{
	/***************	Constants			***************/
	
	/***************	Members				***************/
	public static boolean			SupportScan = true;
    public static int               RebalanceSize = 2;

	public KiWi<Integer,Integer>	kiwi;
    
    /***************	Constructors		***************/
    public KiWiMap()
    {
        ChunkInt.MAX_ITEMS = 100;
    	ChunkInt.initPool();
        KiWi.RebalanceSize = RebalanceSize;
    	this.kiwi = new KiWi<>(new ChunkInt(), SupportScan);
    }
    
    /***************	Methods				***************/

    /** same as put - always puts the new value! even if the key is not absent, it is updated */
    @Override
    public Integer putIfAbsent(Integer k, Integer v)
    {
    	kiwi.put(k, v);
        return null;	// can implement return value but not necessary
    }
    
    /** requires full scan() for atomic size() */
//    @Override
//    public int size()
//    {
//        return -1;
//    }
    
    /** not implemented ATM - can be implemented with chunk.findFirst() */
    @Override
    public boolean isEmpty()
    {
        return false;
    }

    @Override
    public Integer get(Object o)
    {
    	return kiwi.get((Integer)o);
    }

    @Override
    public Integer put(Integer k, Integer v)
    {
    	kiwi.put(k, v);
        size.incrementAndGet();
        sum.getAndAdd(k);
        return null;
    }

    /** same as put(key,null) - which signifies to KiWi that the item is removed */
    @Override
    public Integer remove(Object o)
    {
    	kiwi.put((Integer)o, null);
        size.decrementAndGet();
        sum.getAndAdd(-(Integer)o);
        return null;
    }

    @Override
    public int getRange(Integer[] result, Integer min, Integer max)
    {
        return kiwi.scan(result,min,max);
/*
    	Iterator<Integer> iter = kiwi.scan(min, max);
    	int i;
    	
    	for (i = 0; (iter.hasNext()) && (i < result.length); ++i)
    	{
    		result[i] = iter.next();
    	}
    	
    	return i;
*/
    }
    
    /** same as put(key,val) for each item */
    @Override
    public void putAll(Map<? extends Integer, ? extends Integer> map)
    {
    	for (Integer key : map.keySet())
    	{
    		kiwi.put(key, map.get(key));
    	}
    }
    
    /** Same as get(key) != null **/
    @Override
    public boolean containsKey(Object o)
    {
    	return get(o) != null;
    }

    /** Clear is not really an option (can be implemented non-safe inside KiWi) - we just create new kiwi **/
    @Override
    public void clear()
    {
    	//this.kiwi.debugPrint();
    	ChunkInt.initPool();
    	this.kiwi = new KiWi<>(new ChunkInt(), SupportScan);
    }

    /** Not implemented - can scan all & return keys **/
    @Override
    public Set<Integer> keySet()
    {
        throw new NotImplementedException();
    }

    /** Not implemented - can scan all & return values **/
    @Override
    public Collection<Integer> values()
    {
        throw new NotImplementedException();
    }

    /** Not implemented - can scan all & create entries **/
    @Override
    public Set<Entry<Integer,Integer>> entrySet()
    {
        throw new NotImplementedException();
    }    

    /** Not implemented - can scan all & search **/
    @Override
    public boolean containsValue(Object o)
    {
    	throw new NotImplementedException();
    }

    public void compactAllSerial()
    {
        kiwi.compactAllSerial();
    }
    public int debugCountDups()
    {
    	return kiwi.debugCountDups();
    }
    public int debugCountKeys()
    {
    	return kiwi.debugCountKeys();
    }
    public void debugPrint()
    {
    	kiwi.debugPrint();
    }

    public int debugCountDuplicates() { return kiwi.debugCountDuplicates();}
    public int debugCountChunks() {return 0; }

    public void calcChunkStatistics()
    {
        kiwi.calcChunkStatistics();
    }

    private static AtomicLong totalKeys = new AtomicLong(0);
    private static AtomicLong totalNodes = new AtomicLong(0);
    private static int numI;
    private static int numR;

    private AtomicInteger size = new AtomicInteger();
    private AtomicLong sum = new AtomicLong();

    @Override
    public int size()
    {
        return size.get();
    }

    private long getKeysum()
    {
        return sum.get();
    }

    private boolean contains(int key)
    {
        Integer val = get(key);
        return (val != null && val == 0);
    }

    public static void main2(String[] args) throws InterruptedException
    {
        KiWiMap kiwi = new KiWiMap();
        int key = 30;
        kiwi.put(key, 0);
        totalKeys.addAndGet(key);
        totalNodes.incrementAndGet();

        assert totalKeys.get() == kiwi.getKeysum();
        assert totalNodes.get() == kiwi.size();

        key = 90;
        kiwi.put(key, 0);
        totalKeys.addAndGet(key);
        totalNodes.incrementAndGet();

        assert totalKeys.get() == kiwi.getKeysum();
        assert totalNodes.get() == kiwi.size();

        kiwi.remove(key);
        totalKeys.addAndGet(-key);
        totalNodes.decrementAndGet();

        assert totalKeys.get() == kiwi.getKeysum();
        assert totalNodes.get() == kiwi.size();

        System.out.printf("%d %d%n", totalKeys.get(), kiwi.getKeysum());
        System.out.printf("%d %d%n", totalNodes.get(), kiwi.size());
    }

    private static final int num_iters = 1000000;

    public static void main(String[] args) throws InterruptedException
    {
        KiWiMap kiwi = new KiWiMap();

        ConcurrentMap concurrentMap = new ConcurrentHashMap<Integer, Integer>();

        Random random = new Random();
        final int numThreads = 1;
        ThreadPoolExecutor executor =
                (ThreadPoolExecutor) Executors.newFixedThreadPool(numThreads);

        for (int cur_iter = 0; cur_iter < num_iters; cur_iter++) {
//            if (cur_iter % (num_iters / 100) == 0) {
//                System.out.println(cur_iter / (num_iters / 100));
//            }
            int randonOp = random.nextInt(2);
            if (randonOp == 0) {
                final int key = random.nextInt(50000);
                executor.submit(() -> kiwi.put(key, 0));
                totalKeys.addAndGet(key);
                totalNodes.incrementAndGet();
                numI++;
                concurrentMap.put(key, 0);                
            } else {
                final int key = random.nextInt(50000);
                numR++;
                executor.submit(() -> {
                    if (concurrentMap.remove(key) != null) {
                        totalKeys.addAndGet(-key);
                        totalNodes.decrementAndGet();
                        kiwi.remove(key);
                    }
                });
//                boolean res = concurrentMap.remove((Integer)key);
//                if (res)
//                    executor.submit(() -> kiwi.remove(key));
            }
        }

        for (int cur_iter = 0; cur_iter < num_iters; cur_iter++) {
//            if (cur_iter % (num_iters / 100) == 0) {
//                System.out.println(cur_iter / (num_iters / 100));
//            }
            int randonOp = random.nextInt(3);
            if (randonOp == 0) {
                final int key = random.nextInt(50000);
                concurrentMap.put(key, 0);                
                totalKeys.addAndGet(key);
                totalNodes.incrementAndGet();
                numI++;
                executor.submit(() -> kiwi.put(key, 0));
            } else if(randonOp == 1){
                final int key = random.nextInt(50000);
                numR++;
                Integer val = (Integer)concurrentMap.remove(key);
                executor.submit(() -> {
                    if (concurrentMap.remove(key) != null) {
                        totalKeys.addAndGet(-key);
                        totalNodes.decrementAndGet();
                        kiwi.remove(key);
                    }
                });
//                boolean res = concurrentMap.remove(key);
//                if (res)
//                    executor.submit(() -> kiwi.remove(key));
            } else {
                final int key = random.nextInt(50000);
                executor.submit(() -> {
//                    if (kiwi.contains(key))
//                        System.out.println("kiwi contains" + key);
                  }
                );
            }
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.DAYS);
//        System.out.printf("%d %d%n", numR, numI);
        assert totalKeys.get() == kiwi.getKeysum();
        assert totalNodes.get() == kiwi.size();
        System.out.printf("%d %d%n", totalKeys.get(), kiwi.getKeysum());
        System.out.printf("%d %d%n", totalNodes.get(), kiwi.size());

    }
}
