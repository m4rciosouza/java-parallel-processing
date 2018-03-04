import lombok.extern.slf4j.Slf4j;
import rx.Observable;
import rx.schedulers.Schedulers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * Created by marciosouza on 3/4/18.
 */
@Slf4j
public class ParallelFeatures {

    private static final boolean PRINT_NAMES = true;
    private static final boolean PRINT_THREADS_NAMES = true;
    private static final List<String> NAMES = Arrays.asList((
            "Marcio,Casale,Souza,Luzia,Matheus,Fabio,Edson,Deise,Milton,Zuma").split(","));

    public void singleThreadStream() {
        final long t1 = System.currentTimeMillis();
        List<String> names = NAMES.stream()
                .map(name -> greetingDelay(name))
                .collect(Collectors.toList());
        printResults(names);
        log.info("TIME SPENT FOR singleThreadStream: " + (System.currentTimeMillis()-t1) / 1000.0);
    }

    public void parallelThreadStream() {
        final long t1 = System.currentTimeMillis();
        List<String> names = NAMES.parallelStream()
                .map(name -> greetingDelay(name))
                .collect(Collectors.toList());
        printResults(names);
        log.info("TIME SPENT FOR parallelThreadStream: " + (System.currentTimeMillis()-t1) / 1000.0);
    }

    public void completableMainThreadStream() {
        final long t1 = System.currentTimeMillis();
        List<CompletableFuture<String>> futures = NAMES.stream()
                .map(name -> CompletableFuture.supplyAsync(() -> greetingDelay(name)))
                .collect(Collectors.toList());
        List<String> names = futures.stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList());
        printResults(names);
        log.info("TIME SPENT FOR completableMainThreadStream: " + (System.currentTimeMillis()-t1) / 1000.0);
    }

    public void completableCustomExecutorThreadStream() {
        final long t1 = System.currentTimeMillis();
        ExecutorService executor = Executors.newFixedThreadPool(Math.min(NAMES.size(), 10));
        List<CompletableFuture<String>> futures = NAMES.stream()
                .map(name -> CompletableFuture.supplyAsync(() -> greetingDelay(name), executor))
                .collect(Collectors.toList());
        List<String> names = futures.stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList());
        printResults(names);
        log.info("TIME SPENT FOR completableCustomExecutorThreadStream: " + (System.currentTimeMillis()-t1) / 1000.0);
    }

    public void observablesParallel() {
        final long t1 = System.currentTimeMillis();
        List<String> names = new ArrayList<>(10);
        Observable.from(NAMES)
                .flatMap(name -> Observable.just(name)
                .subscribeOn(Schedulers.io())
                .map(n -> greetingDelay(n)))
                .toBlocking()
                .subscribe(names::add);
        printResults(names);
        log.info("TIME SPENT FOR observablesParallel: " + (System.currentTimeMillis()-t1) / 1000.0);
    }

    private void printResults(List<String> names) {
        if (!PRINT_NAMES || names == null) return;
        log.info(String.join(", ", names));
    }

    private String greetingDelay(String name) {
        if (PRINT_THREADS_NAMES) {
            log.info("Processing item on thread " + Thread.currentThread().getName());
        }
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return String.format("Hello %s!", name);
    }

    public static void main(String[] args) {
        ParallelFeatures pf = new ParallelFeatures();
        log.info("### running ParallelFeatures.singleThreadStream ...");
        pf.singleThreadStream();
        log.info("### running ParallelFeatures.parallelThreadStream ...");
        pf.parallelThreadStream();
        log.info("### running ParallelFeatures.completableMainThreadStream ...");
        pf.completableMainThreadStream();
        log.info("### running ParallelFeatures.completableCustomExecutorThreadStream ...");
        pf.completableCustomExecutorThreadStream();
        log.info("### running ParallelFeatures.observablesParallel ...");
        pf.observablesParallel();
    }
}
