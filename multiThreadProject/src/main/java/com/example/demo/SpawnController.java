package com.example.demo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.FileInputStream;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@RestController
public class SpawnController {

    @Autowired
    public SpawnService spawnService;

    @Autowired
    public ObjectMapper objectMapper;

    public ExecutorService async = Executors.newFixedThreadPool(10);

    @CrossOrigin
    @GetMapping(value = "/getSpawnResultWithAsync",
                produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Object> getSpawnResultWithAsync() throws
                                                            InterruptedException,
                                                            ExecutionException,
                                                            JsonProcessingException {

        System.out.printf("[%s] \n", Thread.currentThread().getName());
        long start = System.currentTimeMillis();
        CompletableFuture<List<String>> siteAContents = getListCompletableFuture(SpawnService.Site.A);
        CompletableFuture<List<String>> siteBContents = getListCompletableFuture(SpawnService.Site.B);

        CompletableFuture<Map<String, Object>> totalSiteContents = siteAContents
                // siteA, siteB completable 都完成後才會回傳
                .thenCombine(siteBContents, (siteA, siteB) -> {
                    // 合并成新的Map
                    Map<String, Object> totalSiteInfo = new HashMap<>();
                    totalSiteInfo.put("A", siteA);
                    totalSiteInfo.put("B", siteB);
                    return totalSiteInfo;
                });
        Map<String, Object> result = totalSiteContents.join();
        long end = System.currentTimeMillis();
        double v = (end - start) / 1000d;
        System.out.println("cost time:" + v + " s");
        System.out.println("final result: " + objectMapper.writeValueAsString(result));
        return new ResponseEntity(result, HttpStatus.OK);
    }

    private CompletableFuture<List<String>> getListCompletableFuture(SpawnService.Site site) {
        CompletableFuture<List<String>> pageAContents = CompletableFuture.supplyAsync(() -> {
                                                                             Integer page = spawnService.getPage(site);
                                                                             System.out.printf("[%s] get %s page count %s \n",
                                                                                               Thread.currentThread()
                                                                                                     .getName(),
                                                                                               site.getDescription(),
                                                                                               page);
                                                                             return page;
                                                                         }, async)
                                                                         .thenApply((pages) -> {
                                                                             System.out.printf(
                                                                                     "[%s] get %s page Info accord to pages %s \n",
                                                                                     Thread.currentThread()
                                                                                           .getName(),
                                                                                     site.getDescription(),
                                                                                     pages);
                                                                             List<CompletableFuture<List<String>>> pageContents = new ArrayList<>();
                                                                             int threadCount = pages / 10 + 1;
                                                                             int restPages = pages % 10;
                                                                             ExecutorService threadPagesPool = Executors.newFixedThreadPool(
                                                                                     threadCount);
                                                                             for (int i = 1; i <= threadCount; i++) {
                                                                                 int start = (i - 1) * 10;
                                                                                 int end = i * 10 - 1;
                                                                                 if (i == threadCount) {
                                                                                     end = start + restPages;
                                                                                 }
                                                                                 // 找出此thread 需爬頁數
                                                                                 List<Integer> pageRange = IntStream.range(
                                                                                                                            start,
                                                                                                                            end + 1)
                                                                                                                    .boxed()
                                                                                                                    .collect(
                                                                                                                            Collectors.toList());

                                                                                 pageContents.add(CompletableFuture.supplyAsync(
                                                                                         () -> {
                                                                                             Thread.currentThread()
                                                                                                   .getName();
                                                                                             System.out.printf(
                                                                                                     "[%s] get %s page range %s \n",
                                                                                                     Thread.currentThread()
                                                                                                           .getName(),
                                                                                                     site.getDescription(),
                                                                                                     pageRange);
                                                                                             List<String> siteInfo = spawnService.getSiteInfo(
                                                                                                     site,
                                                                                                     pageRange);
                                                                                             System.out.printf(
                                                                                                     "[%s] get %s page %s, and siteInfo is %s \n",
                                                                                                     Thread.currentThread()
                                                                                                           .getName(),
                                                                                                     site.getDescription(),
                                                                                                     pageRange,
                                                                                                     siteInfo);
                                                                                             return siteInfo;
                                                                                         },
                                                                                         threadPagesPool));
                                                                             }

                                                                             CompletableFuture<List<String>> allFutureResults = CompletableFuture.allOf(
                                                                                                                                                         pageContents.toArray(new CompletableFuture[pageContents.size()]))
                                                                                                                                                 .thenApply(
                                                                                                                                                         v -> {
                                                                                                                                                             return pageContents.stream()
                                                                                                                                                                                .peek(c -> System.out.println(
                                                                                                                                                                                        site.getDescription()
                                                                                                                                                                                                + " isDone: "
                                                                                                                                                                                                + c.isDone()
                                                                                                                                                                                                + "!"))
                                                                                                                                                                                .map(CompletableFuture::join)
                                                                                                                                                                                .flatMap(
                                                                                                                                                                                        Collection::stream)
                                                                                                                                                                                .collect(
                                                                                                                                                                                        Collectors.toList());
                                                                                                                                                         });
                                                                             ;

                                                                             try {
                                                                                 return allFutureResults.get();
                                                                             } catch (InterruptedException | ExecutionException e) {
                                                                                 return new ArrayList<>();
                                                                             }
                                                                         });
        return pageAContents;
    }

    @CrossOrigin
    @GetMapping(value = "/getSpawnResultWithOutAsync",
                produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Object> getSpawnResultWithOutAsync() throws
                                                               InterruptedException,
                                                               ExecutionException,
                                                               JsonProcessingException {

        System.out.printf("[%s] \n", Thread.currentThread().getName());
        long start = System.currentTimeMillis();
        Integer pageA = spawnService.getPage(SpawnService.Site.A);
        List<Integer> pageRangeA = IntStream.range(0, pageA + 1)
                                           .boxed()
                                           .collect(Collectors.toList());
        List<String> resultA = spawnService.getSiteInfo(SpawnService.Site.A, pageRangeA);

        Integer pageB = spawnService.getPage(SpawnService.Site.B);
        List<Integer> pageRangeB = IntStream.range(0, pageB + 1)
                                           .boxed()
                                           .collect(Collectors.toList());
        List<String> resultB = spawnService.getSiteInfo(SpawnService.Site.B, pageRangeB);
        Map<String, Object> totalSiteInfo = new HashMap<>();
        totalSiteInfo.put("A", resultA);
        totalSiteInfo.put("B", resultB);
        long end = System.currentTimeMillis();
        double v = (end - start) / 1000d;
        System.out.println("cost time:" + v + " s");
        System.out.println("final result: " + objectMapper.writeValueAsString(totalSiteInfo));
        return new ResponseEntity(totalSiteInfo, HttpStatus.OK);
    }


    @CrossOrigin
    @GetMapping(value = "/getBigQueryData",
                produces = MediaType.APPLICATION_JSON_VALUE)
    public  ResponseEntity<Void> getBigQueryData() throws Exception {
        // [START bigquery_simple_app_client]
        Credentials credentials = GoogleCredentials
                .fromStream(new FileInputStream("/Users/joe.su/Downloads/pmax-rich-media-analytics-af62497c6014.json"));
        BigQuery bigquery = BigQueryOptions
                .newBuilder()
                .setCredentials(credentials)
                .build()
                .getService();
        // [END bigquery_simple_app_client]
        // [START bigquery_simple_app_query]
//        QueryJobConfiguration queryConfig =
//                QueryJobConfiguration.newBuilder(
//                                             "SELECT commit, author, repo_name "
//                                                     + "FROM `bigquery-public-data.github_repos.commits` "
//                                                     + "WHERE subject like '%bigquery%' "
//                                                     + "ORDER BY subject DESC LIMIT 10")
//                                     // Use standard SQL syntax for queries.
//                                     // See: https://cloud.google.com/bigquery/sql-reference/
//                                     .setUseLegacySql(false)
//                                     .build();

        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(
                                             "SELECT event_name, count(*) FROM `pmax-rich-media-analytics.analytics_337228405.events_*` GROUP BY event_name")
                                     // Use standard SQL syntax for queries.
                                     // See: https://cloud.google.com/bigquery/sql-reference/
                                     .setUseLegacySql(false)
                                     .build();
        // Create a job ID so that we can safely retry.
        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        // Wait for the query to complete.
        queryJob = queryJob.waitFor();

        // Check for errors
        if (queryJob == null) {
            throw new RuntimeException("Job no longer exists");
        } else if (queryJob.getStatus().getError() != null) {
            // You can also look at queryJob.getStatus().getExecutionErrors() for all
            // errors, not just the latest one.
            throw new RuntimeException(queryJob.getStatus().getError().toString());
        }
        // [END bigquery_simple_app_query]

        // [START bigquery_simple_app_print]
        // Get the results.
        TableResult result = queryJob.getQueryResults();

        // Print all pages of the results.
        for (FieldValueList row : result.iterateAll()) {
            // String type
            String commit = row.get("commit").getStringValue();
            // Record type
            FieldValueList author = row.get("author").getRecordValue();
            String name = author.get("name").getStringValue();
            String email = author.get("email").getStringValue();
            // String Repeated type
            String repoName = row.get("repo_name").getRecordValue().get(0).getStringValue();
            System.out.printf(
                    "Repo name: %s Author name: %s email: %s commit: %s\n", repoName, name, email, commit);
        }
        // [END bigquery_simple_app_print]

        return ResponseEntity.ok().build();
    }
}
