
package test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.couchbase.client.core.metrics.DefaultLatencyMetricsCollectorConfig;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.deps.com.fasterxml.jackson.core.JsonParseException;
import com.couchbase.client.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.JsonMappingException;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

@RestController
@RequestMapping(value ="/v1/consumer")
public class TestResource {
	
	@Autowired
	HttpServletRequest request;	
	
	@Autowired
	HttpServletResponse response;
	

	ObjectMapper mapper = new ObjectMapper();
	
	final Logger logger  = Logger.getLogger("com.couchbase.client");
	
	LimitedStringQueue logs = new LimitedStringQueue(1000);

	TestConfig original = new TestConfig();

	AtomicInteger iteration = new AtomicInteger(0);

	AtomicBoolean running = new AtomicBoolean(false);

	AtomicLong count = new AtomicLong(0);

	AtomicLong statusPrinted = new AtomicLong(0);

	AtomicInteger fails = new AtomicInteger(0);

	AtomicReference<TestConfig> configuration = new AtomicReference<TestConfig>(original);

	ExecutorService pool = Executors.newFixedThreadPool(configuration.get().getConcurrent());

	Random random = new Random();

	enum Client {
		couchbase,
	};

	 @RequestMapping(value = "/test/path" ,method=RequestMethod.POST , produces="text/html")
	 public String testPost(@RequestParam(value="reset" ,defaultValue="false") boolean reset, @RequestParam(value="halt",defaultValue="false") boolean halt, @RequestParam(value="restart",defaultValue="false") boolean restart, @RequestParam(value="config") String config) throws JsonParseException, JsonMappingException, IOException, InterruptedException, URISyntaxException {	
	 
	 
		 if (running.get() && (halt || reset || restart)) {
			 log("Stopping thread pool if necessary");
			pool.shutdown();
			pool.shutdownNow();
			boolean shutdown = pool.awaitTermination(30, TimeUnit.SECONDS);

			if (!shutdown) {
				throw new RuntimeException("Thread pool didn't stop after 30 seconds!");
			}

			running.set(false);
		}

		if (!running.get()) {
			if (reset) {
				 log("Resetting configuration to original");
				configuration.set(original);
			} else if (restart) {
				configuration.set(mapper.readValue(config, TestConfig.class));

				TestConfig iterationConfig = prepareForTest(configuration.get());
				Client client = iterationConfig.getClient();

				try {
					switch (client) {
					case couchbase:
						startCouchbaseTest(iterationConfig);
						break;
					  default:
						throw new UnsupportedOperationException("No idea of how to handle client type " + client);
					}
				} catch (Throwable e) {
					log("Exception starting test: " + e.getMessage());

					 log("Stopping thread pool if necessary");
					pool.shutdown();
					pool.shutdownNow();
					boolean shutdown = pool.awaitTermination(30, TimeUnit.SECONDS);

					if (!shutdown) {
						throw new RuntimeException("Thread pool didn't stop after 30 seconds!");
					}

					running.set(false);
				}
			}
		}
		return "redirect:"+request.getRequestURI().toString();

	}
	 

	TestConfig prepareForTest(TestConfig config) {
		running.set(true);

		Client client = config.getClient();

		log("Using " + client + " client and " + config.getConcurrent() + " threads");
		if (pool != null) {
			pool.shutdown();
		}
		pool = Executors.newFixedThreadPool(config.getConcurrent());

		iteration.incrementAndGet();
		count.set(0);
		fails.set(0);

		return config;
	}


	void startCouchbaseTest(final TestConfig config) {
		CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder().retryDelay(Delay.linear(TimeUnit.MILLISECONDS, config.getRetryDelayMillis())).kvTimeout(300000).networkLatencyMetricsCollectorConfig(DefaultLatencyMetricsCollectorConfig.builder().targetUnit(TimeUnit.MILLISECONDS).build()).networkLatencyMetricsCollectorConfig(DefaultLatencyMetricsCollectorConfig.create(1, TimeUnit.MINUTES)).build();
        
		final Cluster cluster = CouchbaseCluster.create(env, config.getServers());
		
		final Bucket sharedBucket = cluster.openBucket(config.getBucket(), config.getPassword());

		statusPrinted = new AtomicLong(System.currentTimeMillis());

		for (int thread = 1; thread <= config.getConcurrent(); thread++) {
			final int threadId = thread;

			Runnable r = new Runnable() {
				@Override
				public void run() {
					System.out.println("in side run");
					Thread.currentThread().setName("thread-" + threadId);

					Bucket bucket;

					if (config.isCouchbaseSharedThreadBucket()) {
						bucket = sharedBucket;
					} else {
						sharedBucket.close();
						bucket = cluster.openBucket(config.getBucket(), config.getPassword());
					}

					long counter = count.incrementAndGet();
					while (!pool.isShutdown() && counter <= config.getTotal()) {
						boolean failed = false;

						try {
							/* Begin couchbase client method */
							AtomicInteger tri = new AtomicInteger(0);

							JsonDocument status = null;
							String uuid = UUID.randomUUID().toString();

							String addString = "{\"recordTypeCode\":\"D\",\"transactionRefNumber\":\"" + "320133500" + counter + "\",\"transactionDate\":\"11/08/2013\",\"transactionAmount\":" + random.nextInt(999) + "." + random.nextInt(99)
									+ ",\"authorizationCode\":\"\",\"postDate\":\"12/16/2013\",\"transactionCode\":\"0181\",\"referenceSeNumber\":\"1092817261\",\"transPlasticNumber\":\"XX8349261405000\",\"refBatchNumber\":\"\",\"subCode\":\"\",\"billDescLine1Text\":\"\",\"billDescLine2Text\":\"\",\"billDescLine3Text\":\"\",\"refBillCCYCode\":\"\",\"seNumber\":\"\",\"rocInvoiceNumber\":\"\",\"seName\":\"\",\"productNumberCode\":\"ZC\",\"captureCenterRefNumber\":\"\",\"airLineTicketNumber\":\"\",\"airLineDeparDateDesc\":\"\",\"airLineDocTypeCode\":\"\",\"airportFromName\":\"\",\"accIdContextId\":\"TRIUMP\",\"billPostalCode\":\"850248688\",\"billRegionCode\":\"AZ\",\"billCountryCode\":\"US\",\"accLevelTypeCode\":\"B\",\"accStatusCode\":\"\",\"accAgeCode\":\"0\",\"productId\":\"YY\",\"accEffectiveDate\":\"10/04/1996\",\"prevProdIdentifierText\":\"YY\",\"billCycleCode\":\"B12\",\"transIa\":\"YY\",\"transConfigCode\":\"ZC\",\"rocFileId\":\""
									+ "000000000" + random.nextInt(2) + "\"}";

							JsonObject add = JsonObject.fromJson(addString);

							while (status == null && tri.incrementAndGet() < config.getMaxRetries()) {
								try {
									failed = false;
							
		
                                    long startTime = System.currentTimeMillis();
									status = bucket.upsert(JsonDocument.create(uuid, 1800, add));
                                     long elapsedTimeSet = System.currentTimeMillis() - startTime;
                                     
                                     if(elapsedTimeSet > 100) {
                                         logger.info(" spookreq SET Request took ### "+elapsedTimeSet+" ### ms , UUID: "+uuid);
                                     }
									

									if (status == null) {
										failed = true;
									log("Failed save! Thread: " + threadId + " Counter: " + counter + " Try: " + tri.get() + " uuid: " + uuid + " status: " + status);
									} else {
                                        long startTimeGet = System.currentTimeMillis();
										JsonDocument get = bucket.get(uuid);
                                        long elapsedTimeGet = System.currentTimeMillis() - startTimeGet;
                                        

                                        if(elapsedTimeGet > 100) {
                                         logger.info(" spookreq GET Request took ### "+elapsedTimeGet + " ### ms , UUID:" + uuid);
                                        }

										JsonObject content = get == null ? null : get.content();

										if (!Objects.equals(add, content)) {
											failed = true;
									log("Mismatched data! Thread: " + threadId + " Counter: " + counter + " Try: " + tri.get() + " uuid: " + uuid + " Add Value: " + add + " != Get Value: " + content);
										}
									}
								} catch (Throwable e) {
									String stackTrace = "";
/*									try (StringWriter sw = new StringWriter()) {
										e.printStackTrace(new PrintWriter(sw));
										stackTrace = sw.toString();
									} catch (IOException e2) {
										// bleh
									}*/

									failed = true;
									log("Exception! Thread: " + threadId + " Counter: " + counter + " Try: " + tri.get() + " uuid: " + uuid + " Message: " + e.getClass().getName() + " " + e.getMessage() + "\n" + stackTrace);
									try {
										Thread.sleep(config.getRetryWaitMs());
									} catch (InterruptedException e1) {
										// ignore
									}
								}
							}
							/* End couchbase client method */
						} finally {
							counter = count.incrementAndGet();
							if ((counter - 1) % config.getStatusEvery() == 0) {
								long now = System.currentTimeMillis();
								long elapsed = now - statusPrinted.getAndSet(now);
								log("Processed " + (counter - 1) + " total in " + elapsed + "ms for chunk of " + config.getStatusEvery() + ". Thread: " + threadId);
							}
							if (failed) {
								int maxFails = config.getMaxFails();
								if (fails.incrementAndGet() > maxFails) {
									log("Max Fails Exceeded " + fails + " > " + maxFails + ": Thread: " + threadId + " Counter: " + counter);
									pool.shutdown();
								}
							}
						}
					}
				}
			};
			pool.submit(r);
		}
	}
	

	 @RequestMapping(value = "/test/path" , method=RequestMethod.GET ,produces ="text/html")
	public String testGet() throws JsonProcessingException {
		 
		if (pool.isTerminated()) {
			running.set(false);
		}

		StringBuilder retVal = new StringBuilder();

		retVal.append("<h2>Current Status</h2>\n");

		retVal.append("Running?: ").append(running.get()).append("\n<br/>\n");

		retVal.append("Iteration: ").append(iteration).append("\n<br/>\n");

		retVal.append("Count: ").append(count.get()).append("\n<br/>\n");

		retVal.append("Fails: ").append(fails.get()).append("\n<br/>\n");

		if (running.get()) {
			retVal.append("<form method=\"post\">\n");

			retVal.append("<input type=\"hidden\" name=\"halt\" value=\"true\"/>\n");

			retVal.append("<input type=\"submit\" value=\"Halt\"/>\n");

			retVal.append("</form>\n");

			retVal.append("<form method=\"post\">\n");

			retVal.append("<input type=\"hidden\" name=\"reset\" value=\"true\"/>\n");

			retVal.append("<input name=\"reset\" type=\"submit\" value=\"Halt And Reset Configuration\"/>\n");

			retVal.append("</form>\n");
		}

		retVal.append("<h3>Configuration</h3>\n");

		retVal.append("<form method=\"post\">\n");

		retVal.append("<input type=\"hidden\" name=\"restart\" value=\"true\"/>\n");

		retVal.append("<textarea name=\"config\" rows=\"20\" cols=\"60\">").append(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(configuration)).append("</textarea>\n<br/>\n");

		if (running.get()) {
			retVal.append("<input type=\"submit\" value=\"Restart With This Configuration\"/>\n");
		} else {
			retVal.append("<input type=\"submit\" value=\"Start With This Configuration\"/>\n");
		}

		retVal.append("</form><hr/>\n");

		retVal.append("<h3 name=\"logs\">Last " + logs.getLimit() + " Log Entries</h3>\n");

		for (String entry : logs) {
			retVal.append(entry).append("<br/>\n");
		}

		retVal.append("<h3><a name=\"refresh\" href=\"?_=" + System.currentTimeMillis() + "#refresh\">Refresh</a></h3>\n");

		return retVal.toString();
	}

	void log(String entry) {
		logger.warn(entry);
		logs.add(new Date() + ":" + entry);
	}



	static class TestConfig {
		int concurrent = 4;

		int total = 100000000;

		int statusEvery = 10000;

		int maxRetries = 3;

		int maxFails = 10;

		long retryWaitMs = 50;

		boolean memcacheUsePool = false;

		Client client = Client.couchbase;

		String servers = "localhost";

		// int memcachePort = 11211;

		// int postgresPort = 5432;

		boolean couchbaseSharedThreadBucket = true;

		String bucket = "cons_aggregator";

	// 	String postgresDb = "test-db-1";

	// 	String postgresSchema = "test-schema-a";

	// 	String postgresTable = "test1";

	// 	String postgresTableId = "id";

	//	String postgresTableData = "data";

	//	String postgresUser = "";

		String password = "Amex1234";

		boolean couchbaseUsePersistToOne = false;

		boolean couchbaseUseReplicateToOne = false;
		
		long retryDelayMillis = 0L;
		
		public void setRetryDelayMillis(long delay) {
			this.retryDelayMillis = delay;
		}
		
		public long getRetryDelayMillis() {
			return retryDelayMillis;
		}



		public boolean isCouchbaseUsePersistToOne() {
			return couchbaseUsePersistToOne;
		}

		public void setCouchbaseUsePersistToOne(boolean couchbaseUsePersistToOne) {
			this.couchbaseUsePersistToOne = couchbaseUsePersistToOne;
		}

		public boolean isCouchbaseUseReplicateToOne() {
			return couchbaseUseReplicateToOne;
		}

		public void setCouchbaseUseReplicateToOne(boolean couchbaseUseReplicateToOne) {
			this.couchbaseUseReplicateToOne = couchbaseUseReplicateToOne;
		}

		public boolean isCouchbaseSharedThreadBucket() {
			return couchbaseSharedThreadBucket;
		}

		public void setCouchbaseSharedThreadBucket(boolean couchbaseOneBucketForAllThreads) {
			this.couchbaseSharedThreadBucket = couchbaseOneBucketForAllThreads;
		}



		public int getConcurrent() {
			return concurrent;
		}

		public void setConcurrent(int concurrent) {
			this.concurrent = concurrent;
		}

		public int getTotal() {
			return total;
		}

		public void setTotal(int total) {
			this.total = total;
		}

		public int getStatusEvery() {
			return statusEvery;
		}

		public void setStatusEvery(int statusEvery) {
			this.statusEvery = statusEvery;
		}

		public int getMaxRetries() {
			return maxRetries;
		}

		public void setMaxRetries(int maxRetries) {
			this.maxRetries = maxRetries;
		}

		public int getMaxFails() {
			return maxFails;
		}

		public void setMaxFails(int maxFails) {
			this.maxFails = maxFails;
		}

		public long getRetryWaitMs() {
			return retryWaitMs;
		}

		public void setRetryWaitMs(long retryWaitMs) {
			this.retryWaitMs = retryWaitMs;
		}

		public boolean isMemcacheUsePool() {
			return memcacheUsePool;
		}

		public void setMemcacheUsePool(boolean connectionPoolScale) {
			this.memcacheUsePool = connectionPoolScale;
		}

		public Client getClient() {
			return client;
		}

		public void setClient(Client client) {
			this.client = client;
		}

		public String getServers() {
			return servers;
		}

		public void setServers(String servers) {
			this.servers = servers;
		}


		public String getBucket() {
			return bucket;
		}

		public void setBucket(String bucket) {
			this.bucket = bucket;
		}

		public String getPassword() {
			return password;
		}

		public void setPassword(String password) {
			this.password = password;
		}
	}

	static class LimitedStringQueue extends LinkedList<String> {
		private static final long serialVersionUID = 1L;
		private int limit;
		private long total = 0;

		public LimitedStringQueue(int limit) {
			this.limit = limit;
		}

		@Override
		public synchronized boolean add(String o) {
			total++;
			super.add("(" + total + ") " + o);
			while (size() > limit) {
				super.remove();
			}
			return true;
		}

		public int getLimit() {
			return limit;
		}
	}
}
