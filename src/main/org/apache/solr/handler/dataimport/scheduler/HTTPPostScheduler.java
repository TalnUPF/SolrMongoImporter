package org.apache.solr.handler.dataimport.scheduler;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;
import org.apache.solr.core.SolrResourceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import solr.search.util.AppProperties;

/**
 * Credit to
 * https://wiki.apache.org/solr/DataImportHandler
 *
 */
public class HTTPPostScheduler extends TimerTask {
        private String syncEnabled;
        private String[] syncCores;
        private String server;
        private String port;
        private String webapp;
        private String params;
        private String interval;
        private String cores;
        private SolrDataImportProperties p;
        private boolean singleCore;

        private static final Logger logger = LoggerFactory.getLogger(HTTPPostScheduler.class);

        public HTTPPostScheduler(String webAppName, Timer t) throws Exception{
                //load properties from global dataimport.properties
                p = new SolrDataImportProperties();
                reloadParams();
                fixParams(webAppName);

                if(!syncEnabled.equals("1")) throw new Exception("Schedule disabled");

                if(syncCores == null || (syncCores.length == 1 && syncCores[0].isEmpty())){
                        singleCore = true;
                        logger.info("<delta> Single core identified in dataimport.properties");
                }else{
                        singleCore = false;
                        logger.info("<delta> Multiple cores identified in dataimport.properties. Sync active for: " + cores);
                        initializeDataimportFile();
                }
        }

        private void reloadParams(){
                p.loadProperties(true);
                syncEnabled = p.getProperty(SolrDataImportProperties.SYNC_ENABLED);
                cores           = p.getProperty(SolrDataImportProperties.SYNC_CORES);
                server          = p.getProperty(SolrDataImportProperties.SERVER);
                port            = p.getProperty(SolrDataImportProperties.PORT);
                webapp          = p.getProperty(SolrDataImportProperties.WEBAPP);
                params          = p.getProperty(SolrDataImportProperties.PARAMS);
                interval        = p.getProperty(SolrDataImportProperties.INTERVAL);
                syncCores       = cores != null ? cores.split(",") : null;
        }

        private void fixParams(String webAppName){
                if(server == null || server.isEmpty())  server = "localhost";
                if(port == null || port.isEmpty())              port = "8080";
                if(webapp == null || webapp.isEmpty())  webapp = webAppName;
                if(interval == null || interval.isEmpty() || getIntervalInt() <= 0) interval = "30";
        }

        public void run() {
                try{
                        // check mandatory params
                        if(server.isEmpty() || webapp.isEmpty() || params == null || params.isEmpty()){
                                logger.warn("<delta> Insuficient info provided for data import");
                                logger.info("<delta> Reloading global dataimport.properties");
                                reloadParams();

                        // single-core
                        }else if(singleCore){
                                prepUrlSendHttpPost();

                        // multi-core
                        }else if(syncCores.length == 0 || (syncCores.length == 1 && syncCores[0].isEmpty())){
                                logger.warn("<delta> No cores scheduled for data import");
                                logger.info("<delta> Reloading global dataimport.properties");
                                reloadParams();

                        }else{
                                for(String core : syncCores){
                                        prepUrlSendHttpPost(core);
                                        sleep();
                                }
                        }
                }catch(Exception e){
                        logger.error("Failed to prepare for sendHttpPost", e);
                        reloadParams();
                }
        }

        private void sleep() {
        	try {
				Thread.sleep(90000L);
			} catch (InterruptedException e) {
				//ignore
			}
        }

        private void prepUrlSendHttpPost(){
                String coreUrl = "http://" + server + ":" + port + "/" + webapp + params;
                sendHttpPost(coreUrl, null);
        }

        private void prepUrlSendHttpPost(String coreName){
                String coreUrl = "http://" + server + ":" + port + "/" + webapp + "/" + coreName + params;
                sendHttpPost(coreUrl, coreName);
        }


        private void sendHttpPost(String completeUrl, String coreName){
                DateFormat df = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss");
                Date startTime = new Date();

                // prepare the core var
                String core = coreName == null ? "" : "[" + coreName + "] ";
                
                logger.info(core + "<delta> Process started at .............. " + df.format(startTime));

                try{

                    URL url = new URL(completeUrl);
                    HttpURLConnection conn = (HttpURLConnection)url.openConnection();

                    conn.setRequestMethod("POST");
                    conn.setRequestProperty("type", "submit");
                    conn.setDoOutput(true);
//                    logger.info(core + "<delta> Request method\t" + conn.getRequestMethod());
//                    logger.info(core + "<delta> Using port\t" + port);
//                    logger.info(core + "<delta> Application name\t" + webapp);
//                    logger.info(core + "<delta> URL params\t" + params);
                    logger.info(core + "<delta> delta URL: " + conn.getURL());
                        // Send HTTP POST
                    conn.setConnectTimeout(6000);//timeout 5 seconds
                    conn.connect();

                    logger.info(core + "<delta> Succesfully connected to server\t" + server);
                    
                    logger.info(core + "<delta> Response message\t" + conn.getResponseMessage());
                    logger.info(core + "<delta> Response code\t" + conn.getResponseCode());

                    //listen for change in properties file if an error occurs
                    if(conn.getResponseCode() != 200){
                        reloadParams();
                    }

                    conn.disconnect();
                    logger.info(core + "<delta> Disconnected from server\t" + server);
                    Date endTime = new Date();
                    logger.info(core + "<delta> Process ended at ................ " + df.format(endTime));
                }catch(MalformedURLException mue){
                        logger.error("Failed to assemble URL for HTTP POST:" + completeUrl, mue);
                }catch(IOException ioe){
                        logger.error("Failed to connect to the specified URL while trying to send HTTP POST:"+completeUrl, ioe);
                }catch(Exception e){
                        logger.error("Failed to send HTTP POST:"+completeUrl, e);
                }
        }

        /**
         * Note: This is my customization work.
         * Initialize coreName/conf/dataimport.properties if it doesn't exist, 
         * The purpose is delta import is slower than full import, when it's processing millions of records, it will take forever.
         * Note: 
         * 1. coreName/conf/dataimport.properties won't get updated unless there's changes (modified/deleted) detected
         * 2. The recommendation is that after Solr is started up, run full import.
         */
        private void initializeDataimportFile() {
        	SolrResourceLoader loader = new SolrResourceLoader();
       		@SuppressWarnings("deprecation")
			String solrHome = loader.getInstanceDir();
        		
        		for(String coreName : cores.split(",")) {
	        		File dataimportProp = new File(solrHome+"/" + coreName+"/conf/dataimport.properties");
	        		if(!dataimportProp.exists()) {
	        			logger.warn(dataimportProp.getAbsolutePath() + " not existed, creating one");
	        			FileWriter fw = null;
	        			try {
							fw = new FileWriter(dataimportProp);
							//2012-05-21 20\:56\:40
							DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH\\:mm\\:ss");
			                String fakeLastIndexTime = df.format(new Date());
							fw.write(coreName + ".last_index_time=" + fakeLastIndexTime);
							fw.write(System.getProperty("line.separator"));
							fw.write("last_index_time=" + fakeLastIndexTime);
						} catch (IOException e) {
							logger.error("couldn't write to" + dataimportProp.getAbsolutePath());
						}finally{
							if(fw!=null) {
								try {
									fw.close();
								} catch (IOException e) {
									//ignore
								}
							}
						}
	        		}else {
	        			logger.warn(dataimportProp.getAbsolutePath() + " exists, [OK]");
	        		}
        		}
        }

		public int getIntervalInt() {
                try{
                        return Integer.parseInt(interval);
                }catch(NumberFormatException e){
                        logger.warn("Unable to convert 'interval' to number. Using default value (10) instead", e);
                        return 10; //return default in case of error
                }
        }
}