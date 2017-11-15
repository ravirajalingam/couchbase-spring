package test;

import java.util.UUID;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.json.JsonObject;

import test.service.TestServ;

@RestController
@RequestMapping("/api/add")
public class CbTest {
	
	final static Logger logger  = Logger.getLogger("CbTest.class");
	
	  private final Bucket bucket;
	  

	    @Autowired
	    public CbTest(Bucket bucket) {
	        this.bucket = bucket;
	    }


	    
	    /**
	     * Create a rec , this method will generate required dat here and pushed to the couchbase.
	     * @return 
	     */
	    @RequestMapping(value="/insertandreads", method=RequestMethod.POST,produces="text/html")
	    public Object createRec() {
	    	
	    //	logger.info("Method generats");
	    	
			String addString = "{\"recordTypeCode\":\"D\",\"transactionRefNumber\":\"" + "320133500" + "\",\"transactionDate\":\"11/08/2013\",\"transactionAmount\":" + 100 + "." + 00
			+ ",\"authorizationCode\":\"\",\"postDate\":\"12/16/2013\",\"transactionCode\":\"0181\",\"referenceSeNumber\":\"1092817261\",\"transPlasticNumber\":\"XX8349261405000\",\"refBatchNumber\":\"\",\"subCode\":\"\",\"billDescLine1Text\":\"\",\"billDescLine2Text\":\"\",\"billDescLine3Text\":\"\",\"refBillCCYCode\":\"\",\"seNumber\":\"\",\"rocInvoiceNumber\":\"\",\"seName\":\"\",\"productNumberCode\":\"ZC\",\"captureCenterRefNumber\":\"\",\"airLineTicketNumber\":\"\",\"airLineDeparDateDesc\":\"\",\"airLineDocTypeCode\":\"\",\"airportFromName\":\"\",\"accIdContextId\":\"TRIUMP\",\"billPostalCode\":\"850248688\",\"billRegionCode\":\"AZ\",\"billCountryCode\":\"US\",\"accLevelTypeCode\":\"B\",\"accStatusCode\":\"\",\"accAgeCode\":\"0\",\"productId\":\"YY\",\"accEffectiveDate\":\"10/04/1996\",\"prevProdIdentifierText\":\"YY\",\"billCycleCode\":\"B12\",\"transIa\":\"YY\",\"transConfigCode\":\"ZC\",\"rocFileId\":\""
			+ "000000000" + 1 + "\"}";
	        
			JsonObject add = JsonObject.fromJson(addString);
			
			String uuid = UUID.randomUUID().toString();
	        
	        return TestServ.createAndRead(bucket, add ,uuid);
	    }
	    
	    
	    @RequestMapping(value = "/health" , method=RequestMethod.GET ,produces ="text/html")
	    public String health() {
	    return "SUCCESS";
	    }


}
