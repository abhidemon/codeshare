package com.unbxd.feed.processor.parser.json.V2;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.unbxd.feed.asynxecutor.TaskExecutor;
import com.unbxd.feed.asynxecutor.impl.BufferExecutor;
import com.unbxd.feed.constants.Constants;
import com.unbxd.feed.constants.ErrorMessage;
import com.unbxd.feed.constants.FeedConstants;
import com.unbxd.feed.constants.UnbxdActionType;
import com.unbxd.feed.data.model.in.FeedMetaData;
import com.unbxd.feed.data.model.out.DetailedFileUploadResponse;
import com.unbxd.feed.exceptions.ParsingException;
import com.unbxd.feed.exceptions.UnbxdFeedException;
import com.unbxd.feed.processor.FeedProcessor;
import com.unbxd.feed.processor.parser.json.JSONParser;
import com.unbxd.feed.processor.validator.FeedValidator;
import com.unbxd.feed.processor.validator.V2.FeaturedFieldSchemaV2;
import com.unbxd.feed.processor.validator.V2.UnbxdDataTypeV2;
import com.unbxd.feed.service.GlobalSchemaService;
import com.unbxd.feed.service.SchemaService;
import com.unbxd.feed.service.impl.SpringApplicationContextAware;
import com.unbxd.feed.asynxecutor.impl.MongoWriteBuffer;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.*;

public class JSONParserV2 extends JSONParser{
	


	private static final Logger LOGGER = Logger.getLogger(JSONParserV2.class);
	private Map<String, BasicDBObject> dumpSchemaMap = new HashMap<>();  
	private Long timeToValidateProducts = 0L;
	private Long timeToUploadProducts = 0L;
	private Long timeToParseProducts = 0L;
	private Long timeToMaintainSchema = 0L;
	
	public void init() {
		schemaService = (SchemaService)SpringApplicationContextAware.getBean("schemaService");	
		feedProcessor = (FeedProcessor)SpringApplicationContextAware.getBean("feedProcessor");
		globalSchemaService = (GlobalSchemaService)SpringApplicationContextAware.getBean("globalSchemaService");
	}
	
	@Override
	protected void clear() {
		super.clear();
		dumpSchemaMap = new HashMap<>();  
		timeToValidateProducts = 0L;
		timeToUploadProducts = 0L;
		timeToParseProducts = 0L;
		timeToMaintainSchema = 0L;			
	}	
	
    public void parseFeedTag(JsonParser parser, FeedMetaData data)
    throws Exception {
    	clear();
    	if(parser.nextToken() != JsonToken.START_OBJECT ) {
        	ParsingException ex = new ParsingException(105L, ErrorMessage.e105L);
        	ex.setFieldName(FeedConstants.FEED);
        	ex.setRowNum(String.valueOf(parser.getCurrentLocation().getLineNr()));
        	ex.setColNum(String.valueOf(parser.getCurrentLocation().getColumnNr()));
        	LOGGER.error("Row: " + ex.getRowNum() + "Col: " + ex.getColNum() + ErrorMessage.e105L);
        	LOGGER.error("fieldName: " + FeedConstants.FEED);
        	LOGGER.error("Feed tag Missing");
        	throw ex; 
    	}
    	
    	parser.nextToken();
    	if(parser.getCurrentName().equalsIgnoreCase(FeedConstants.FEED)) {
    		parseFeedName(parser, data);
    	} else {
        	ParsingException ex = new ParsingException(105L, ErrorMessage.e105L);
        	ex.setFieldName(FeedConstants.FEED);
        	ex.setRowNum(String.valueOf(parser.getCurrentLocation().getLineNr()));
        	ex.setColNum(String.valueOf(parser.getCurrentLocation().getColumnNr()));
        	LOGGER.error("Row: " + ex.getRowNum() + "Col: " + ex.getColNum() + ErrorMessage.e105L);
        	LOGGER.error("fieldName: " + FeedConstants.FEED);
        	LOGGER.error("Feed tag Missing");
        	throw ex;   
    	}
    	
    }
    
    public void parseFeedName(JsonParser parser, FeedMetaData data) 
    		throws Exception{
    	
    	if(parser.nextToken() != JsonToken.START_OBJECT ) {
        	ParsingException ex = new ParsingException(105L, ErrorMessage.e105L);
        	ex.setFieldName(FeedConstants.FEED);
        	ex.setRowNum(String.valueOf(parser.getCurrentLocation().getLineNr()));
        	ex.setColNum(String.valueOf(parser.getCurrentLocation().getColumnNr()));
        	LOGGER.error("Row: " + ex.getRowNum() + "Col: " + ex.getColNum() + ErrorMessage.e105L);
        	LOGGER.error("fieldName: " + FeedConstants.FEED);
        	LOGGER.error("Feed tag Missing");
        	throw ex;     		
    	}
    	
    	parser.nextToken();
    	
    	if(parser.getCurrentName().equals(FeedConstants.CATALOG)) {    	
    		parseCatalog(parser, data);
    		parser.nextToken();
        	LOGGER.debug(String.format("****PERFORMANCE**** of userId: %s siteName: %s fileName: %s", data.getUserId(), data.getFeedName(), data.getUnbxdFileName()));
        	LOGGER.debug("Total timeToValidate: " + timeToValidateProducts);  
        	LOGGER.debug("Total timeToUpload: " + timeToUploadProducts);  
        	LOGGER.debug("Total timeToParse: " + timeToParseProducts);
        	LOGGER.debug("Total timeToDigSchema: " + timeToMaintainSchema);
        	LOGGER.debug("*******************");    		
    	} if(parser.getCurrentName().equals(FeedConstants.TAXONOMY)) {		
    		
    		Long timeBeforeTaxonomyParsing = System.currentTimeMillis();
    		parseTaxonomy(parser, data);
	    	Long timeToParse = System.currentTimeMillis() - timeBeforeTaxonomyParsing;
	    	LOGGER.debug(String.format("Total time in parsing schema: %s of userId: %s siteName: %s fileName: %s", timeToParse, data.getUserId(), data.getFeedName(), data.getUnbxdFileName()));      		
    		parser.nextToken();
    	} 
    }
    
    public void parseCatalog(JsonParser parser, FeedMetaData data) 
    		throws Exception {
    	
    	if(parser.nextToken() != JsonToken.START_OBJECT ) {
        	ParsingException ex = new ParsingException(105L, ErrorMessage.e105L);
        	ex.setFieldName(FeedConstants.CATALOG);
        	ex.setRowNum(String.valueOf(parser.getCurrentLocation().getLineNr()));
        	ex.setColNum(String.valueOf(parser.getCurrentLocation().getColumnNr()));
        	LOGGER.error("Row: " + ex.getRowNum() + "Col: " + ex.getColNum() + ErrorMessage.e105L);
        	LOGGER.error("fieldName: " + FeedConstants.CATALOG);
        	LOGGER.error("Invalid tag.");
        	throw ex;     		
    	}

    	LOGGER.info("asdfgh");

    	parser.nextToken();
    	    	
    	Long timeBeforeSchemaParsing = System.currentTimeMillis();
    	if(parser.getCurrentName().equals(FeedConstants.SCHEMA)) {
			LOGGER.info("Schema found...");
			parseSchema(parser, data);
    		parser.nextToken();
    	}
    	Long timeToParseSchema = System.currentTimeMillis() - timeBeforeSchemaParsing;
    	timeToMaintainSchema += timeToParseSchema;
    	
    	
    	Long timeToGetSchemaDump = System.nanoTime();
    	List<BasicDBObject> dumpSchema = schemaService.getTempSchema(data);
    	
    	/*
    	 * get FieldName to Schema Map
    	 */    	
    	dumpSchemaMap = FeedProcessor.getFieldNameToSchemaMap(dumpSchema); 
    	//DEC1SPRINT 
    	//use global schema here
    	Map<String, BasicDBObject> savedSchema = globalSchemaService.getGlobalSchema(data);
    	savedSchema.putAll(dumpSchemaMap);    
       	Long timeToDigSchema = System.nanoTime() - timeToGetSchemaDump;
    	timeToMaintainSchema += timeToDigSchema;    	
    	
    	Long timeBeforeProductsParsing = System.currentTimeMillis();    	
    	while(UnbxdActionType.isUnbxdAction(parser.getCurrentName())) {	
    		parseActionTag(parser, data, savedSchema);
    	} 
    	updateFieldsForDisplay(data.getSiteId());
    	Long timeToParse = System.currentTimeMillis() - timeBeforeProductsParsing;
    	LOGGER.debug(String.format("Total time in ProductsParsing: %s for userId: %s siteName: %s fileName: %s", timeToParse, data.getUserId(), data.getFeedName(), data.getUnbxdFileName()));  	
    }
    
    public void parseSchema(JsonParser parser, FeedMetaData data) 
    		throws Exception {
		LOGGER.info("Parsing Schema ...");
    	parser.nextToken();
    	
    	if(parser.getCurrentName().equalsIgnoreCase(FeedConstants.SCHEMA)) {
			LOGGER.info("Found shcmea tag...");
			parseSchemaData(parser, data);
    	} else {
    		LOGGER.debug("No Schema");
    	}
    }
    
    public void parseSchemaData(JsonParser parser, FeedMetaData data) 
    		throws Exception {

		LOGGER.info("Inside parseSchemaData");
    	Set<String> reservedFieldNames = FeaturedFieldSchemaV2.getFieldNameToDefaultSchemaMap().keySet();

		LOGGER.info("Got resetved fieldnames: "+Arrays.asList(reservedFieldNames));


		if(parser.getCurrentToken() != JsonToken.START_ARRAY) {
            LOGGER.info("Inside Found startarray 1");
        	if(parser.nextToken() != JsonToken.START_ARRAY ) {
                LOGGER.info("Inside Found startarray 2");

                ParsingException ex = new ParsingException(105L, ErrorMessage.e105L);
            	ex.setFieldName(FeedConstants.SCHEMA);
            	ex.setRowNum(String.valueOf(parser.getCurrentLocation().getLineNr()));
            	ex.setColNum(String.valueOf(parser.getCurrentLocation().getColumnNr()));
            	LOGGER.error("Row: " + ex.getRowNum() + "Col: " + ex.getColNum() + ErrorMessage.e105L);
            	LOGGER.error("fieldName: " + FeedConstants.SCHEMA);
            	LOGGER.error("Invalid tag.");
            	throw ex;
        	}    		
    	}
        List<BasicDBObject> deltaSchemaUploaded = new ArrayList<>();
        LOGGER.info("Inside Found startarray 1");

        int i=6;

        if(parser.getCurrentToken() == JsonToken.START_ARRAY) {
            LOGGER.info("Inside Found startarray 3");
    		while(parser.getCurrentToken() != JsonToken.END_ARRAY) {
                LOGGER.info("Inside Found startarray 4");
    			while (parser.nextToken() != JsonToken.START_OBJECT);    			
    			BasicDBObject aSchemaObject = new BasicDBObject();
    			while(parser.getCurrentToken() == JsonToken.START_OBJECT) {
                    LOGGER.info("Inside Found startarray 5");
    				parseSchemaObject(parser, data, aSchemaObject);
    				
    				if(!reservedFieldNames.contains((String)aSchemaObject.get(FeedConstants.SCHEMA_FIELD_NAME))){
                        LOGGER.info("inside found array :6" );
                        deltaSchemaUploaded.add(new BasicDBObject(aSchemaObject));
                    }

                    LOGGER.info("inside found array : 7");

                    aSchemaObject.clear();
    				parser.nextToken();
    			}
    		}
            LOGGER.info("inside found array : 8");
			validateDeltaSchema(deltaSchemaUploaded,
					String.valueOf(parser.getCurrentLocation().getLineNr()),
					String.valueOf(parser.getCurrentLocation().getColumnNr()));
        	/*
        	 * Upload Schema to dump coll
        	 */
            LOGGER.info("Now uploading schema ..: ");
			feedProcessor.uploadSchema(deltaSchemaUploaded, data);
    	} else {
        	ParsingException ex = new ParsingException(105L, ErrorMessage.e105L);
        	ex.setFieldName(FeedConstants.SCHEMA);
        	ex.setRowNum(String.valueOf(parser.getCurrentLocation().getLineNr()));
        	ex.setColNum(String.valueOf(parser.getCurrentLocation().getColumnNr()));
        	LOGGER.error("Row: " + ex.getRowNum() + "Col: " + ex.getColNum() + ErrorMessage.e105L);
        	LOGGER.error("fieldName: " + FeedConstants.SCHEMA);
        	LOGGER.error("Schema tag is an array");
        	throw ex;    		    		
    	}
    } 
    
    public void parseSchemaObject(JsonParser parser, FeedMetaData data, BasicDBObject aSchemaObject) 
    throws Exception{
    	
		Map<String, Object> localObj = null;
    	try {
    		localObj = (Map<String, Object>)parser.readValueAs(Map.class);
		} catch(Exception ex) {
    		throw new JsonParseException("Invalid JSON in schema.", parser.getCurrentLocation());
    	}   	

    	if(null == localObj || localObj.isEmpty()) {
    		throw new JsonParseException("Invalid JSON in schema.", parser.getCurrentLocation());
    	}
    	
    	for(Map.Entry<String, Object> anEntry : localObj.entrySet()) {
    		String entry = anEntry.getKey();
    		if(entry.equalsIgnoreCase(FeedConstants.SCHEMA_MULTI_VALUE)) {
    			entry = FeedConstants.SCHEMA_MULTI_VALUED;
    		}
    		//--handle boolean values
			Object received=anEntry.getValue();
			Object modified=anEntry.getValue();
    		try{
				if (received instanceof Boolean){
					if((Boolean)received){
						modified="true";
					}else{
						modified="false";
					}
				}
			}catch(Exception e){
    			e.printStackTrace();
			}
			//end
    		aSchemaObject.put(entry, modified);
    	}
    	try {
    		validateSchemaObject(aSchemaObject, parser);
    	}catch(ParsingException ex) {
        	throw ex; 		
    	}
    }
    
    
	private void validateSchemaObject(BasicDBObject aSchemaObject,
			JsonParser parser) throws ParsingException {
		String fieldType = aSchemaObject.getString(FeedConstants.DATA_TYPE);
		String multiVal = aSchemaObject.getString(FeedConstants.SCHEMA_MULTI_VALUED);
		String autoSugg = aSchemaObject.getString(FeedConstants.SCHEMA_AUTO_SUGGEST);
		
		if(!UnbxdDataTypeV2.descToDataTypeMap.containsKey(fieldType)) {
	    	LOGGER.error("Invalid field type"); 
	    	ParsingException pex = new ParsingException(107L, ErrorMessage.e107L);
	    	pex.setFieldName(FeedConstants.SCHEMA);
	    	pex.setRowNum(String.valueOf(parser.getCurrentLocation().getLineNr()));
	    	pex.setColNum(String.valueOf(parser.getCurrentLocation().getColumnNr()));
	    	throw pex;
		} 
		
		if(!UnbxdDataTypeV2.boolTypeSet.contains(multiVal)) {
	    	LOGGER.error("Invalid multiVal."); 
	    	ParsingException pex = new ParsingException(108L, ErrorMessage.e108L);
	    	pex.setFieldName(FeedConstants.SCHEMA);
	    	pex.setRowNum(String.valueOf(parser.getCurrentLocation().getLineNr()));
	    	pex.setColNum(String.valueOf(parser.getCurrentLocation().getColumnNr()));
	    	throw pex;
		} 
		
		if(!UnbxdDataTypeV2.boolTypeSet.contains(autoSugg)) {
	    	LOGGER.error("Invalid autoSuggest."); 
	    	ParsingException pex = new ParsingException(106L, ErrorMessage.e106L);
	    	pex.setFieldName(FeedConstants.SCHEMA);
	    	pex.setRowNum(String.valueOf(parser.getCurrentLocation().getLineNr()));
	    	pex.setColNum(String.valueOf(parser.getCurrentLocation().getColumnNr()));
	    	throw pex;
		} 

		
	}

	public void parseProducts(JsonParser parser, FeedMetaData data, UnbxdActionType action, Map<String, BasicDBObject> savedSchema) 
    		throws Exception {
    	
    	if(parser.nextToken() != JsonToken.START_OBJECT ) {
        	ParsingException ex = new ParsingException(105L, ErrorMessage.e105L);
        	ex.setFieldName(FeedConstants.ITEMS);
        	ex.setRowNum(String.valueOf(parser.getCurrentLocation().getLineNr()));
        	ex.setColNum(String.valueOf(parser.getCurrentLocation().getColumnNr()));
        	LOGGER.error("Row: " + ex.getRowNum() + "Col: " + ex.getColNum() + ErrorMessage.e105L);
        	LOGGER.error("fieldName: " + FeedConstants.ITEMS);
        	LOGGER.error("Invalid tag.");
        	throw ex;
    	}
    	
    	parser.nextToken();
    	
    	if(parser.nextToken() != JsonToken.START_ARRAY ) {
        	ParsingException ex = new ParsingException(105L, ErrorMessage.e105L);
        	ex.setFieldName(FeedConstants.ITEMS);
        	ex.setRowNum(String.valueOf(parser.getCurrentLocation().getLineNr()));
        	ex.setColNum(String.valueOf(parser.getCurrentLocation().getColumnNr()));
        	LOGGER.error("Row: " + ex.getRowNum() + "Col: " + ex.getColNum() + ErrorMessage.e105L);
        	LOGGER.error("fieldName: " + FeedConstants.ITEMS);
        	LOGGER.error("Invalid tag.");
        	throw ex;
    	} 
    	
    	parser.nextToken();
		long timeBeforeDumpingProducts = System.nanoTime();
		long totalTimeSpentInParseNValidation = 0;
		int totalNumberOfProductProcessed = 0;
		TaskExecutor bufferExecutor =  new BufferExecutor( data.getFile());
		LOGGER.debug("For siteId "+data.getSiteId()+" , Traversing the json parser.");
		while(parser.getCurrentToken() != JsonToken.END_ARRAY) {
			long timeBeforeParsingProduct = System.nanoTime();
    		BasicDBObject aProduct = new BasicDBObject();
    		parseProduct(parser, data, action, aProduct, savedSchema);
    		data.setTask(UnbxdActionType.getTask(action));
			totalTimeSpentInParseNValidation += System.nanoTime() - timeBeforeParsingProduct;

    		/*
    		 * Upload Product
    		 */
			feedProcessor.uploadProduct(bufferExecutor, new BasicDBObject(aProduct), data,
            		action);
			totalNumberOfProductProcessed++;
    		
    		aProduct.clear();
    		parser.nextToken();
    	}
		bufferExecutor.close();

		long totalTimeProcessing = System.nanoTime() - timeBeforeDumpingProducts;
		LOGGER.info("NoOfProducts: " + totalNumberOfProductProcessed + ",  site: " + data.getFeedName() +
				", user: " + data.getUserId() + ", feedFile: " + data.getUnbxdFileName()
				+ ", timeForParse: " + totalTimeSpentInParseNValidation + ", totalTime: " + totalTimeProcessing
				+ ", action: " + action.toString());

    	parser.nextToken();
    }
    
    public void parseActionTag(JsonParser parser, FeedMetaData data, Map<String, BasicDBObject> allSchema) 
    throws Exception {
    	while(UnbxdActionType.isUnbxdAction(parser.getCurrentName())) {
	    	UnbxdActionType action = UnbxdActionType.getActionType(parser.getCurrentName());
	    	parseProducts(parser, data, action, allSchema);
	    	parser.nextToken();
    	}
    }

	/**
	 * Method to process the product object in case of json format data, Mainly following cases are handled
	 * 1. In case the value of the field is null, discarding the value
	 * 2. In case the value of the field is non multivalued, where schema is multivalued, manipulating the data
	 * @param product
	 * @param allSchema
	 * @return
	 */
	public BasicDBObject processProduct(BasicDBObject product, Map<String, BasicDBObject> allSchema) {
		BasicDBObject processedProduct = new BasicDBObject();
		String fieldName;
		for(Map.Entry<String, Object> fieldValue:product.entrySet()) {
			fieldName = fieldValue.getKey();
			if(product.get(fieldName) == null) {
				//case when value of the field is null
				continue;
			}
			if(fieldName.equals(FeedConstants.ASSOC_PRODUCTS) && product.get(fieldName) instanceof List<?>) {
				BasicDBList childProducts = new BasicDBList();
				//case when value of the field is Object
				for(Object childProduct: (List<?>)product.get(fieldName)) {
					if(!(childProduct instanceof BasicDBObject)) {
						childProducts.add(childProduct);
						continue;
					}
					childProducts.add(processProduct((BasicDBObject)childProduct, allSchema));
				}
				processedProduct.put(fieldName, childProducts);
			} else if(allSchema.containsKey(fieldName) &&
					FeedValidator.getMultiValueData(allSchema.get(fieldName)).equalsIgnoreCase(FeedConstants.TRUE)) {
				//case when schema for the field is set to multiValued
				if(!(product.get(fieldName) instanceof List<?>)) {
					// in case of the value is not a List
					List<Object> value = new ArrayList<Object>();
					value.add(product.get(fieldName));
					processedProduct.put(fieldName, value);
				} else {
					processedProduct.put(fieldName, product.get(fieldName));
				}
			} else {
				processedProduct.put(fieldName, product.get(fieldName));
			}
		}
		return processedProduct;
	}
    
    public void parseProduct(JsonParser parser, FeedMetaData data, UnbxdActionType action, 
    		BasicDBObject aProduct, Map<String, BasicDBObject> allSchema) 
    		throws Exception {
    	
		Map<String, Object> localObj = null;
    	try {
    		localObj = (Map<String, Object>)parser.readValueAs(Map.class);
    	} catch(Exception ex) {
    		throw new JsonParseException("Invalid JSON in items key.", parser.getCurrentLocation());
    	}   	

    	if(null == localObj || localObj.isEmpty()) {
    		throw new JsonParseException("Invalid JSON in items key.", parser.getCurrentLocation());
    	}

		BasicDBObject dbo = processProduct(FeedProcessor.createBasicDBObjectFromMap(localObj), allSchema);
//		validateFieldNames(dbo,
//				String.valueOf(parser.getCurrentLocation().getLineNr()),
//				String.valueOf(parser.getCurrentLocation().getColumnNr()));		
		
    	if(null != dbo.get(FeedConstants.ASSOC_PRODUCTS)) {
//    		if(!allSchema.containsKey(FeedConstants.SKU_FIELD)) {
//    			allSchema.put(FeedConstants.SKU_FIELD, getDefaultSchemaForSingleValLinkField(FeedConstants.SKU_FIELD));
//        		updateAllProducts(data);
//    		}
    		@SuppressWarnings("unchecked")
			List<LinkedHashMap<String, Object>> linkedMapList = (List<LinkedHashMap<String, Object>>)dbo.get(FeedConstants.ASSOC_PRODUCTS);
    		List<BasicDBObject> dboList = new ArrayList<>();
    		for (LinkedHashMap<String, Object> linkedMap : linkedMapList) {
    			BasicDBObject tempDbo = new BasicDBObject();
				for(Map.Entry<String, Object> aEntry : linkedMap.entrySet()) {
					tempDbo.put(aEntry.getKey(), aEntry.getValue());
				}
				dboList.add(tempDbo);
			}    		
    		dbo.put(FeedConstants.ASSOC_PRODUCTS, dboList);
    		dbo = getFieldValuePairForVersion1(dbo, data, String.valueOf(parser.getCurrentLocation().getLineNr()),
					String.valueOf(parser.getCurrentLocation().getColumnNr()));
//    		prepareAssociatedProducts(dbo);
    	} else {    		
//    		dbo.put(FeedConstants.TYPE_PARENT, FeedConstants.TYPE_CHILD_VAL);
//    		if(allSchema.containsKey(FeedConstants.SKU_FIELD)) {
//    			dbo.put(FeedConstants.SKU_FIELD, dbo.get(FeedConstants.UNIQUE_ID));
//    		}    		
    	}		
		
		aProduct.clear();
    	
    	for(Map.Entry<String, Object> anEntry : dbo.entrySet()) {
    		aProduct.put(anEntry.getKey(), anEntry.getValue());
    		if(!anEntry.getKey().equalsIgnoreCase(FeedConstants.ASSOC_PRODUCTS)) {
    			fieldsForDisplay.add(anEntry.getKey());
    		}
			if (!anEntry.getKey().equalsIgnoreCase(FeedConstants.TYPE_PARENT)
					&& !anEntry.getKey().equalsIgnoreCase(
							FeedConstants.ASSOC_PRODUCTS)
					&& !allSchema.containsKey(anEntry.getKey())) {
    			unknownSchemaFields.add(anEntry.getKey());    	
    		}
    	}     	
    	
    	Long timeBeforeSchemaDig = System.nanoTime();
		Long time = System.currentTimeMillis();
    	Map<String, BasicDBObject> newSchema = new HashMap<String, BasicDBObject>();
    	for(Map.Entry<String, BasicDBObject> anEntry : allSchema.entrySet()) {    		
    		if(!dumpSchemaMap.containsKey(anEntry.getKey())) {  
    			BasicDBObject dbo1 = anEntry.getValue();
    			dbo1.put(Constants.CREATED_TIMESTAMP, time);
    			dbo1.put(Constants.LAST_MODIFIED_TIMESTAMP, time);    			
    			newSchema.put(anEntry.getKey(), dbo1);
    			dumpSchemaMap.put(anEntry.getKey(), dbo1);
    		}
    	}
    	
    	/*
    	 * Only commit if there are changes
    	 */
    	if(!newSchema.isEmpty()) {
    		List<BasicDBObject> dumpFinalSchema = new ArrayList<>();
    		for(Map.Entry<String, BasicDBObject> anEntry : newSchema.entrySet()) {
    			dumpFinalSchema.add(anEntry.getValue());
    		}
    		schemaService.saveSchemaUpdatesDump(dumpFinalSchema, data);
    	}
    	
    	Long timeToSynSchema = System.nanoTime() - timeBeforeSchemaDig;
    	
    	timeToMaintainSchema += timeToSynSchema;
    	
    	try {
        	Long timeBeforeProductsValidation = System.nanoTime();
        	String rNum = String.valueOf(parser.getCurrentLocation().getLineNr());
        	String cNum =String.valueOf(parser.getCurrentLocation().getColumnNr());	        	
    		FeedValidator.validateProduct(aProduct, data, allSchema, rNum, cNum, fieldErrors);
	    	Long timeToValidate = System.nanoTime() - timeBeforeProductsValidation;
	    	timeToValidateProducts += timeToValidate;
	    	
    	} catch(ParsingException ex) {
    		LOGGER.error(ex.getMessage());   	
    		throw ex;    	
    	} catch(Exception ex) {
    		LOGGER.error(ex.getMessage());   		
    		throw new Exception(ErrorMessage.e309L);
    	}  	
    	
    }

	public void parseTaxonomy(JsonParser parser, FeedMetaData data) 
    		throws Exception {
    	
    	if(parser.nextToken() != JsonToken.START_OBJECT) {
        	ParsingException ex = new ParsingException(105L, ErrorMessage.e105L);
        	ex.setFieldName(FeedConstants.TAXONOMY);
        	ex.setRowNum(String.valueOf(parser.getCurrentLocation().getLineNr()));
        	ex.setColNum(String.valueOf(parser.getCurrentLocation().getColumnNr()));
        	LOGGER.error("Row: " + ex.getRowNum() + "Col: " + ex.getColNum() + ErrorMessage.e105L);
        	LOGGER.error("fieldName: " + FeedConstants.CATALOG);
        	LOGGER.error("Invalid tag."); 
        	throw ex;
    	} 
    	
    	parser.nextToken();
    	
    	if(parser.getCurrentName().equals(FeedConstants.TREE)) {
    		parseTrees(parser, data);
    		parser.nextToken();
    	} if(parser.getCurrentName().equals(FeedConstants.MAPPING)) {
			if(data.getSite().getIsTaxonomyMappingEnabled() != null && !data.getSite().getIsTaxonomyMappingEnabled()) {
				LOGGER.error("mapping feature is not enabled for the customer " +
						data.getSite().getSiteNameInternal());
				ParsingException ex = new ParsingException(126L, ErrorMessage.e126L);
				ex.setFieldName(FeedConstants.MAPPING);
				ex.setRowNum(String.valueOf(parser.getCurrentLocation().getLineNr()));
				ex.setColNum(String.valueOf(parser.getCurrentLocation().getColumnNr()));
				throw ex;
			} else {
				parseMapping(parser, data);
				parser.nextToken();
			}
    	}   	
    	
    }
    
    public void parseMapping(JsonParser parser, FeedMetaData data) 
    		throws Exception {

    	if(parser.nextToken() != JsonToken.START_ARRAY) {
        	ParsingException ex = new ParsingException(105L, ErrorMessage.e105L);
        	ex.setFieldName(FeedConstants.MAPPING);
        	ex.setRowNum(String.valueOf(parser.getCurrentLocation().getLineNr()));
        	ex.setColNum(String.valueOf(parser.getCurrentLocation().getColumnNr()));
        	LOGGER.error("Row: " + ex.getRowNum() + "Col: " + ex.getColNum() + ErrorMessage.e105L);
        	LOGGER.error("fieldName: " + FeedConstants.MAPPING);
        	LOGGER.error("Invalid tag."); 
        	throw ex;    		
    	} 
    	
    	if(parser.nextToken() != JsonToken.START_OBJECT) {
        	ParsingException ex = new ParsingException(105L, ErrorMessage.e105L);
        	ex.setFieldName(FeedConstants.MAPPING);
        	ex.setRowNum(String.valueOf(parser.getCurrentLocation().getLineNr()));
        	ex.setColNum(String.valueOf(parser.getCurrentLocation().getColumnNr()));
        	LOGGER.error("Row: " + ex.getRowNum() + "Col: " + ex.getColNum() + ErrorMessage.e105L);
        	LOGGER.error("fieldName: " + FeedConstants.MAPPING);
        	LOGGER.error("Invalid tag."); 
        	throw ex;      		
    	}  
    	
		List<BasicDBObject> mapNodeList = new ArrayList<BasicDBObject>();
		BasicDBObject aMap = new BasicDBObject();	
		
    	while(parser.getCurrentToken() != JsonToken.END_ARRAY) {
    		parseProductToTaxonomyNodeMapping(parser, data, aMap);
    		mapNodeList.add(new BasicDBObject(aMap));
    		aMap.clear();
    		parser.nextToken();
    	} 
    	uploadMapping(mapNodeList, data);
    } 
    
    public void parseProductToTaxonomyNodeMapping(JsonParser parser, FeedMetaData data, BasicDBObject aMap) 
    		throws Exception {    	
		Map<String, Object> localObj = null;
    	try {
    		localObj = (Map<String, Object>)parser.readValueAs(Map.class);
    	} catch(Exception ex) {
    		throw new JsonParseException("Invalid JSON in mapping key.", parser.getCurrentLocation());
    	}   	

    	if(null == localObj || localObj.isEmpty()) {
    		throw new JsonParseException("Invalid JSON in mapping key.", parser.getCurrentLocation());
    	}
    	for(Map.Entry<String, Object> anEntry : localObj.entrySet()) {
    		aMap.put(anEntry.getKey(), anEntry.getValue());
    	} 
    }    
    
    public void parseTrees(JsonParser parser, FeedMetaData data) 
    		throws Exception {
    	
    	if(parser.nextToken() != JsonToken.START_ARRAY ) {
        	ParsingException ex = new ParsingException(105L, ErrorMessage.e105L);
        	ex.setFieldName(FeedConstants.TREE);
        	ex.setRowNum(String.valueOf(parser.getCurrentLocation().getLineNr()));
        	ex.setColNum(String.valueOf(parser.getCurrentLocation().getColumnNr()));
        	LOGGER.error("Row: " + ex.getRowNum() + "Col: " + ex.getColNum() + ErrorMessage.e105L);
        	LOGGER.error("fieldName: " + FeedConstants.TREE);
        	LOGGER.error("Invalid tag."); 
        	throw ex;      		    	
    	} 
    	
    	if(parser.nextToken() != JsonToken.START_OBJECT ) {
        	ParsingException ex = new ParsingException(105L, ErrorMessage.e105L);
        	ex.setFieldName(FeedConstants.TREE);
        	ex.setRowNum(String.valueOf(parser.getCurrentLocation().getLineNr()));
        	ex.setColNum(String.valueOf(parser.getCurrentLocation().getColumnNr()));
        	LOGGER.error("Row: " + ex.getRowNum() + "Col: " + ex.getColNum() + ErrorMessage.e105L);
        	LOGGER.error("fieldName: " + FeedConstants.TREE);
        	LOGGER.error("Invalid tag."); 
        	throw ex;
    	}
    	
		List<BasicDBObject> treeList = new ArrayList<>();
		BasicDBObject aTree = new BasicDBObject();		
		
    	while(parser.getCurrentToken() != JsonToken.END_ARRAY) {
    		parseTree(parser, data, aTree);
    		treeList.add(new BasicDBObject(aTree));
    		aTree.clear();
    		parser.nextToken();
    	} 
    	
        /*
         * does an upsert
         */
        feedProcessor.uploadTaxonomyData(treeList, data);    	
    } 
    
    public void parseTree(JsonParser parser, FeedMetaData data, BasicDBObject aTree) 
    		throws Exception {
		Map<String, Object> localObj = null;
    	try {
    		localObj = (Map<String, Object>)parser.readValueAs(Map.class);
    	} catch(Exception ex) {
    		throw new JsonParseException("Invalid JSON in taxonomy tree key.", parser.getCurrentLocation());
    	}   	

    	if(null == localObj || localObj.isEmpty()) {
    		throw new JsonParseException("Invalid JSON in taxonomy tree key.", parser.getCurrentLocation());
    	}
    	for(Map.Entry<String, Object> anEntry : localObj.entrySet()) {
    		aTree.put(anEntry.getKey(), anEntry.getValue());
    	}         	
    }
    
	public static void main(String[] args) {
		String jsonFile = "/Users/unbxd11/Documents/test.json";
//		try {
			
//			JsonParser jp = jsonInputFactory.createJsonParser(new File(jsonFile));	
//			FeedMetaData data = new FeedMetaData();
//	        Mongo m = MongoConnector.getMongoInstance();
//	        DB db = m.getDB("jsonTest");
//	        DBCollection coll = db.getCollection("jsonParse");			
//			data.setCollection(coll);
//			new JSONParserV2().parseFeedTag(jp, data);
//		} catch (Exception e1) {
//			
//		}
		
	}

	@Override
	protected DetailedFileUploadResponse processData(String feedFile, FeedMetaData feedMetaData) {
		DetailedFileUploadResponse resp = new DetailedFileUploadResponse();           
		JsonParser jp = null;
        try{
        	jp = jsonInputFactory.createJsonParser(new File(feedFile));	                       
            parseFeedTag(jp, feedMetaData);
        }catch(ParsingException ex){            
            LOGGER.error(ex.getMessage(), ex);
            resp.setRowNum(ex.getRowNum());
            resp.setColNum(ex.getColNum());
        	resp.setMessage(ex.getMessage());
        	resp.setStatusCode(ex.getErrorCode());
        } catch(JsonParseException ex){
        	LOGGER.error(ex.getMessage());
        	resp.setRowNum(String.valueOf(jp.getCurrentLocation().getLineNr()));
        	resp.setColNum(String.valueOf(jp.getCurrentLocation().getColumnNr()));        	
        	resp.setMessage("Invalid JSON.");
        	resp.setStatusCode(512L);                    
        } catch(UnbxdFeedException ex) {
        	if(ex.getErrors() != null && ex.getErrors().size() > 0) {
        		resp.setStatusCode(ex.getErrors().get(0).getCode().getErrorCode());
        		resp.setMessage(ex.getErrors().get(0).getMessage());
        	} else {
        		LOGGER.fatal(String.format("The errors size can never be zero. Feedname: %s UserId: %s Filename: %s", feedMetaData.getFeedName(), feedMetaData.getUserId(), feedMetaData.getUnbxdFileName()));
            	resp.setMessage(ErrorMessage.e309L);
            	resp.setStatusCode(309L); 
        	}
        } catch(Exception ex){
			ex.printStackTrace();
        	LOGGER.error(ex.getMessage());
        	resp.setMessage(ErrorMessage.e309L);
        	resp.setStatusCode(309L);                    
        }finally{            
            try {
            	jp.close();
            } catch (Exception ex) {
            	LOGGER.error(ex.getMessage());
            	resp.setMessage(ErrorMessage.e309L);
            	resp.setStatusCode(309L);

            }
        }        
    	resp.setUnknownSchemaFields(unknownSchemaFields);       
    	resp.setFieldErrors(fieldErrors); 
    	if(feedMetaData.getIntegrationStep()) {
    		resp.setIntegrationStep(true);
    	}    	
    	return resp; 
	}	

}
