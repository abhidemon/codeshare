	package com.unbxd.feed.controller;


	import com.fasterxml.jackson.databind.ObjectMapper;
	import com.unbxd.feed.asynxecutor.TaskExecutorFactory;
	import com.unbxd.feed.constants.*;
	import com.unbxd.feed.dao.AutocompleteCronDao;
	import com.unbxd.feed.data.model.in.*;
	import com.unbxd.feed.data.model.out.*;
	import com.unbxd.feed.exceptions.UnbxdFeedException;
	import com.unbxd.feed.exchange.NetworkCalls;
	import com.unbxd.feed.facet.model.in.Facet;
	import com.unbxd.feed.lock.ProcessingLockMonitor;
	import com.unbxd.feed.model.IndexField;
	import com.unbxd.feed.model.Subscriber;
	import com.unbxd.feed.model.Subscription;
	import com.unbxd.feed.processor.FeedProcessor;
	import com.unbxd.feed.processor.SolrCloudService;
	import com.unbxd.feed.processor.ringmaster.RingMasterService;
	import com.unbxd.feed.processor.ringmaster.model.NodeInfo;
	import com.unbxd.feed.processor.ringmaster.model.NodeInfoResponse;
	import com.unbxd.feed.processor.validator.V2.FeaturedFieldSchemaV2;
	import com.unbxd.feed.response.*;
	import com.unbxd.feed.service.*;
	import com.unbxd.feed.service.impl.AnalyticsCronServiceImpl;
	import com.unbxd.feed.site.model.in.SiteObject;
	import com.unbxd.feed.user.model.in.UserInfo;
	import com.unbxd.feed.utils.Utilities;
	import com.unbxd.rc.feed.dispatch.model.CommonResponse;
	import com.unbxd.rc.feed.dispatch.service.SiteRequestService;
	import com.unbxd.rc.feed.dispatch.service.SubscriberService;
	import com.unbxd.rc.feed.dispatch.service.impl.PushToSolrService;
	import com.unbxd.rc.feed.dispatch.service.impl.RulesetHelperService;
	import com.unbxd.rc.feed.dispatch.service.impl.SubscribersDataDispatchServiceImpl;
	import com.unbxd.rc.feed.dispatch.site.dao.SiteRequestDAO;
	import org.apache.log4j.Logger;
	import org.springframework.core.io.ClassPathResource;
	import org.springframework.core.io.support.PropertiesLoaderUtils;
	import org.springframework.data.repository.query.Param;
	import org.springframework.stereotype.Controller;
	import org.springframework.util.StringUtils;
	import org.springframework.web.bind.annotation.PathVariable;
	import org.springframework.web.bind.annotation.RequestMapping;
	import org.springframework.web.bind.annotation.RequestMethod;
	import org.springframework.web.bind.annotation.ResponseBody;

	import javax.annotation.Resource;
	import javax.servlet.ServletException;
	import javax.servlet.http.HttpServletRequest;
	import javax.servlet.http.HttpServletResponse;
	import java.io.IOException;
	import java.util.*;

	@Controller
	@RequestMapping("/api")
	public class FeedController {

		private static final Logger LOGGER =
				Logger.getLogger(FeedController.class);

		private static final String DEPLOY_CODE = "deployCode";

		@Resource(name="subscriberService")
		private SubscriberService subscriberService;
		@Resource(name="userInfoService")
		private UserInfoService userInfoService;
		@Resource(name="rulesetHelperService")
		private RulesetHelperService rulesetHelperService;
		@Resource(name="feedService")
		private FeedService feedService;
		@Resource(name="siteRequestService")
		private SiteRequestService siteReqService;
		@Resource(name="objectMapperService")
		private ObjectMapperService objMapperService;
		@Resource(name="schemaService")
		private SchemaService schemaService;
		@Resource(name="globalSchemaService")
		private GlobalSchemaService globalSchemaService;
		@Resource(name = "feedDataService")
		private FeedDataService feedDataService;
		@Resource(name = "pushToSolrService")
		private PushToSolrService pushToSolrService;
		@Resource(name = "ringMasterService")
		private RingMasterService ringMasterService;
		@Resource(name = "solrCloudService")
		private SolrCloudService solrCloudService;

		@Resource(name="feedProcessor")
		private FeedProcessor feedProcessor;

		@Resource(name="analyticsCronService")
		AnalyticsCronService analyticsCronService;

		@Resource(name="autocompleteCronDao")
		AutocompleteCronDao autocompleteCronDao;

		@Resource(name = "facetService")
		FacetService facetService;

		@Resource(name = "siteRequestDAO")
		SiteRequestDAO siteRequestDAO;

		@Resource(name = "dbHealthCheck")
		DbHealthCheck dbHealthCheck;

		@Resource(name = "subscribersDataDispatchService")
		SubscribersDataDispatchServiceImpl subscribersDataDispatchService;

		@Resource(name ="analyticsCronService")
		AnalyticsCronServiceImpl analyticsCronServiceImpl;


			@RequestMapping(value = "/getFeedMetadata", method = RequestMethod.GET)
		public @ResponseBody SiteDataStatsModel getFeedMetadata(HttpServletRequest
				request, HttpServletResponse response) {
			SiteDataStatsModel resp = null;
			Long siteId = Long.parseLong(request.getParameter("siteId"));

			if(null == siteId) {
				LOGGER.debug("getFeedMetadata FAILED as siteId is NULL");
				return null;
			}

			resp = siteReqService.getFeedMetaData(siteId);
			return resp;
		}

		@RequestMapping(value = "/saveUnknownFieldSchema", method = RequestMethod.POST)
		public @ResponseBody FileUploadResponse
		unknownFieldSchemaUpload(HttpServletRequest
				request, HttpServletResponse response) {
			FileUploadResponse resp = null;

			SchemaRequestModel schemaObjects =
					objMapperService.getSchemaObjectFromJSON(
							request.getParameter("schema"));

			if(null == schemaObjects) {
				return resp;
			}

			if(null == schemaObjects.getUnbxdFileName() || null == schemaObjects.getSiteId()
					|| null == schemaObjects.getSchema()) {
				return resp;
			}

			LOGGER.debug("saveUnknownFieldSchema requested for siteId: "
					+ schemaObjects.getSiteId());

			// check if facet name exists with same name
			try {
				List<Facet> facetList = facetService.getAllFacet(schemaObjects.getSiteId());
				Set<String> facetNameSet = new HashSet<>();
				for (Facet facet : facetList) {
					facetNameSet.add(facet.getFacetName());
				}
				for (SchemaForDisplay sfd : schemaObjects.getSchema()) {
					if(facetNameSet.contains(sfd.getFieldName())) {
						resp = new FileUploadResponse();
						resp.setStatusCode(902L);
						resp.setMessage(String.format("facet name exists with the name %s. Change the field name", sfd.getFieldName()));
						resp.setUnbxdFileName(schemaObjects.getUnbxdFileName());
						return resp;
					}
				}
			} catch (UnbxdFeedException e) {
				LOGGER.error("Received exception while fetching all facets for siteId: " + schemaObjects.getSiteId());
				e.printStackTrace();
				resp = new FileUploadResponse();
				resp.setStatusCode(312L);
				resp.setMessage(ErrorMessage.e312L);
				resp.setUnbxdFileName(schemaObjects.getUnbxdFileName());
				return resp;
			}

			schemaService.saveSchemaUpdatesDump(schemaObjects.getSchema(),
					schemaObjects.getSiteId(), schemaObjects.getUnbxdFileName());

			//DEC1SPRINT
			//dump delta schema to global schema
			globalSchemaService.saveGlobalSchema(schemaObjects.getSchema(), schemaObjects.getSiteId());

			resp= feedProcessor.reProcess(schemaObjects.getUnbxdFileName());

			LOGGER.debug("saveUnknownFieldSchema response for siteId: "
					+ schemaObjects.getSiteId() + " response: " + resp);

			return resp;
		}

		private void validateSchemaList(SchemaRequestModel schemaObjects) {

		}

		@RequestMapping(value = "/indexSite", method = RequestMethod.GET)
		public @ResponseBody String
			indexSiteAPI(HttpServletRequest
					request, HttpServletResponse response) {
				String siteName = request.getParameter("siteName");
				Long siteId = Long.parseLong(request.getParameter("siteId"));
				Long userId = Long.parseLong(request.getParameter("userId"));

				if(null == siteName || null == siteId || null == userId) {
					return "Please provide valid request parameters.";
				}

				LOGGER.debug("indexSite requested for siteId: "
						+ siteId);

				return feedProcessor.indexSite(siteId, siteName, userId);
		}

		@RequestMapping(value = "/index/{siteKey}", method = RequestMethod.GET)
		public @ResponseBody String index(@PathVariable("siteKey") String siteKey, HttpServletRequest request, HttpServletResponse response) {
				if(null == siteKey) {
					return "sitekey missing";
				}

				LOGGER.debug("index requested for siteId: " + siteKey);
				SiteObject siteInfo = siteReqService.getSiteByInternalSiteName(siteKey);
				if(siteInfo == null) {
					return "No site present with siteKey: " + siteKey;
				}

				return feedProcessor.indexSite(siteInfo.getSiteId(), siteInfo.getSiteName(), siteInfo.getUserId());
		}

		@RequestMapping(value = "/getFileStatus", method = RequestMethod.GET)
		public @ResponseBody DetailedFileUploadResponse
		getFileStatus(HttpServletRequest
				request, HttpServletResponse response) {
			DetailedFileUploadResponse resp = null;
			String unbxdFileName = String.valueOf(request.getParameter("unbxdFileName"));
			String apiKey = request.getParameter("key");
			if(null == apiKey || null == unbxdFileName) {
				resp = new DetailedFileUploadResponse();
				resp.setMessage("Invalid Request");
				resp.setStatusCode(1003L);
				return resp;
			}
			UserInfo user = userInfoService.getUserInfoByAPIKey(apiKey);
			if(null == user) {
				resp = new DetailedFileUploadResponse();
				resp.setMessage("Cannot find a user for this APIKey.");
				resp.setStatusCode(1003L);
				return resp;
			}
			resp = feedService.getFileStatus(unbxdFileName, user.getUserId());
			return resp;
		}

		@RequestMapping(value = "/createSite", method = RequestMethod.POST)
		public @ResponseBody CommonResponse createSite(HttpServletRequest
				request, HttpServletResponse response) {
			if(Constants.SOLR_CLOUD){
			//	return new FeedControllerNew().createSite(request,response);
				CommonResponse resp = new CommonResponse();
				String siteDetails = request.getParameter("siteDetails");
				SiteObject obj = objMapperService.getSiteObject(siteDetails);

				if (null == obj) {
					resp.setSuccess("false");
					resp.setMessage("Invalid request.");
					return resp;
				}

				Long siteId = obj.getSiteId();

				LOGGER.debug("createSite requested for siteId: "
						+ obj.getSiteId());
				//todo:following three calls should be in parallel or handled properly, one fails all fail and revert
				try {
					siteReqService.createSite(obj);
				} catch (Exception ex) {
					LOGGER.error("createSite requested for siteId: "
							+ obj.getSiteId() + " FAILED because of " + ex.getMessage(), ex);
					resp.setSuccess("false");
					resp.setMessage(ex.getMessage());
					return resp;
				}

				try {
					SiteObject site = siteReqService.getSiteRequestBySiteId(siteId);
					solrCloudService.createSite(site.getSiteNameInternal());
				} catch (Exception ex) {
					LOGGER.error("createSite solr-cloud requested for siteId: "
							+ obj.getSiteId() + " FAILED because of " + ex.getMessage(), ex);
					resp.setSuccess("false");
					resp.setMessage(ex.getMessage());
					return resp;
				}

				resp.setSuccess("true");
				LOGGER.debug("createSite requested for siteId: "
						+ obj.getSiteId() + " SUCCESS");
				resp.setMessage("Site created successfully.");
				return resp;

			}
			CommonResponse resp = new CommonResponse();
			String siteDetails = request.getParameter("siteDetails");
			SiteObject obj = objMapperService.getSiteObject(siteDetails);

			if(null == obj) {
				resp.setSuccess("false");
				resp.setMessage("Invalid request.");
				return resp;
			}

			Long siteId = obj.getSiteId();

			LOGGER.debug("createSite requested for siteId: "
					+ obj.getSiteId());

			try {
				siteReqService.createSite(obj);
			} catch(Exception ex) {
				LOGGER.error("createSite requested for siteId: "
						+ obj.getSiteId()+ " FAILED because of " + ex.getMessage(),ex);
				resp.setSuccess("false");
				resp.setMessage(ex.getMessage());
				return resp;
			}

			resp.setSuccess("true");
			LOGGER.debug("createSite requested for siteId: "
					+ obj.getSiteId()+ " SUCCESS");
			resp.setMessage("Site created successfully.");
			return resp;
		}

		@RequestMapping(value = "/{siteKey}/configure", method = RequestMethod.GET)
		public @ResponseBody
		void configure(@PathVariable("siteKey") String siteKey, HttpServletRequest
				request, HttpServletResponse response) {

			if (null == siteKey || siteKey.length() == 0) {
				response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			}
			LOGGER.debug("pushing configuration for siteId: " + siteKey);
			SiteObject siteInfo = siteReqService.getSiteByInternalSiteName(siteKey);
			if (siteInfo == null) {
				response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			}
			//solrcloud
			Boolean configureSolrCloud = pushToSolrService.pushConfig(siteInfo, Constants.SOLR_CLOUD_SC, siteKey, 0L);
			if (configureSolrCloud) {
				response.setStatus(HttpServletResponse.SC_OK);
			} else {
				response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
			}
			//rm
			NodeInfoResponse rmResp = ringMasterService.getRmEndpoints(siteKey);
			if (rmResp != null && rmResp.isSuccess() && rmResp.getNodes() != null && rmResp.getNodes().size() > 0) {
				Boolean configureSolr=false;
				for(NodeInfo info:rmResp.getNodes()	) {
					List<NodeInfo> infoL =new ArrayList<NodeInfo>();
					infoL.add(info);
					configureSolr = pushToSolrService.pushConfig(siteInfo,
							NodeInfo.getServerAddress(infoL),info.getCoreName(), 0L);
				}
				//todo:work on error handling
				if (!configureSolr) {
					//todo:rewrite with better error handling
					response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
				}
			} else {
				response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
			}
		}

		@RequestMapping(value = "/updateSite", method = RequestMethod.POST)
		public @ResponseBody CommonResponse updateSite(HttpServletRequest request, HttpServletResponse response) {
			CommonResponse resp = new CommonResponse();
			String siteDetails = request.getParameter("siteDetails");

			ObjectMapper mapper = new ObjectMapper();
			Subscription subscription = null;
			try {
				subscription = mapper.readValue(siteDetails, Subscription.class);
			} catch (IOException e) {
				LOGGER.error("Error while parsing the request object while updating " +
						"site reason " + e.getMessage());
				response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
				resp.setSuccess("false");
				resp.setMessage("Invalid request.");
				LOGGER.debug("unable to deserialize");
				return resp;
			}

			SiteObject site = siteReqService.getSiteByInternalSiteName(subscription.getSiteNameInternal());
			if(null == site) {
				response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
				resp.setSuccess("false");
				resp.setMessage("Site does not exist.");
				LOGGER.error("Site does not exist with key " + subscription.getSiteNameInternal());
				return resp;
			}
			LOGGER.info("Request recieved for subscription of " + subscription.getSubscribers() +
					" for site " + subscription.getSiteNameInternal());

			siteReqService.updateSubscriber(site.getSiteId(), subscription.getSubscribers());
			resp.setSuccess("true");
			resp.setMessage("Site updated successfully.");
			return resp;
		}

		@RequestMapping(value = "/getFeedStatus", method = RequestMethod.GET)
		public @ResponseBody DetailedFeedUploadResponse
		getFeedStatus(HttpServletRequest
				request, HttpServletResponse response) {
			DetailedFeedUploadResponse resp = null;
			String isiteName = String.valueOf(
					request.getParameter("isiteName"));

			String apiKey = request.getParameter("key");

			if(null == apiKey || null == isiteName) {
				resp = new DetailedFeedUploadResponse();
				resp.setMessage("Invalid Request. ApiKey or siteName is null.");
				LOGGER.debug("Invalid Request. ApiKey or siteName is null.");
				resp.setStatusCode(1003L);
				return resp;
			}

			UserInfo user = userInfoService.getUserInfoByAPIKey(apiKey);

			if(null == user) {
				resp = new DetailedFeedUploadResponse();
				resp.setMessage("Invalid Request. No user found for this APiKey.");
				LOGGER.debug("Invalid Request. No user found for this APiKey.");
				resp.setStatusCode(1003L);
				return resp;
			}

			resp = feedService.getFeedStatus(isiteName, user.getUserId());

			return resp;
		}

		@RequestMapping(value = "/getTaxonomyMap", method = RequestMethod.GET)
		public @ResponseBody TaxonomyNodeDataWrapper
		getTaxonomyMap(HttpServletRequest
				request, HttpServletResponse response) {
			TaxonomyNodeDataWrapper resp = null;
			String isiteName = String.valueOf(
					request.getParameter("isiteName"));
			if(null == isiteName) {
				LOGGER.debug("isiteName is null.");
				return resp;
			}

			resp = siteReqService.getTaxonomyData(isiteName);

			return resp;
		}

		@RequestMapping(value = "/deleteSite", method = RequestMethod.POST)
		public @ResponseBody CommonResponse deleteSite(HttpServletRequest
				request, HttpServletResponse response) {

			CommonResponse resp = new CommonResponse();
			Long siteId = Long.parseLong(request.getParameter("siteId"));
			SiteObject obj = siteReqService.getSiteRequestBySiteId(siteId);

			if(null == siteId) {
				resp.setSuccess("false");
				resp.setMessage("Invalid request.");
				return resp;
			}

			try {
				siteReqService.deleteSite(siteId);
				if(null != obj)
					feedService.delete(obj.getUserId(), obj.getSiteName());
			} catch(Exception ex) {
				resp.setSuccess("false");
				resp.setMessage(ex.getMessage());
				LOGGER.debug("deleteSite requested for siteId: "
						+ siteId+ " FAILED !! Error : "+ ex.getMessage() , ex );
				return resp;
			}

			resp.setSuccess("true");
			LOGGER.debug("deleteSite requested for siteId: "
					+ siteId+ " SUCCESS");
			resp.setMessage("Site deleted successfully");
			return resp;
		}

		@RequestMapping(value = "/getIndexFields", method = RequestMethod.GET)
		public @ResponseBody List<IndexField>
		getIndexFields(HttpServletRequest
				request, HttpServletResponse response) {
			List<IndexField> resp = new ArrayList<>();
			Long siteId = Long.parseLong(request.getParameter("siteId"));
			if(null == siteId) {
				return resp;
			}
			resp = globalSchemaService.getIndexFieldsForDisplay(siteId);
			return resp;
		}

		@RequestMapping(value = "/getFeedHistory", method = RequestMethod.GET)
		public @ResponseBody List<FeedHistoryResponse>
		getFeedHistory(HttpServletRequest
				request, HttpServletResponse response) {

			List<FeedHistoryResponse> resp = new ArrayList<>();
			Long userId = Long.valueOf(request.getParameter("userId"));

			resp = feedService.getFeedHistory(userId);
			return resp;
		}

		@RequestMapping(value = "/getFeedHistoryForSite", method = RequestMethod.GET)
		public @ResponseBody List<FeedHistoryResponse>
		getFeedHistoryPerSite(HttpServletRequest
				request, HttpServletResponse response) {

			List<FeedHistoryResponse> resp = new ArrayList<>();
			Long userId = Long.valueOf(request.getParameter("userId"));
			String siteName = request.getParameter("siteName");
			resp = feedService.getFeedHistoryPerSite(userId, siteName);
			return resp;
		}



	   @RequestMapping("/search")
	   public @ResponseBody
	   ResultSet search(HttpServletRequest request, HttpServletResponse response) {
		   QueryModel query = null;
		   try {
			   query = objMapperService.getQueryModel(request.getParameter("query"));
		   } catch (Exception e) {
			   ResultSet rs = new ResultSet();
			   rs.setSuccess(false);
			   rs.setMessage("Invalid query");
			   return rs;
		   }

		   if(query == null || query.getQuery() == null){
			   ResultSet rs = new ResultSet();
			   rs.setSuccess(false);
			   rs.setMessage("Invalid query");
			   return rs;
		   }

		   return feedDataService.search(query);
	   }


	   @RequestMapping(value = "/sendDataToAnalytics", method = RequestMethod.POST)
	   public @ResponseBody CommonResponse sendDataToAnalytics (HttpServletRequest request, HttpServletResponse response) {
		   String iSiteName = request.getParameter("iSiteName");
		   CommonResponse resp = new CommonResponse();

		   if(iSiteName == null || iSiteName.isEmpty()) {
			   resp.setSuccess("false");
			   resp.setMessage("iSiteName is missing in the query parameter");
			   return resp;
		   }

		   SiteObject siteDetail = siteReqService.getSiteByInternalSiteName(iSiteName);

		   if(siteDetail == null) {
			   resp.setSuccess("false");
			   resp.setMessage("No site received for iSiteName: " + iSiteName);
			   return resp;
		   }

		   boolean status = analyticsCronService.updateLastPushTimeStampForASite(siteDetail.getSiteId(), 0L);

		   if(status == false) {
			   resp.setSuccess("false");
			   resp.setMessage("Analytics is not a subscriber for this site as its entry is not present in analyticsCron DB. iSiteName: " + iSiteName);
			   return resp;
		   }

		   resp.setSuccess("true");
		   resp.setMessage("Request received successfully. The data will be sent to analytics soon.");
		   return resp;
	   }

	   @RequestMapping(value = "/sendAutosuggestDataToSearch", method = RequestMethod.POST)
	   public @ResponseBody CommonResponse sendAutosuggestDataToSearch (HttpServletRequest request, HttpServletResponse response) {
		   String iSiteName = request.getParameter("iSiteName");
		   String indexingType = request.getParameter(Constants.AUTOSUGGES_INDEXING_TYPE);
		   if(indexingType == null) {
			   indexingType = AutoSuggestIndexingType.FULL.toString();
		   }
		   CommonResponse resp = new CommonResponse();

		   if(iSiteName == null || iSiteName.isEmpty()) {
			   resp.setSuccess("false");
			   resp.setMessage("iSiteName is missing in the query parameter");
			   return resp;
		   }

		   SiteObject siteDetail = siteReqService.getSiteByInternalSiteName(iSiteName);

		   if(siteDetail == null) {
			   resp.setSuccess("false");
			   resp.setMessage("No site received for iSiteName: " + iSiteName);
			   return resp;
		   }

		   String ipAddress = request.getHeader("X-FORWARDED-FOR");
		   if(ipAddress == null) {
			   ipAddress = request.getRemoteAddr();
		   }
		   LOGGER.info(String.format("Received request to generate autsuggest data for siteKey: %s and ipAddress: %s", iSiteName, ipAddress));

		   boolean status = autocompleteCronDao.updateEntry(siteDetail.getSiteId(), 0L, indexingType);

		   if(status == false) {
			   resp.setSuccess("false");
			   resp.setMessage("autosuggest data cannot be sent for this site as its entry is not present in autocompleteCron DB. iSiteName: " + iSiteName);
			   return resp;
		   }

		   resp.setSuccess("true");
		   resp.setMessage("Request received successfully. The auto suggest data will be sent to search soon.");
		   return resp;
	   }

	   @RequestMapping(value = "/getFeaturedFields", method = RequestMethod.GET)
	   public @ResponseBody FeaturedFieldsResponse getFeaturedFields(HttpServletRequest request, HttpServletResponse response) {
		   String iSiteName = request.getParameter("iSiteName");
		   FeaturedFieldsResponse resp = new FeaturedFieldsResponse();

		   if(iSiteName == null || iSiteName.isEmpty()) {
			   resp.setStatusCode(500);
			   return resp;
		   }

		   SiteObject siteDetail = siteReqService.getSiteByInternalSiteName(iSiteName);

		   if(siteDetail == null) {
			   resp.setStatusCode(501);
			   return resp;
		   }

		   Set<String> fields = FeaturedFieldSchemaV2.getReservedKeyNamesSet();
		   resp.setFieldSet(fields);
		   resp.setStatusCode(200);

		   return resp;
	   }

		@RequestMapping(value = "/saveDimensionMap/{iSiteName}", method = RequestMethod.POST)
		public @ResponseBody CommonResponse saveDimensionMap(@PathVariable("iSiteName") String iSiteName,
				HttpServletRequest request, HttpServletResponse response) {

		   CommonResponse resp = new CommonResponse();

		   if(iSiteName == null || iSiteName.isEmpty()) {
			   resp.setSuccess("false");
			   resp.setMessage("iSiteName is missing in the query parameter for saveDimensionMap.");
			   return resp;
		   }

		   SiteObject siteDetail = siteReqService.getSiteByInternalSiteName(iSiteName);

		   if(siteDetail == null) {
			   resp.setSuccess("false");
			   resp.setMessage("No site found for iSiteName: " + iSiteName
						+ " for saveDimensionMap request.");
			   LOGGER.error("No site found for iSiteName: " + iSiteName
					   + " for saveDimensionMap request.");
			   return resp;
		   }

			Long siteId = siteDetail.getSiteId();
			Map<String,String> mappingData = new HashMap<>();
			try {
				mappingData = objMapperService.getPostData(request.getInputStream());
		   } catch(Exception ex) {
			   LOGGER.error( "iSiteName: " + iSiteName
						+ " for saveDimensionMap request. " + ex.getMessage());
			   resp.setSuccess("false");
			   resp.setMessage("Bad data for iSiteName: " + iSiteName
						+ " for saveDimensionMap request.");
			   return resp;
		   }

		   Set<String> errorSet = globalSchemaService.saveDimensionMappingData(mappingData, siteDetail.getSiteId());

		   if(errorSet.isEmpty()) {
			   resp.setSuccess("true");
			   resp.setMessage("saveDimensionMap processed successfully for: " + iSiteName);
			   /**
				* Notify Analytics
				*/
			   NetworkCalls.notifyAnalyticsForDimensionMapUpdate(iSiteName);
			   return resp;
		   } else {
			   String message = StringUtils.collectionToCommaDelimitedString(errorSet);
			   resp.setSuccess("false");
			   resp.setMessage(message);
			   LOGGER.error( "saveDimensionMap: Could not save mapping data for: " + iSiteName + " coz: " + message);
			   return resp;
		   }
	   }

	   @RequestMapping(value = "/getDimensionMap", method = RequestMethod.GET)
	   public @ResponseBody SaveDimensionMapResponse getDimensionMap(HttpServletRequest request, HttpServletResponse response) {
		   String iSiteName = request.getParameter("iSiteName");
		   SaveDimensionMapResponse resp = new SaveDimensionMapResponse();

		   if(iSiteName == null || iSiteName.isEmpty()) {
			   LOGGER.error("iSiteName is missing in the query parameter for getDimensionMap.");
			   resp.setStatusCode(500);
			   return resp;
		   }

		   SiteObject siteDetail = siteReqService.getSiteByInternalSiteName(iSiteName);

		   if(siteDetail == null) {
			   resp.setStatusCode(501);
			   LOGGER.error("No site found for iSiteName: " + iSiteName + " for getDimensionMap request.");
			   return resp;
		   }

		   Map<String, String> data = schemaService.getDimensionMap(siteDetail.getSiteId());

		   if(null == data || data.isEmpty()) {
			   resp.setStatusCode(502);
			   LOGGER.error("No data found for iSiteName: " + iSiteName + " for getDimensionMap request.");
			   return resp;
		   } else {
			   resp.setStatusCode(200);
			   resp.setData(data);
			   return resp;
		   }
	   }

	   @RequestMapping(value = "/getUnmappedFields", method = RequestMethod.GET)
	   public @ResponseBody GetDimensionMapResponse getUnmappedFields(HttpServletRequest request, HttpServletResponse response) {
		   String iSiteName = request.getParameter("iSiteName");
		   GetDimensionMapResponse resp = new GetDimensionMapResponse();

		   if(iSiteName == null || iSiteName.isEmpty()) {
			   LOGGER.error("iSiteName is missing in the query parameter for getUnmappedFields.");
			   resp.setStatusCode(500);
			   return resp;
		   }

		   SiteObject siteDetail = siteReqService.getSiteByInternalSiteName(iSiteName);

		   if(siteDetail == null) {
			   resp.setStatusCode(501);
			   LOGGER.error("No site found for iSiteName: " + iSiteName + " for getUnmappedFields request.");
			   return resp;
		   }

		   Map<String,Set<String>> data = schemaService.getUnMappedFields(siteDetail.getSiteId());

		   if(null == data || data.isEmpty()) {
			   resp.setStatusCode(502);
			   LOGGER.error("No data found for iSiteName: " + iSiteName + " for getUnmappedFields request.");
			   return resp;
		   } else {
			   resp.setStatusCode(200);
			   resp.setData(data);
			   return resp;
		   }
	   }

		@RequestMapping(value = "/getDimensionMapAndFields", method = RequestMethod.GET)
		public @ResponseBody
		GetDimensionMapAndFieldsResponse getDimensionMapAndFields(
				HttpServletRequest request, HttpServletResponse response) {

			String iSiteName = request.getParameter("iSiteName");
			GetDimensionMapAndFieldsResponse resp = new GetDimensionMapAndFieldsResponse();

			try {
				LOGGER.info("Received request to getDimensionMapAndFields for iSiteName: "
						+ iSiteName);
				validate(iSiteName);
				SiteObject siteDetail = siteReqService
						.getSiteByInternalSiteName(iSiteName);
				List<Errors> errors = new ArrayList<>();

				if (siteDetail == null) {
					errors.add(new Errors(ErrorCode.SiteDetailNull, String.format(
							"No site details found for iSiteName %s", iSiteName)));
					throw new UnbxdFeedException(errors);
				}

				if (siteDetail.getSiteId() == null || siteDetail.getSiteId() <= 0) {
					errors.add(new Errors(
							ErrorCode.SiteIdInvalid,
							String.format(
									"siteId is null or invalid for iSiteName: %s, siteId: %s",
									iSiteName, siteDetail.getSiteId())));
					throw new UnbxdFeedException(errors);
				}

				List<IndexField> indexFields = globalSchemaService
						.getIndexFieldsForDisplay(siteDetail.getSiteId());
				List<String> fields = new ArrayList<>();
				for (IndexField indexField : indexFields) {
					fields.add(indexField.getFieldName());
				}

				Map<String, String> dimensionMap = schemaService
						.getDimensionMap(siteDetail.getSiteId());
				resp.setDimensionMap(dimensionMap);
				resp.setFields(fields);
				resp.setStatus(OperationStatus.Success);
			} catch (UnbxdFeedException ex) {
				resp.setErrors(ex.getErrors());
				resp.setStatus(OperationStatus.Failure);
				LOGGER.error("Received UnbxdFeedException for getDimensionMapAndFields for iSiteName: "
						+ iSiteName);
			} catch (Exception ex) {
				List<Errors> errors = new ArrayList<>();
				errors.add(new Errors(ErrorCode.NA, String.format(
						"Received the exception: %s ,of type %s", ex.getMessage(),
						ex.getClass().getName())));
				resp.setErrors(errors);
				resp.setStatus(OperationStatus.Failure);
				LOGGER.error(String
						.format("Received the exception: %s ,of type %s, for iSiteName: %s",
								ex.getMessage(), ex.getClass().getName(), iSiteName));
			}

			return resp;
		}

		@RequestMapping(value = "/deployCode", method = RequestMethod.POST)
		public @ResponseBody CommonResponse
		deploy(HttpServletRequest
				request, HttpServletResponse response) {

			CommonResponse resp = new CommonResponse();
			Map<String,String> mappingData = new HashMap<>();
			try {
				mappingData = objMapperService.getPostData(request.getInputStream());
			} catch(Exception ex) {
				LOGGER.error("deployCode failed " + ex.getMessage());
				resp.setSuccess("false");
				resp.setMessage("Bad data");
				return resp;
			}
			if(mappingData.get(DEPLOY_CODE).equals("true")) {
				Constants.serverActive = false;
				resp.setMessage("Server Inactive");
			} else {
				Constants.serverActive = true;
				resp.setMessage("Server Active");
			}

			resp.setSuccess("true");

			return resp;
		}

		@RequestMapping(value = "/processingLockMonitor.do", produces = "application/json")
		public @ResponseBody Map<UnbxdFeedFileInfo, Integer> processingLockMonitor() {
			return ProcessingLockMonitor.PROCESSING_LOCK_WAITING_TIME;
		}

		private void validate(String iSiteName) throws UnbxdFeedException {
			List<Errors> errors = new ArrayList<>();
			if(Utilities.isNullOrEmpty(iSiteName)) {
				errors.add(new Errors(ErrorCode.DataValidationFailed, "query parameter iSiteName cannot be null or empty"));
				throw new UnbxdFeedException(errors);
			}
		}

		@RequestMapping(value = "/getHealthStatus.do", produces = "application/json")
		public
		@ResponseBody Integer getHealthStatus(HttpServletRequest request,
								HttpServletResponse response) throws ServletException, IOException {

			return 200;
		}

		@RequestMapping(value = "/getTaskStatus" , method = RequestMethod.GET)
		public @ResponseBody
		TaskStatusResponse getTaskStatus() {
			TaskStatusResponse resp = new TaskStatusResponse();
			resp.setStatus(OperationStatus.Success);
			resp.setTaskExecutors(TaskExecutorFactory.getExecutorDetails());
			return resp;
		}

		@RequestMapping(value = "/getMongoSHealthStatus.do", produces = "application/json")
		public @ResponseBody Integer getMongoSHealthStatus(HttpServletRequest request,
														   HttpServletResponse response) throws ServletException, IOException {
			ClassPathResource res = new ClassPathResource(Constants.APPLICATION_PROPS_DEV);
			try {
				Properties props = PropertiesLoaderUtils.loadProperties(res);
				String dbinstance = props.getProperty("databaseInstance");
				if (dbHealthCheck.isHealthy(dbinstance)) {
					return 200;
				}
			}
			catch ( Exception ex)
				{
					LOGGER.error("Exception occurred while doing DB Healthcheck, Exception is : ",ex);
					LOGGER.error("DB HealthCheck not Successful");
				}
			response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
			return 501;
		}


		@RequestMapping(value = "/testAnalyticsSubscriberApi.do", produces = "application/json")
		public @ResponseBody String testAnalyticsSubscriberApi(HttpServletRequest request,
																HttpServletResponse response, @Param("data") String data, @Param("siteId") Long siteId) throws ServletException, IOException {

			Subscriber subscriber = analyticsCronServiceImpl.getSubscriber(siteId);

			LOGGER.info(subscriber);
			boolean success = false;
			try{

				String uri = subscriber.getUri();

				subscriber.setUri(uri + String.format("?type=%s&action=%s", FeedDataImportType.FULL.toString(), FeedDataSendAction.START.toString()));
				data = "[]";
				success = subscribersDataDispatchService.sendDataToSubscriber(subscriber, data);

				uri = uri + String.format("?action=%s&type=%s&commit=%s", FeedDataSendAction.END.toString(), FeedDataImportType.FULL.toString(), false);

			}catch (Exception e){
				LOGGER.error(e);
				return e.getMessage();
			}
			return success+"";

		}



	}
