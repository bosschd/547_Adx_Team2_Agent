package edu.umich.tacadx.agents;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.EnumMap;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.lang.Object;
import java.lang.Number;
import java.lang.Double;


import se.sics.isl.transport.Transportable;
import se.sics.tasim.aw.Agent;
import se.sics.tasim.aw.Message;
import se.sics.tasim.props.SimulationStatus;
import se.sics.tasim.props.StartInfo;
import tau.tac.adx.ads.properties.AdType;
import tau.tac.adx.demand.CampaignStats;
import tau.tac.adx.devices.Device;
import tau.tac.adx.props.AdxBidBundle;
import tau.tac.adx.props.AdxQuery;
import tau.tac.adx.props.PublisherCatalog;
import tau.tac.adx.props.PublisherCatalogEntry;
import tau.tac.adx.report.adn.AdNetworkReport;
import tau.tac.adx.report.adn.MarketSegment;
import tau.tac.adx.report.demand.AdNetBidMessage;
import tau.tac.adx.report.demand.AdNetworkDailyNotification;
import tau.tac.adx.report.demand.CampaignOpportunityMessage;
import tau.tac.adx.report.demand.CampaignReport;
import tau.tac.adx.report.demand.CampaignReportKey;
import tau.tac.adx.report.demand.InitialCampaignMessage;
import tau.tac.adx.report.publisher.AdxPublisherReport;
import tau.tac.adx.report.publisher.AdxPublisherReportEntry;
import edu.umich.eecs.tac.props.Ad;
import edu.umich.eecs.tac.props.BankStatus;

/**
 * 
 * @author Mariano Schain
 * 
 */
public class Team2_Ad_Network extends Agent {

	private final Logger log = Logger
			.getLogger(Team2_Ad_Network.class.getName());

	/*
	 * Basic simulation information. An agent should receive the {@link
	 * StartInfo} at the beginning of the game or during recovery.
	 */
	@SuppressWarnings("unused")
	private StartInfo startInfo;

	/**
	 * Messages received:
	 * 
	 * We keep all the {@link CampaignReport campaign reports} 
	 * delivered to the agent. We also keep the initialization 
	 * messages {@link PublisherCatalog} and
	 * {@link InitialCampaignMessage} and the most recent messages and reports
	 * {@link CampaignOpportunityMessage}, {@link CampaignReport}, and
	 * {@link AdNetworkDailyNotification}.
	 */
	private final Queue<CampaignReport> campaignReports;
	private PublisherCatalog publisherCatalog;
	private InitialCampaignMessage initialCampaignMessage;
	private AdNetworkDailyNotification adNetworkDailyNotification;
	private EnumMap<MarketSegment, Double> segmentValues;

	/*
	 * The addresses of server entities to which the agent should send the daily
	 * bids data
	 */
	private String demandAgentAddress;
	private String adxAgentAddress;

	/*
	 * we maintain a list of queries - each characterized by the web site (the
	 * publisher), the device type, the ad type, and the user market segment
	 */
	private AdxQuery[] queries;

	/**
	 * Information regarding the latest campaign opportunity announced
	 */
	private CampaignData pendingCampaign;

	/**
	 * We maintain a collection (mapped by the campaign id) of the campaigns won
	 * by our agent.
	 */
	private Map<Integer, CampaignData> myCampaigns;

	/*
	 * the bidBundle to be sent daily to the AdX
	 */
	private AdxBidBundle bidBundle;

	/*
	 * The current bid level for the user classification service
	 */
	double ucsBid;

	/*
	 * The targeted service level for the user classification service
	 */
	double ucsTargetLevel;

	private Map<String, Integer> ageProbabilityMap;

	private Map<String, Integer> incomeProbabilityMap;

	private Map<String, Integer> genderProbabilityMap;

	/*
	 * current day of simulation
	 */
	private int day;

	private Random randomGenerator;

	public Team2_Ad_Network() {
		campaignReports = new LinkedList<CampaignReport>();
		ageProbabilityMap = new HashMap<String, Integer>();
		incomeProbabilityMap = new HashMap<String, Integer>();
		genderProbabilityMap = new HashMap<String, Integer>();
		segmentValues = new EnumMap<MarketSegment, Double>(MarketSegment.class);
		Double zero = Double.valueOf(0.0d);
		segmentValues.put(MarketSegment.YOUNG, zero);
		segmentValues.put(MarketSegment.OLD, zero);
		segmentValues.put(MarketSegment.LOW_INCOME, zero);
		segmentValues.put(MarketSegment.HIGH_INCOME, zero);
		segmentValues.put(MarketSegment.MALE, zero);
		segmentValues.put(MarketSegment.FEMALE, zero);
		setUpMaps();
	}

	void setUpMaps() {
		ageProbabilityMap.put("yahoo", 71);	
		ageProbabilityMap.put("cnn", 58);	
		ageProbabilityMap.put("nyt", 57);	//?
		ageProbabilityMap.put("hfn", 54);	
		ageProbabilityMap.put("msn", 68);
		ageProbabilityMap.put("fox", 60);
		ageProbabilityMap.put("amazon", 67);	
		ageProbabilityMap.put("ebay", 69);
		ageProbabilityMap.put("walmart", 72);	
		ageProbabilityMap.put("target", 71);	//?
		ageProbabilityMap.put("bestbuy", 72);	//?
		ageProbabilityMap.put("sears", 62);	//?
		ageProbabilityMap.put("webmd", 62);	//?
		ageProbabilityMap.put("ehow", 72);	//?
		ageProbabilityMap.put("ask", 70);	//?
		ageProbabilityMap.put("tripadvisor", 60);	//?
		ageProbabilityMap.put("cnet", 68);	//?
		ageProbabilityMap.put("weather", 64);	//?
		incomeProbabilityMap.put("yahoo", 42);	
		incomeProbabilityMap.put("cnn", 37);	
		incomeProbabilityMap.put("nyt", 36);	//?
		incomeProbabilityMap.put("hfn", 73);	
		incomeProbabilityMap.put("msn", 41);
		incomeProbabilityMap.put("fox", 38);
		incomeProbabilityMap.put("amazon", 40);	
		incomeProbabilityMap.put("ebay", 39);
		incomeProbabilityMap.put("walmart", 45);	
		incomeProbabilityMap.put("target", 41);	//?
		incomeProbabilityMap.put("bestbuy", 41);	//?
		incomeProbabilityMap.put("sears", 40);	//?
		incomeProbabilityMap.put("webmd", 43);	//?
		incomeProbabilityMap.put("ehow", 41);	//?
		incomeProbabilityMap.put("ask", 43);	//?
		incomeProbabilityMap.put("tripadvisor", 32);	//?
		incomeProbabilityMap.put("cnet", 42);	//?
		incomeProbabilityMap.put("weather", 35);	//?
		genderProbabilityMap.put("yahoo", 48);	
		genderProbabilityMap.put("cnn", 58);	
		genderProbabilityMap.put("nyt", 53);	//?
		genderProbabilityMap.put("hfn", 51);	
		genderProbabilityMap.put("msn", 49);
		genderProbabilityMap.put("fox", 55);
		genderProbabilityMap.put("amazon", 48);	
		genderProbabilityMap.put("ebay", 52);
		genderProbabilityMap.put("walmart", 43);	
		genderProbabilityMap.put("target", 40);	//?
		genderProbabilityMap.put("bestbuy", 52);	//?
		genderProbabilityMap.put("sears", 48);	//?
		genderProbabilityMap.put("webmd", 40);	//?
		genderProbabilityMap.put("ehow", 46);	//?
		genderProbabilityMap.put("ask", 43);	//?
		genderProbabilityMap.put("tripadvisor", 45);	//?
		genderProbabilityMap.put("cnet", 60);	//?
		genderProbabilityMap.put("weather", 51);	//?
	}

	@Override
	protected void messageReceived(Message message) {
		try {
			Transportable content = message.getContent();
			
			//log.fine(message.getContent().getClass().toString());
			
			if (content instanceof InitialCampaignMessage) {
				handleInitialCampaignMessage((InitialCampaignMessage) content);
			} else if (content instanceof CampaignOpportunityMessage) {
				handleICampaignOpportunityMessage((CampaignOpportunityMessage) content);
			} else if (content instanceof CampaignReport) {
				handleCampaignReport((CampaignReport) content);
			} else if (content instanceof AdNetworkDailyNotification) {
				handleAdNetworkDailyNotification((AdNetworkDailyNotification) content);
			} else if (content instanceof AdxPublisherReport) {
				handleAdxPublisherReport((AdxPublisherReport) content);
			} else if (content instanceof SimulationStatus) {
				handleSimulationStatus((SimulationStatus) content);
			} else if (content instanceof PublisherCatalog) {
				handlePublisherCatalog((PublisherCatalog) content);
			} else if (content instanceof AdNetworkReport) {
				handleAdNetworkReport((AdNetworkReport) content);
			} else if (content instanceof StartInfo) {
				handleStartInfo((StartInfo) content);
			} else if (content instanceof BankStatus) {
				handleBankStatus((BankStatus) content);
			} else {
				log.info("UNKNOWN Message Received: " + content);
			}

		} catch (NullPointerException e) {
			this.log.log(Level.SEVERE,
					"Exception thrown while trying to parse message." + e);
			return;
		}
	}

	private void handleBankStatus(BankStatus content) {
		log.info("Day " + day + " :" + content.toString());
	}

	/**
	 * Processes the start information.
	 * 
	 * @param startInfo
	 *            the start information.
	 */
	protected void handleStartInfo(StartInfo startInfo) {
		this.startInfo = startInfo;
	}

	/**
	 * Process the reported set of publishers
	 * 
	 * @param publisherCatalog
	 */
	private void handlePublisherCatalog(PublisherCatalog publisherCatalog) {
		this.publisherCatalog = publisherCatalog;
		generateAdxQuerySpace();
	}

	/**
	 * On day 0, a campaign (the "initial campaign") is allocated to each
	 * competing agent. The campaign starts on day 1. The address of the
	 * server's AdxAgent (to which bid bundles are sent) and DemandAgent (to
	 * which bids regarding campaign opportunities may be sent in subsequent
	 * days) are also reported in the initial campaign message
	 */
	private void handleInitialCampaignMessage(
			InitialCampaignMessage campaignMessage) {
		log.info(campaignMessage.toString());

		day = 0;

		initialCampaignMessage = campaignMessage;
		demandAgentAddress = campaignMessage.getDemandAgentAddress();
		adxAgentAddress = campaignMessage.getAdxAgentAddress();

		CampaignData campaignData = new CampaignData(initialCampaignMessage);
		campaignData.setBudget(initialCampaignMessage.getReachImps() / 1000.0);

		/*
		 * The initial campaign is already allocated to our agent so we add it
		 * to our allocated-campaigns list.
		 */
		log.info("Day " + day + ": Allocated campaign - " + campaignData);
		myCampaigns.put(initialCampaignMessage.getId(), campaignData);
	}

	/**
	 * On day n ( > 0) a campaign opportunity is announced to the competing
	 * agents. The campaign starts on day n + 2 or later and the agents may send
	 * (on day n) related bids (attempting to win the campaign). The allocation
	 * (the winner) is announced to the competing agents during day n + 1.
	 */
	private void handleICampaignOpportunityMessage(
			CampaignOpportunityMessage com) {

		boolean doIBid = true;
	
		day = com.getDay();

		pendingCampaign = new CampaignData(com);
		log.info("Day " + day + ": Campaign opportunity - " + pendingCampaign);

		/*
		 * The campaign requires com.getReachImps() impressions. The competing
		 * Ad Networks bid for the total campaign Budget (that is, the ad
		 * network that offers the lowest budget gets the campaign allocated).
		 * The advertiser is willing to pay the AdNetwork at most 1$ CPM,
		 * therefore the total number of impressions may be treated as a reserve
		 * (upper bound) price for the auction.
		 */
		
		for (CampaignData campaign : myCampaigns.values()) {
			for(MarketSegment current1 : campaign.targetSegment) {
				for(MarketSegment current2 : campaign.targetSegment) {
					if(current1 != current2) {
						//Identical market segments
						if(pendingCampaign.targetSegment.contains(current1) && pendingCampaign.targetSegment.contains(current2)) {
							doIBid = false;
						}
						//Overlapping
						if(pendingCampaign.targetSegment.contains(current1)) {
							switch (current2) {
								case MALE:
									if(!pendingCampaign.targetSegment.contains(MarketSegment.FEMALE)) {
										doIBid = false;
									}
									break;
								case FEMALE:
									if(!pendingCampaign.targetSegment.contains(MarketSegment.MALE)) {
										doIBid = false;
									}
									break;
								case YOUNG:
									if(!pendingCampaign.targetSegment.contains(MarketSegment.OLD)) {
										doIBid = false;
									}
									break;
								case OLD:
									if(!pendingCampaign.targetSegment.contains(MarketSegment.YOUNG)) {
										doIBid = false;
									}
									break;
								case LOW_INCOME:
									if(!pendingCampaign.targetSegment.contains(MarketSegment.HIGH_INCOME)) {
										doIBid = false;
									}
									break;
								case HIGH_INCOME:
									if(!pendingCampaign.targetSegment.contains(MarketSegment.LOW_INCOME)) {
										doIBid = false;
									}
									break;
							}
						}
						if(pendingCampaign.targetSegment.contains(current2)) {
							switch (current1) {
								case MALE:
									if(!pendingCampaign.targetSegment.contains(MarketSegment.FEMALE)) {
										doIBid = false;
									}
									break;
								case FEMALE:
									if(!pendingCampaign.targetSegment.contains(MarketSegment.MALE)) {
										doIBid = false;
									}
									break;
								case YOUNG:
									if(!pendingCampaign.targetSegment.contains(MarketSegment.OLD)) {
										doIBid = false;
									}
									break;
								case OLD:
									if(!pendingCampaign.targetSegment.contains(MarketSegment.YOUNG)) {
										doIBid = false;
									}
									break;
								case LOW_INCOME:
									if(!pendingCampaign.targetSegment.contains(MarketSegment.HIGH_INCOME)) {
										doIBid = false;
									}
									break;
								case HIGH_INCOME:
									if(!pendingCampaign.targetSegment.contains(MarketSegment.LOW_INCOME)) {
										doIBid = false;
									}
									break;
							}
						}
					}
				}
			}
		}
		if(doIBid) {
			double campaignValue = 0.0;
			for(MarketSegment segment : pendingCampaign.targetSegment) {
				campaignValue += 0.5 * segmentValues.get(segment);
			}
//			long cmpBid = 1 + Math.abs((randomGenerator.nextLong())
//					% (com.getReachImps()));
//			long cmpBid = 1;

	
		/*
		 * Adjust ucs bid s.t. target level is achieved. Note: The bid for the
		 * user classification service is piggybacked
		 */

			if (adNetworkDailyNotification != null) {
				double ucsLevel = adNetworkDailyNotification.getServiceLevel();
				campaignValue = campaignValue * ucsLevel;
				
				double prevUcsBid = ucsBid;

			/* UCS Bid should not exceed 0.2 */
				ucsBid = Math.min(0.1 + 0.1*randomGenerator.nextDouble(), prevUcsBid * (1 + ucsTargetLevel - ucsLevel));

				log.info("Day " + day + ": Adjusting ucs bid: was " + prevUcsBid
						+ " level reported: " + ucsLevel + " target: "
						+ ucsTargetLevel + " adjusted: " + ucsBid);
			} else {
				log.info("Day " + day + ": Initial ucs bid is " + ucsBid);
			}

		/* Note: Campaign bid is in millis */
			long cmpBid = (long) campaignValue;
			
			double cmpBidUnits = cmpBid / 1000.0;
//			System.out.println(cmpBidUnits);

			log.info("Day " + day + ": Campaign total budget bid: " + cmpBidUnits);
			
			AdNetBidMessage bids = new AdNetBidMessage(ucsBid, pendingCampaign.id,
				cmpBid);
			sendMessage(demandAgentAddress, bids);
		}
	}

	/**
	 * On day n ( > 0), the result of the UserClassificationService and Campaign
	 * auctions (for which the competing agents sent bids during day n -1) are
	 * reported. The reported Campaign starts in day n+1 or later and the user
	 * classification service level is applicable starting from day n+1.
	 */
	private void handleAdNetworkDailyNotification(
			AdNetworkDailyNotification notificationMessage) {

		adNetworkDailyNotification = notificationMessage;

		log.info("Day " + day + ": Daily notification for campaign "
				+ adNetworkDailyNotification.getCampaignId());

		String campaignAllocatedTo = " allocated to "
				+ notificationMessage.getWinner();

		if ((pendingCampaign.id == adNetworkDailyNotification.getCampaignId())
				&& (notificationMessage.getCost() != 0)) {

			/* add campaign to list of won campaigns */
			pendingCampaign.setBudget(notificationMessage.getCost());

			myCampaigns.put(pendingCampaign.id, pendingCampaign);

			campaignAllocatedTo = " WON at cost "
					+ notificationMessage.getCost();
		}

		log.info("Day " + day + ": " + campaignAllocatedTo
				+ ". UCS Level set to " + notificationMessage.getServiceLevel()
				+ " at price " + notificationMessage.getPrice()
				+ " Qualit Score is: " + notificationMessage.getQualityScore());
	}

	/**
	 * The SimulationStatus message received on day n indicates that the
	 * calculation time is up and the agent is requested to send its bid bundle
	 * to the AdX.
	 */
	private void handleSimulationStatus(SimulationStatus simulationStatus) {
		log.info("Day " + day + " : Simulation Status Received");
		sendBidAndAds();
		log.info("Day " + day + " ended. Starting next day");
		++day;
	}

	/**
	 * 
	 */
	protected void sendBidAndAds() {

		bidBundle = new AdxBidBundle();
		int entrySum = 0;

		/*
		 * 
		 */
		for (CampaignData campaign : myCampaigns.values()) {

			int dayBiddingFor = day + 1;

			/* A fixed random bid, for all queries of the campaign */
			/*
			 * Note: bidding per 1000 imps (CPM) - no more than average budget
			 * revenue per imp
			 */

			Random rnd = new Random();
			double avgCmpRevenuePerImp = campaign.budget / campaign.reachImps;
			double rbid = 1000.0 * rnd.nextDouble() * avgCmpRevenuePerImp;

			/*
			 * add bid entries w.r.t. each active campaign with remaining
			 * contracted impressions.
			 * 
			 * for now, a single entry per active campaign is added for queries
			 * of matching target segment.
			 */

			if ((dayBiddingFor >= campaign.dayStart)
					&& (dayBiddingFor <= campaign.dayEnd)
					&& (campaign.impsTogo() >= 0)) {

				int entCount = 0;
				for (int i = 0; i < queries.length; i++) {

					Set<MarketSegment> segmentsList = queries[i].getMarketSegments();

					for (MarketSegment marketSegment : segmentsList) {
						for(MarketSegment campaignSegment: campaign.targetSegment) {
							if (campaignSegment == marketSegment) {
								/*
								 * among matching entries with the same campaign id,
								 * the AdX randomly chooses an entry according to
								 * the designated weight. by setting a constant
								 * weight 1, we create a uniform probability over
								 * active campaigns
								 */
								++entCount;
								bidBundle.addQuery(queries[i], rbid, new Ad(null),
										campaign.id, 1);
							}
						}
					}
					
					if (segmentsList.size() == 0) {
						++entCount;
						bidBundle.addQuery(queries[i], rbid, new Ad(null),
								campaign.id, 1);
					}
				}
				double impressionLimit = 0.5 * campaign.impsTogo();
				double budgetLimit = 0.8 * Math.max(0, campaign.budget
						- campaign.stats.getCost());
				bidBundle.setCampaignDailyLimit(campaign.id,
						(int) impressionLimit, budgetLimit);
				entrySum += entCount;
				log.info("Day " + day + ": Updated " + entCount
						+ " Bid Bundle entries for Campaign id " + campaign.id);
			}
		}

		if (bidBundle != null) {
			log.info("Day " + day + ": Sending BidBundle");
			sendMessage(adxAgentAddress, bidBundle);
		}
	}

	/**
	 * Campaigns performance w.r.t. each allocated campaign
	 */
	private void handleCampaignReport(CampaignReport campaignReport) {

		campaignReports.add(campaignReport);

		/*
		 * for each campaign, the accumulated statistics from day 1 up to day
		 * n-1 are reported
		 */
		for (CampaignReportKey campaignKey : campaignReport.keys()) {
			int cmpId = campaignKey.getCampaignId();
			CampaignStats cstats = campaignReport.getCampaignReportEntry(
					campaignKey).getCampaignStats();
			myCampaigns.get(cmpId).setStats(cstats);

			log.info("Day " + day + ": Updating campaign " + cmpId + " stats: "
					+ cstats.getTargetedImps() + " tgtImps "
					+ cstats.getOtherImps() + " nonTgtImps. Cost of imps is "
					+ cstats.getCost());
		}
	}


	void resetSegmentValues() {
		Double zero = Double.valueOf(0.0d);
		segmentValues.put(MarketSegment.YOUNG, zero);
		segmentValues.put(MarketSegment.OLD, zero);
		segmentValues.put(MarketSegment.LOW_INCOME, zero);
		segmentValues.put(MarketSegment.HIGH_INCOME, zero);
		segmentValues.put(MarketSegment.MALE, zero);
		segmentValues.put(MarketSegment.FEMALE, zero);
	}

	/**
	 * Users and Publishers statistics: popularity and ad type orientation
	 */
	private void handleAdxPublisherReport(AdxPublisherReport adxPublisherReport) {
		log.info("Publishers Report: ");
		resetSegmentValues();
//		segmentValues.put(MarketSegment.YOUNG, 0.0d);
		for (PublisherCatalogEntry publisherKey : adxPublisherReport.keys()) {
			AdxPublisherReportEntry entry = adxPublisherReport.getEntry(publisherKey);
			String name = entry.getPublisherName();
			int popularity = entry.getPopularity();
			Double youngSegmentValue = segmentValues.get(MarketSegment.YOUNG);
			Double oldSegmentValue = segmentValues.get(MarketSegment.OLD);
			Double lowSegmentValue = segmentValues.get(MarketSegment.LOW_INCOME);
			Double highSegmentValue = segmentValues.get(MarketSegment.HIGH_INCOME);
			Double maleSegmentValue = segmentValues.get(MarketSegment.MALE);
			Double femaleSegmentValue = segmentValues.get(MarketSegment.FEMALE);
			if(ageProbabilityMap.containsKey(name)) {
				double newValue = youngSegmentValue.doubleValue() + popularity * 0.01 * ageProbabilityMap.get(name).doubleValue();
				Double newYoung = Double.valueOf(newValue);
				segmentValues.put(MarketSegment.YOUNG, newYoung);
				newValue = oldSegmentValue.doubleValue() + popularity * 0.01 * (1 - ageProbabilityMap.get(name).doubleValue());
				Double newOld = Double.valueOf(newValue);
				segmentValues.put(MarketSegment.OLD, newOld);
				newValue = lowSegmentValue.doubleValue() + popularity * 0.01 * incomeProbabilityMap.get(name).doubleValue();
				Double newLow = Double.valueOf(newValue);
				segmentValues.put(MarketSegment.LOW_INCOME, newLow);
				newValue = highSegmentValue.doubleValue() + popularity * 0.01 * (1 - incomeProbabilityMap.get(name).doubleValue());
				Double newHigh = Double.valueOf(newValue);
				segmentValues.put(MarketSegment.HIGH_INCOME, newHigh);
				newValue = maleSegmentValue.doubleValue() + popularity * 0.01 * genderProbabilityMap.get(name).doubleValue();
				Double newMale = Double.valueOf(newValue);
				segmentValues.put(MarketSegment.MALE, newMale);
				newValue = femaleSegmentValue.doubleValue() + popularity * 0.01 * (1 - genderProbabilityMap.get(name).doubleValue());
				Double newFemale = Double.valueOf(newValue);
				segmentValues.put(MarketSegment.FEMALE, newFemale);
			}
			else {
				double newValue = youngSegmentValue.doubleValue() + popularity * 0.5;
				Double newYoung = Double.valueOf(newValue);
				segmentValues.put(MarketSegment.YOUNG, newYoung);
				newValue = oldSegmentValue.doubleValue() + popularity * 0.5;
				Double newOld = Double.valueOf(newValue);
				segmentValues.put(MarketSegment.OLD, newOld);
				newValue = lowSegmentValue.doubleValue() + popularity * 0.5;
				Double newLow = Double.valueOf(newValue);
				segmentValues.put(MarketSegment.LOW_INCOME, newLow);
				newValue = highSegmentValue.doubleValue() + popularity * 0.5;
				Double newHigh = Double.valueOf(newValue);
				segmentValues.put(MarketSegment.HIGH_INCOME, newHigh);
				newValue = maleSegmentValue.doubleValue() + popularity * 0.5;
				Double newMale = Double.valueOf(newValue);
				segmentValues.put(MarketSegment.MALE, newMale);
				newValue = femaleSegmentValue.doubleValue() + popularity * 0.5;
				Double newFemale = Double.valueOf(newValue);
				segmentValues.put(MarketSegment.FEMALE, newFemale);
			}
			log.info(entry.toString());
		}
	}

	/**
	 * 
	 * @param AdNetworkReport
	 */
	private void handleAdNetworkReport(AdNetworkReport adnetReport) {
		
		log.info("Day "+ day + " : AdNetworkReport");
		/*
		 for (AdNetworkKey adnetKey : adnetReport.keys()) {
		 
			double rnd = Math.random();
			if (rnd > 0.95) {
				AdNetworkReportEntry entry = adnetReport
						.getAdNetworkReportEntry(adnetKey);
				log.info(adnetKey + " " + entry);
			}
		}
        */
	}

	@Override
	protected void simulationSetup() {
		randomGenerator = new Random();
		day = 0;
		bidBundle = new AdxBidBundle();
		ucsTargetLevel = 0.5 + (randomGenerator.nextInt(5) + 1) / 10.0;
		
		/* initial bid between 0.1 and 0.2 */
		ucsBid = 0.1 + 0.1*randomGenerator.nextDouble();
		
		myCampaigns = new HashMap<Integer, CampaignData>();
		log.fine("AdNet " + getName() + " simulationSetup");
	}

	@Override
	protected void simulationFinished() {
		campaignReports.clear();
		bidBundle = null;
	}

	/**
	 * A user visit to a publisher's web-site results in an impression
	 * opportunity (a query) that is characterized by the the publisher, the
	 * market segment the user may belongs to, the device used (mobile or
	 * desktop) and the ad type (text or video).
	 * 
	 * An array of all possible queries is generated here, based on the
	 * publisher names reported at game initialization in the publishers catalog
	 * message
	 */
	private void generateAdxQuerySpace() {
		if (publisherCatalog != null && queries == null) {
			Set<AdxQuery> querySet = new HashSet<AdxQuery>();

			/*
			 * for each web site (publisher) we generate all possible variations
			 * of device type, ad type, and user market segment
			 */
			for (PublisherCatalogEntry publisherCatalogEntry : publisherCatalog) {
				String publishersName = publisherCatalogEntry
						.getPublisherName();
				for (MarketSegment userSegment : MarketSegment.values()) {
					Set<MarketSegment> singleMarketSegment = new HashSet<MarketSegment>();
					singleMarketSegment.add(userSegment);

					querySet.add(new AdxQuery(publishersName,
							singleMarketSegment, Device.mobile, AdType.text));

					querySet.add(new AdxQuery(publishersName,
							singleMarketSegment, Device.pc, AdType.text));

					querySet.add(new AdxQuery(publishersName,
							singleMarketSegment, Device.mobile, AdType.video));

					querySet.add(new AdxQuery(publishersName,
							singleMarketSegment, Device.pc, AdType.video));

				}
				
				/**
				 * An empty segments set is used to indicate the "UNKNOWN" segment
				 * such queries are matched when the UCS fails to recover the user's
				 * segments.
				 */
				querySet.add(new AdxQuery(publishersName,
						new HashSet<MarketSegment>(), Device.mobile,
						AdType.video));
				querySet.add(new AdxQuery(publishersName,
						new HashSet<MarketSegment>(), Device.mobile,
						AdType.text));
				querySet.add(new AdxQuery(publishersName,
						new HashSet<MarketSegment>(), Device.pc, AdType.video));
				querySet.add(new AdxQuery(publishersName,
						new HashSet<MarketSegment>(), Device.pc, AdType.text));
			}
			queries = new AdxQuery[querySet.size()];
			querySet.toArray(queries);
		}
	}

	private class CampaignData {
		/* campaign attributes as set by server */
		Long reachImps;
		long dayStart;
		long dayEnd;
		Set<MarketSegment> targetSegment;
		double videoCoef;
		double mobileCoef;
		int id;

		/* campaign info as reported */
		CampaignStats stats;
		double budget;

		public CampaignData(InitialCampaignMessage icm) {
			reachImps = icm.getReachImps();
			dayStart = icm.getDayStart();
			dayEnd = icm.getDayEnd();
			targetSegment = icm.getTargetSegment(); // FIXME hack from interface mismatch
			videoCoef = icm.getVideoCoef();
			mobileCoef = icm.getMobileCoef();
			id = icm.getId();

			stats = new CampaignStats(0, 0, 0);
			budget = 0.0;
		}

		public void setBudget(double d) {
			budget = d;
		}

		public CampaignData(CampaignOpportunityMessage com) {
			dayStart = com.getDayStart();
			dayEnd = com.getDayEnd();
			id = com.getId();
			reachImps = com.getReachImps();
			targetSegment = com.getTargetSegment(); // FIXME hack from interface mismatch
			mobileCoef = com.getMobileCoef();
			videoCoef = com.getVideoCoef();
			stats = new CampaignStats(0, 0, 0);
			budget = 0.0;
		}

		@Override
		public String toString() {
			String targetString = "";
			for(MarketSegment segment: targetSegment) {
				targetString.concat(segment.name());
				targetString.concat(" ");
			}
			return "Campaign ID " + id + ": " + "day " + dayStart + " to "
					+ dayEnd + " " + targetString + ", reach: "
					+ reachImps + " coefs: (v=" + videoCoef + ", m="
					+ mobileCoef + ")";
		}

		int impsTogo() {
			return (int) Math.max(0, reachImps - stats.getTargetedImps());
		}

		void setStats(CampaignStats s) {
			stats.setValues(s);
		}

	}

}
