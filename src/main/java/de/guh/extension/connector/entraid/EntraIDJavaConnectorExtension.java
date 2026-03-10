package de.guh.extension.connector.entraid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.Logger;
import org.dom4j.Element;

import de.guh.connector.core.ConnectorRequest;
import de.guh.connector.core.ConnectorResponse;
import de.guh.connector.core.JavaConnectorExtension;
import de.guh.connector.core.NodeCollection;
import de.guh.connector.core.RelationCollection;
import de.guh.connector.core.connection.ConnectionManager;
import de.guh.connector.core.connection.ConnectionTestResult;
import de.guh.plugin.msgraph.MSGraphRequest;
import de.guh.plugin.msgraph.MSGraphResponse;
import de.guh.plugin.msgraph.MSGraphSession;
import de.guh.plugin.rules.RuleEvaluableString;
import de.guh.plugin.xml.XML;

public class EntraIDJavaConnectorExtension implements JavaConnectorExtension {

	/**
	 * Check connection to Entra ID
	 * Tests authentication and connection using ConnectionManager
	 * 
	 * @param thislog    Logger instance
	 * @param coRequest  Request with connection parameters
	 * @param coResponse Response object (not used for connection test)
	 * @return XML with connection test result
	 * @throws Exception if connection test fails
	 */
	@Override
	public XML checkConnection(Logger thislog, ConnectorRequest coRequest, ConnectorResponse coResponse) {
		thislog.info("checkConnection()...[running]");

		// Extract parameters from ConnectorRequest
		String authority = coRequest.getParameter("gp_param1");
		String tenant = coRequest.getParameter("gp_param2");
		String tenantname = coRequest.getParameter("gp_param3");
		String clientid = coRequest.getParameter("gp_param4");
		String scope = coRequest.getParameter("gp_param5");
		String connecteddomain = coRequest.getParameter("gp_param6");
		String proxyhost = coRequest.getParameter("gp_param7");
		String proxyport = coRequest.getParameter("gp_param8");
		String sslproxystring = coRequest.getParameter("gp_param9");
		String proxyusername = coRequest.getParameter("gp_param10");
		String secret = coRequest.getParameter("gp_pwparam1"); // Already decrypted by ConnectorRequest!
		String proxypassword = coRequest.getParameter("gp_pwparam2"); // Already decrypted!

		// Parse boolean
		boolean sslproxy = false;
		if (sslproxystring != null && sslproxystring.equalsIgnoreCase("true")) {
			sslproxy = true;
		}

		// Log parameters
		thislog.info("checkConnection()...[authority]: " + authority);
		thislog.info("checkConnection()...[tenant]: " + tenant);
		thislog.info("checkConnection()...[tenantname]: " + tenantname);
		thislog.info("checkConnection()...[clientid]: " + clientid);
		thislog.info("checkConnection()...[scope]: " + scope);
		thislog.info("checkConnection()...[connecteddomain]: " + connecteddomain);
		thislog.info("checkConnection()...[proxyhost]: " + proxyhost);
		thislog.info("checkConnection()...[proxyport]: " + proxyport);
		thislog.info("checkConnection()...[sslproxy]: " + sslproxy);
		thislog.info("checkConnection()...[proxyusername]: " + proxyusername);
		thislog.info("checkConnection()...[secret]: successfully read");
		thislog.info("checkConnection()...[proxypassword]: successfully read");

		// Build connection parameters for ConnectionManager
		Map<String, String> connectionParams = extractConnectionParams(coRequest);

		// Test via ConnectionManager
		thislog.info("checkConnection()...testing connection via ConnectionManager");
		ConnectionTestResult result =
				ConnectionManager.getInstance().testConnection(connectionParams, MSGraphManagedConnection.class, new MSGraphConnectionFactory());

		// Build XML response
		XML xmlConnection = new XML("connection");

		if (result.isSuccess()) {
			thislog.info("checkConnection()...[SUCCESS] Successfully connected to Entra ID");
			xmlConnection.appendNode("result", "SUCCESS");
			xmlConnection.appendNode("resultcode", "success");
			xmlConnection.appendNode("connected", "true");
			xmlConnection.appendNode("duration", String.valueOf(result.getDuration().toMillis()));
		} else {
			thislog.error("checkConnection()...[ERROR] Unable to connect to Entra ID: " + result.getMessage());
			xmlConnection.appendNode("result", "ERROR");
			xmlConnection.appendNode("resultcode", result.getMessage());
			xmlConnection.appendNode("connected", "false");
			xmlConnection.appendNode("duration", String.valueOf(result.getDuration().toMillis()));

			// Add exception details if available
			if (result.getException() != null) {
				xmlConnection.appendNode("error", result.getException().getMessage());
			}
		}

		return xmlConnection;
	}

	/**
	 * Get preview data from Entra ID
	 * Returns XML with limited data set for frontend preview
	 * NOW WITH SUBQUERY SUPPORT!
	 * 
	 * @param thislog    Logger instance
	 * @param coRequest  Request with connection and preview parameters
	 * @param coResponse Response object (not used for preview)
	 * @return XML with preview data (max 10 entries)
	 * @throws Exception if preview fails
	 */
	@Override
	public XML getNodePreview(Logger thislog, ConnectorRequest coRequest, ConnectorResponse coResponse) throws Exception {

		thislog.info("getNodePreview()...[running]");

		// Extract connection parameters (same as getNodeData)
		String authority = coRequest.getParameter("gp_param1");
		String tenant = coRequest.getParameter("gp_param2");
		String tenantname = coRequest.getParameter("gp_param3");
		String clientid = coRequest.getParameter("gp_param4");
		String scope = coRequest.getParameter("gp_param5");
		String connecteddomain = coRequest.getParameter("gp_param6");
		String proxyhost = coRequest.getParameter("gp_param7");
		String proxyport = coRequest.getParameter("gp_param8");
		String sslproxystring = coRequest.getParameter("gp_param9");
		boolean sslproxy = false;
		if (sslproxystring != null && sslproxystring.equalsIgnoreCase("true"))
			sslproxy = true;
		String proxyusername = coRequest.getParameter("gp_param10");
		String secret = coRequest.getParameter("gp_pwparam1");
		String proxypassword = coRequest.getParameter("gp_pwparam2");

		// Log connection parameters
		thislog.info("getNodePreview()...[authority]: " + authority);
		thislog.info("getNodePreview()...[tenant]: " + tenant);
		thislog.info("getNodePreview()...[tenantname]: " + tenantname);
		thislog.info("getNodePreview()...[clientid]: " + clientid);
		thislog.info("getNodePreview()...[scope]: " + scope);
		thislog.info("getNodePreview()...[connecteddomain]: " + connecteddomain);
		thislog.info("getNodePreview()...[proxyhost]: " + proxyhost);
		thislog.info("getNodePreview()...[proxyport]: " + proxyport);
		thislog.info("getNodePreview()...[sslproxy]: " + sslproxy);
		thislog.info("getNodePreview()...[proxyusername]: " + proxyusername);
		thislog.info("getNodePreview()...[secret]: successfully read");
		thislog.info("getNodePreview()...[proxypassword]: successfully read");

		// Extract node collection parameters (same as getNodeData)
		String objecttype = coRequest.getParameter("cp_param1");
		String resourcepath = coRequest.getParameter("cp_param2");
		String uniqueattribute = coRequest.getParameter("cp_param3");
		String select = coRequest.getParameter("cp_param4");
		String filter = coRequest.getParameter("cp_param5");
		String expand = coRequest.getParameter("cp_param6");
		String resourcepathsubquery = coRequest.getParameter("cp_param7");
		String resourcepathsubqueryselect = coRequest.getParameter("cp_param8");
		String resourcepathsubqueryfilter = coRequest.getParameter("cp_param9");
		String subqueryattributeprefix = coRequest.getParameter("cp_param10");

		// Log collection parameters
		thislog.info("getNodePreview()...[objecttype]: " + objecttype);
		thislog.info("getNodePreview()...[resourcepath]: " + resourcepath);
		thislog.info("getNodePreview()...[uniqueattribute]: " + uniqueattribute);
		thislog.info("getNodePreview()...[select]: " + select);
		thislog.info("getNodePreview()...[filter]: " + filter);
		thislog.info("getNodePreview()...[expand]: " + expand);
		thislog.info("getNodePreview()...[resourcepathsubquery]: " + resourcepathsubquery);
		thislog.info("getNodePreview()...[resourcepathsubqueryselect]: " + resourcepathsubqueryselect);
		thislog.info("getNodePreview()...[resourcepathsubqueryfilter]: " + resourcepathsubqueryfilter);
		thislog.info("getNodePreview()...[subqueryattributeprefix]: " + subqueryattributeprefix);

		// Get session from pool (same as getNodeData)
		MSGraphSession session = getOrCreateSession(coRequest);

		if (!session.isReady()) {
			thislog.error("getNodePreview()...[ERROR] Unable to connect to Entra ID");

			// Return error XML
			XML xmlError = new XML("entries");
			xmlError.appendNode("error", "Unable to connect to Entra ID: " + session.getStatusmessage());
			return xmlError;
		}

		// Build request (same as getNodeData)
		MSGraphRequest request = new MSGraphRequest(MSGraphRequest.type_GET, resourcepath);

		if (filter != null && !filter.equals("")) {
			request.addParameter("$filter", filter);
		}

		if (expand != null && !expand.equals("")) {
			request.addParameter("$expand", expand);
		}

		if (select != null && !select.equals("")) {
			request.addParameter("$select", select);
		}

		// PREVIEW SPECIFIC: Limit to 10 results
		request.addParameter("$top", "10");

		thislog.info("getNodePreview()...Sending request (limited to 10 results)");
		MSGraphResponse response = sendRequestWithRetry(session, request, coRequest, thislog);

		// CHANGED: Pass session and coRequest for subquery support
		XML xmlPreview = renderNodePreviewResponse(thislog, session, response, coRequest, objecttype, uniqueattribute, select);

		thislog.info("getNodePreview()...[SUCCESS] Preview data retrieved");

		return xmlPreview;
	}

	/**
	 * Renders MSGraph response data as XML for preview
	 * Similar to renderNodeResponse but creates XML instead of CSV
	 * NOW WITH SUBQUERY SUPPORT!
	 * 
	 * @param thislog         Logger instance
	 * @param session         MSGraph session (needed for subquery)
	 * @param response        MSGraph API response
	 * @param coRequest       Connector request (needed for subquery parameters)
	 * @param objecttype      Type of object (user, group, etc.)
	 * @param uniqueattribute Unique identifier attribute name
	 * @param select          Selected attributes (comma-separated)
	 * @return XML with preview entries (max 10)
	 */
	private XML renderNodePreviewResponse(Logger thislog, MSGraphSession session, MSGraphResponse response, ConnectorRequest coRequest, String objecttype,
			String uniqueattribute, String select) {

		thislog.debug("renderNodePreviewResponse()...objecttype: " + objecttype);

		XML xmlEntries = new XML("entries");

		// Get XML data from response (same as in renderNodeResponse!)
		XML responsexmlfull = response.getDataAsXML();

		if (responsexmlfull == null || !responsexmlfull.hasNodes()) {
			thislog.info("renderNodePreviewResponse()...no data returned");
			return xmlEntries;
		}

		// Count elements
		long elementCount = responsexmlfull.getElementStream().count();
		thislog.info("renderNodePreviewResponse()...processing " + elementCount +
			" entries");

		// Reset stream (count() consumes it)
		responsexmlfull = response.getDataAsXML();

		// Get subquery parameters (same as in renderNodeResponse)
		String resourcepathsubquery = coRequest.getParameter("cp_param7");
		String resourcepathsubqueryselect = coRequest.getParameter("cp_param8");
		String resourcepathsubqueryfilter = coRequest.getParameter("cp_param9");
		String subqueryattributeprefix = coRequest.getParameter("cp_param10");

		// Split select string to array
		String[] selectarr = select.split(",");

		// Counter for limit (max 10)
		AtomicInteger counter = new AtomicInteger(0);

		// Process each element (limit to 10)
		responsexmlfull.getElementStream().forEach((Element p) -> {

			// Stop after 10 entries
			if (counter.get() >= 10) {
				return;
			}

			XML xmlElement = new XML(p);
			XML xmlEntry = new XML("entry");

			// Get unique attribute value
			String uniqueattributevalue = xmlElement.findNode(uniqueattribute).getText();

			if (uniqueattributevalue != null && !uniqueattributevalue.equals("")) {

				thislog.debug("renderNodePreviewResponse()...processing preview entry: " + uniqueattribute +
					"=" +
					uniqueattributevalue);

				// Add unique attribute first
				xmlEntry.appendNode(uniqueattribute, uniqueattributevalue);

				// Add all selected attributes
				for (int i = 0; i < selectarr.length; i++) {

					if (!selectarr[i].equalsIgnoreCase(uniqueattribute)) {
						String value = xmlElement.findNode(selectarr[i]).getText();

						if (value == null) {
							value = "";
						}

						// Clean value (same as in renderNodeResponse)
						value = value.replace("\n", "").replace("\r", "");

						// Add to entry
						xmlEntry.appendNode(selectarr[i], value);
					}
				}

				if (resourcepathsubquery != null && !resourcepathsubquery.equals("")) {

					String finalresourcepathsubquery = resourcepathsubquery.replace("$id", uniqueattributevalue);

					thislog.debug("renderNodePreviewResponse()...  Executing subquery for preview: " + finalresourcepathsubquery);

					MSGraphRequest subqueryrequest = new MSGraphRequest(MSGraphRequest.type_GET, finalresourcepathsubquery);

					if (resourcepathsubqueryselect != null && !resourcepathsubqueryselect.equals("")) {
						subqueryrequest.addParameter("$select", resourcepathsubqueryselect);
					}

					if (resourcepathsubqueryfilter != null && !resourcepathsubqueryfilter.equals("")) {
						subqueryrequest.addParameter("$filter", resourcepathsubqueryfilter);
					}

					try {
						MSGraphResponse subqueryresponse = sendRequestWithRetry(session, subqueryrequest, coRequest, thislog);

						// Add subquery data to xmlEntry
						addSubqueryDataToPreviewEntry(thislog, subqueryresponse, xmlEntry, subqueryattributeprefix);

						thislog.debug("renderNodePreviewResponse()...  Subquery data added to preview entry");

					} catch (Exception e) {
						thislog.error("renderNodePreviewResponse()...subquery failed for preview entry, skipping subquery data: " + e.getMessage());
						// Continue without subquery data (entry still valid)
					}
				}

				// Add entry to entries
				xmlEntries.appendXML(xmlEntry);

				// Increment counter
				counter.incrementAndGet();
			}
		});

		thislog.info("renderNodePreviewResponse()...created XML with " + counter.get() +
			" entries");

		return xmlEntries;
	}

//	/**
//	 * Adds subquery response data to a preview entry XML
//	 * Helper method for renderPreviewResponse
//	 * 
//	 * @param thislog                 Logger instance
//	 * @param subqueryresponse        Subquery response from MSGraph
//	 * @param xmlEntry                The entry XML to add data to
//	 * @param subqueryattributeprefix Prefix for subquery attributes
//	 */
//	private void addSubqueryDataToPreviewEntry(Logger thislog, MSGraphResponse subqueryresponse, XML xmlEntry, String subqueryattributeprefix) {
//
//		XML subqueryresponsexml = subqueryresponse.getDataAsXML();
//
//		// Count subquery elements
//		long subqueryElementCount = subqueryresponsexml.getElementStream().count();
//		thislog.debug("  Subquery returned " + subqueryElementCount +
//			" elements");
//
//		// Reset stream
//		subqueryresponsexml = subqueryresponse.getDataAsXML();
//
//		subqueryresponsexml.getElementStream().forEach((Element p) -> {
//
//			XML xml = new XML(p);
//
//			// Check if this is a metadata element (like odata.context)
//			if (!xml.getElement().getName().equalsIgnoreCase("value")) {
//				thislog.debug("    Skipping metadata element: " + xml.getElement().getName());
//				return; // Skip metadata
//			}
//
//			if (xml.hasNodes()) {
//				String key = xml.getFirstChild().getElement().getName();
//				String value = xml.getFirstChild().getText();
//
//				if (value == null) {
//					value = "";
//				}
//
//				// Clean value
//				value = value.replace("\n", "").replace("\r", "");
//
//				thislog.debug("    Adding subquery attribute to preview: " + subqueryattributeprefix +
//					key +
//					"=" +
//					value);
//
//				// Add to XML entry
//				xmlEntry.appendNode(subqueryattributeprefix + key, value);
//			}
//		});
//	}

	/**
	 * Get relation preview data from Entra ID
	 * Returns XML with limited data set for frontend preview
	 * Shows source-target relationships with multi-value collapse
	 * 
	 * @param thislog    Logger instance
	 * @param coRequest  Request with connection and preview parameters
	 * @param coResponse Response object (not used for preview)
	 * @return XML with preview data (max 10 entries)
	 * @throws Exception if preview fails
	 */
	@Override
	public XML getRelationPreview(Logger thislog, ConnectorRequest coRequest, ConnectorResponse coResponse) throws Exception {

		thislog.info("getRelationPreview()...[running]");

		// Extract connection parameters (same as getNodePreview)
		String authority = coRequest.getParameter("gp_param1");
		String tenant = coRequest.getParameter("gp_param2");
		String tenantname = coRequest.getParameter("gp_param3");
		String clientid = coRequest.getParameter("gp_param4");
		String scope = coRequest.getParameter("gp_param5");
		String connecteddomain = coRequest.getParameter("gp_param6");
		String proxyhost = coRequest.getParameter("gp_param7");
		String proxyport = coRequest.getParameter("gp_param8");
		String sslproxystring = coRequest.getParameter("gp_param9");
		boolean sslproxy = false;
		if (sslproxystring != null && sslproxystring.equalsIgnoreCase("true"))
			sslproxy = true;
		String proxyusername = coRequest.getParameter("gp_param10");
		String secret = coRequest.getParameter("gp_pwparam1");
		String proxypassword = coRequest.getParameter("gp_pwparam2");

		// Log connection parameters
		thislog.info("getRelationPreview()...[authority]: " + authority);
		thislog.info("getRelationPreview()...[tenant]: " + tenant);
		thislog.info("getRelationPreview()...[tenantname]: " + tenantname);
		thislog.info("getRelationPreview()...[clientid]: " + clientid);
		thislog.info("getRelationPreview()...[scope]: " + scope);
		thislog.info("getRelationPreview()...[connecteddomain]: " + connecteddomain);
		thislog.info("getRelationPreview()...[proxyhost]: " + proxyhost);
		thislog.info("getRelationPreview()...[proxyport]: " + proxyport);
		thislog.info("getRelationPreview()...[sslproxy]: " + sslproxy);
		thislog.info("getRelationPreview()...[proxyusername]: " + proxyusername);
		thislog.info("getRelationPreview()...[secret]: successfully read");
		thislog.info("getRelationPreview()...[proxypassword]: successfully read");

		String objecttype = coRequest.getParameter("cp_param1");
		String resourcepath = coRequest.getParameter("cp_param2");
		String select = coRequest.getParameter("cp_param3");
		String sourceuniqueattribute = coRequest.getParameter("cp_param4");
		String filter = coRequest.getParameter("cp_param5");
		String resourcepathsubquery = coRequest.getParameter("cp_param6");
		String resourcepathsubqueryselect = coRequest.getParameter("cp_param7");
		String resourcepathsubqueryfilter = coRequest.getParameter("cp_param8");
		String subqueryattributeprefix = coRequest.getParameter("cp_param9");

		// Log collection parameters
		thislog.info("getRelationPreview()...[objecttype]: " + objecttype);
		thislog.info("getRelationPreview()...[resourcepath]: " + resourcepath);
		thislog.info("getRelationPreview()...[select]: " + select);
		thislog.info("getRelationPreview()...[sourceuniqueattribute]: " + sourceuniqueattribute);
		thislog.info("getRelationPreview()...[filter]: " + filter);
		thislog.info("getRelationPreview()...[resourcepathsubquery]: " + resourcepathsubquery);
		thislog.info("getRelationPreview()...[resourcepathsubqueryselect]: " + resourcepathsubqueryselect);
		thislog.info("getRelationPreview()...[resourcepathsubqueryfilter]: " + resourcepathsubqueryfilter);
		thislog.info("getRelationPreview()...[subqueryattributeprefix]: " + subqueryattributeprefix);

		// Get session from pool (same as getNodePreview)
		MSGraphSession session = getOrCreateSession(coRequest);

		if (!session.isReady()) {
			thislog.error("getRelationPreview()...[ERROR] Unable to connect to Entra ID");

			// Return error XML
			XML xmlError = new XML("entries");
			xmlError.appendNode("error", "Unable to connect to Entra ID: " + session.getStatusmessage());
			return xmlError;
		}

		// Build request (same as getNodePreview)
		MSGraphRequest request = new MSGraphRequest(MSGraphRequest.type_GET, resourcepath);

		if (filter != null && !filter.equals("")) {
			request.addParameter("$filter", filter);
		}

		if (select != null && !select.equals("")) {
			request.addParameter("$select", select);
		}

		// PREVIEW SPECIFIC: Limit to 10 results
		request.addParameter("$top", "10");

		thislog.info("getRelationPreview()...Sending request (limited to 10 results)");
		MSGraphResponse response = sendRequestWithRetry(session, request, coRequest, thislog);

		// Render relation preview response (with multi-value collapse)
		XML xmlPreview = renderRelationPreviewResponse(thislog, session, response, coRequest, objecttype, sourceuniqueattribute, select, resourcepathsubquery,
				resourcepathsubqueryselect, resourcepathsubqueryfilter, subqueryattributeprefix);

		thislog.info("getRelationPreview()...[SUCCESS] Preview data retrieved");

		return xmlPreview;
	}

	/**
	 * Renders MSGraph response data as XML for relation preview
	 * Shows source-target relationships with multi-value collapse
	 * One entry per source object (with collapsed target information)
	 * 
	 * @param thislog                    Logger instance
	 * @param session                    MSGraph session (needed for subquery)
	 * @param response                   MSGraph API response (source objects)
	 * @param coRequest                  Connector request (needed for subquery
	 *                                       parameters)
	 * @param objecttype                 Type of object (user, group, etc.)
	 * @param sourceuniqueattribute      Source identifier attribute name (e.g.
	 *                                       "id")
	 * @param select                     Selected source attributes
	 *                                       (comma-separated)
	 * @param resourcepathsubquery       Subquery path (e.g. "/users/$id/memberOf")
	 * @param resourcepathsubqueryselect Subquery select (e.g. "id,displayName")
	 * @param resourcepathsubqueryfilter Subquery filter
	 * @param subqueryattributeprefix    Prefix for target attributes (e.g.
	 *                                       "target-")
	 * @return XML with relation preview entries (max 10)
	 */
	private XML renderRelationPreviewResponse(Logger thislog, MSGraphSession session, MSGraphResponse response, ConnectorRequest coRequest, String objecttype,
			String sourceuniqueattribute, String select, String resourcepathsubquery, String resourcepathsubqueryselect, String resourcepathsubqueryfilter,
			String subqueryattributeprefix) {

		thislog.debug("renderRelationPreviewResponse()...objecttype: " + objecttype);

		XML xmlEntries = new XML("entries");

		// Get XML data from response (same as in renderNodePreviewResponse)
		XML responsexmlfull = response.getDataAsXML();

		if (responsexmlfull == null || !responsexmlfull.hasNodes()) {
			thislog.info("renderRelationPreviewResponse()...no data returned");
			return xmlEntries;
		}

		// Count elements
		long elementCount = responsexmlfull.getElementStream().count();
		thislog.info("renderRelationPreviewResponse()...processing " + elementCount +
			" source elements");

		// Reset stream (count() consumes it)
		responsexmlfull = response.getDataAsXML();

		// Split select string to array (for source attributes)
		String[] selectarr = select.split(",");

		// Counter for limit (max 10)
		AtomicInteger counter = new AtomicInteger(0);

		// Process each source element (limit to 10)
		responsexmlfull.getElementStream().forEach((Element p) -> {

			// Stop after 10 entries
			if (counter.get() >= 10) {
				return;
			}

			XML xmlElement = new XML(p);
			XML xmlEntry = new XML("entry");

			// Get source unique attribute value
			String sourceuniqueattributevalue = xmlElement.findNode(sourceuniqueattribute).getText();

			if (sourceuniqueattributevalue != null && !sourceuniqueattributevalue.equals("")) {

				thislog.debug("renderRelationPreviewResponse()...processing source: " + sourceuniqueattribute +
					"=" +
					sourceuniqueattributevalue);

				// Add source unique attribute first
				xmlEntry.appendNode(sourceuniqueattribute, sourceuniqueattributevalue);

				// Add all selected source attributes
				for (int i = 0; i < selectarr.length; i++) {

					if (!selectarr[i].equalsIgnoreCase(sourceuniqueattribute)) {
						String value = xmlElement.findNode(selectarr[i]).getText();

						if (value == null) {
							value = "";
						}

						// Clean value
						value = value.replace("\n", "").replace("\r", "");

						// Add to entry
						xmlEntry.appendNode(selectarr[i], value);
					}
				}

				// Execute subquery to get target data (if configured)
				if (resourcepathsubquery != null && !resourcepathsubquery.equals("")) {

					String finalresourcepathsubquery = resourcepathsubquery.replace("$id", sourceuniqueattributevalue);

					thislog.debug("renderRelationPreviewResponse()...  Executing subquery: " + finalresourcepathsubquery);

					MSGraphRequest subqueryrequest = new MSGraphRequest(MSGraphRequest.type_GET, finalresourcepathsubquery);

					if (resourcepathsubqueryselect != null && !resourcepathsubqueryselect.equals("")) {
						subqueryrequest.addParameter("$select", resourcepathsubqueryselect);
					}

					if (resourcepathsubqueryfilter != null && !resourcepathsubqueryfilter.equals("")) {
						subqueryrequest.addParameter("$filter", resourcepathsubqueryfilter);
					}

					try {
						MSGraphResponse subqueryresponse = sendRequestWithRetry(session, subqueryrequest, coRequest, thislog);

						// Add collapsed target data to xmlEntry
						addCollapsedTargetDataToPreviewEntry(thislog, subqueryresponse, xmlEntry, resourcepathsubqueryselect, subqueryattributeprefix);

						thislog.debug("renderRelationPreviewResponse()...  Target data added to preview entry");

					} catch (Exception e) {
						thislog.error("renderRelationPreviewResponse()...subquery failed for source, skipping target data: " + e.getMessage());
						// Continue without target data (entry still valid with source data only)
					}
				}

				// Add entry to entries
				xmlEntries.appendXML(xmlEntry);

				// Increment counter
				counter.incrementAndGet();
			}
		});

		thislog.info("renderRelationPreviewResponse()...created XML with " + counter.get() +
			" entries");

		return xmlEntries;
	}

	/**
	 * Adds collapsed target data to a relation preview entry
	 * Shows count for multi-value relationships: "(N) - FirstValue"
	 * Helper method for renderRelationPreviewResponse
	 * 
	 * @param thislog                    Logger instance
	 * @param subqueryresponse           Subquery response from MSGraph (target
	 *                                       objects)
	 * @param xmlEntry                   The entry XML to add target data to
	 * @param resourcepathsubqueryselect Selected target attributes
	 *                                       (comma-separated)
	 * @param subqueryattributeprefix    Prefix for target attributes
	 */
	private void addCollapsedTargetDataToPreviewEntry(Logger thislog, MSGraphResponse subqueryresponse, XML xmlEntry, String resourcepathsubqueryselect,
			String subqueryattributeprefix) {

		XML subqueryresponsexml = subqueryresponse.getDataAsXML();

		// Count target elements
		long targetElementCount = subqueryresponsexml.getElementStream().count();
		thislog.debug("  Subquery returned " + targetElementCount +
			" target elements");

		// Reset stream
		subqueryresponsexml = subqueryresponse.getDataAsXML();

		// Collect all values per attribute (for multi-value detection)
		Map<String, List<String>> attributeValues = new HashMap<>();

		// Split select to know which attributes to extract
		String[] selectarr = resourcepathsubqueryselect.split(",");

		// Process all target elements
		subqueryresponsexml.getElementStream().forEach((Element p) -> {

			XML xml = new XML(p);

			// Skip metadata elements (like odata.context)
			if (!xml.getElement().getName().equalsIgnoreCase("value")) {
				thislog.debug("    Skipping metadata element: " + xml.getElement().getName());
				return;
			}

			// Extract all selected attributes from this target
			for (String attributeName : selectarr) {
				String value = xml.findNode(attributeName).getText();

				if (value == null) {
					value = "";
				}

				// Clean value
				value = value.replace("\n", "").replace("\r", "");

				// Add to collection with prefix
				String prefixedAttributeName = subqueryattributeprefix + attributeName;
				attributeValues.computeIfAbsent(prefixedAttributeName, k -> new ArrayList<>()).add(value);

				thislog.debug("      Collected: " + prefixedAttributeName +
					" = " +
					value);
			}
		});

		// Add to XML with multi-value indicator
		for (Map.Entry<String, List<String>> entry : attributeValues.entrySet()) {
			String attributeName = entry.getKey();
			List<String> values = entry.getValue();

			if (values != null && values.size() > 1) {
				// MULTI-VALUE: Show count and first value
				String displayValue = "(" + values.size() +
					") - " +
					values.get(0);
				thislog.debug("    Adding multi-value target attribute: " + attributeName +
					"=" +
					displayValue);
				xmlEntry.appendNode(attributeName, displayValue);

			} else if (values != null && values.size() == 1) {
				// SINGLE VALUE: Show value only
				thislog.debug("    Adding single-value target attribute: " + attributeName +
					"=" +
					values.get(0));
				xmlEntry.appendNode(attributeName, values.get(0));

			} else {
				// EMPTY: Add empty node
				thislog.debug("    Adding empty target attribute: " + attributeName);
				xmlEntry.appendNode(attributeName, "");
			}
		}

		thislog.debug("  Added " + attributeValues.size() +
			" target attributes to preview entry");
	}

	/**
	 * Adds subquery response data to a preview entry XML
	 * Handles multi-value attributes (shows count if multiple values exist)
	 * Processes ALL attributes from $select (not just first child)
	 * Helper method for renderPreviewResponse
	 * 
	 * @param thislog                 Logger instance
	 * @param subqueryresponse        Subquery response from MSGraph
	 * @param xmlEntry                The entry XML to add data to
	 * @param subqueryattributeprefix Prefix for subquery attributes
	 */
	private void addSubqueryDataToPreviewEntry(Logger thislog, MSGraphResponse subqueryresponse, XML xmlEntry, String subqueryattributeprefix) {

		XML subqueryresponsexml = subqueryresponse.getDataAsXML();

		// Count subquery elements
		long subqueryElementCount = subqueryresponsexml.getElementStream().count();
		thislog.debug("  Subquery returned " + subqueryElementCount +
			" elements");

		// Reset stream
		subqueryresponsexml = subqueryresponse.getDataAsXML();

		// Collect all values per attribute (for multi-value detection)
		Map<String, List<String>> attributeValues = new HashMap<>();

		// First pass: Collect all values from ALL child elements
		subqueryresponsexml.getElementStream().forEach((Element p) -> {

			XML xml = new XML(p);

			// Skip metadata elements (like odata.context)
			if (!xml.getElement().getName().equalsIgnoreCase("value")) {
				thislog.debug("    Skipping metadata element: " + xml.getElement().getName());
				return;
			}

			// Process ALL child nodes (not just first!)
			if (xml.hasNodes()) {
				@SuppressWarnings("unchecked")
				List<Element> children = xml.getElement().elements();

				thislog.debug("    Processing " + children.size() +
					" child elements in this value node");

				for (Element child : children) {
					String key = child.getName();
					String value = child.getText();

					if (value == null) {
						value = "";
					}

					// Clean value
					value = value.replace("\n", "").replace("\r", "");

					// Add to collection with prefix
					String attributeName = subqueryattributeprefix + key;
					attributeValues.computeIfAbsent(attributeName, k -> new ArrayList<>()).add(value);

					thislog.debug("      Collected: " + attributeName +
						" = " +
						value);
				}
			}
		});

		// Second pass: Add to XML with multi-value indicator
		for (Map.Entry<String, List<String>> entry : attributeValues.entrySet()) {
			String attributeName = entry.getKey();
			List<String> values = entry.getValue();

			if (values != null && values.size() > 1) {
				// MULTI-VALUE: Show count and first value
				String displayValue = "(" + values.size() +
					") - " +
					values.get(0);
				thislog.debug("    Adding multi-value subquery attribute: " + attributeName +
					"=" +
					displayValue);
				xmlEntry.appendNode(attributeName, displayValue);

			} else if (values != null && values.size() == 1) {
				// SINGLE VALUE: Show value only
				thislog.debug("    Adding single-value subquery attribute: " + attributeName +
					"=" +
					values.get(0));
				xmlEntry.appendNode(attributeName, values.get(0));

			} else {
				// EMPTY: Add empty node
				thislog.debug("    Adding empty subquery attribute: " + attributeName);
				xmlEntry.appendNode(attributeName, "");
			}
		}

		thislog.debug("  Added " + attributeValues.size() +
			" subquery attributes to preview entry");
	}

	@Override
	public boolean getNodeData(Logger thislog, NodeCollection nodecollection, ConnectorRequest coRequest, ConnectorResponse coResponse) throws Exception {

		String authority = coRequest.getParameter("gp_param1");
		String tenant = coRequest.getParameter("gp_param2");
		String tenantname = coRequest.getParameter("gp_param3");
		String clientid = coRequest.getParameter("gp_param4");
		String scope = coRequest.getParameter("gp_param5");
		String connecteddomain = coRequest.getParameter("gp_param6");
		String proxyhost = coRequest.getParameter("gp_param7");
		String proxyport = coRequest.getParameter("gp_param8");
		String sslproxystring = coRequest.getParameter("gp_param9");
		boolean sslproxy = false;
		if (sslproxystring != null && sslproxystring.equalsIgnoreCase("true"))
			sslproxy = true;
		String proxyusername = coRequest.getParameter("gp_param10");

		String secret = coRequest.getParameter("gp_pwparam1");
		String proxypassword = coRequest.getParameter("gp_pwparam2");

		thislog.info("phase_getnodedata()...[authority] : " + authority);
		thislog.info("phase_getnodedata()...[tenant] : " + tenant);
		thislog.info("phase_getnodedata()...[tenantname] : " + tenantname);
		thislog.info("phase_getnodedata()...[clientid] : " + clientid);
		thislog.info("phase_getnodedata()...[scope] : " + scope);
		thislog.info("phase_getnodedata()...[connecteddomain] : " + connecteddomain);
		thislog.info("phase_getnodedata()...[proxyhost] : " + proxyhost);
		thislog.info("phase_getnodedata()...[proxyport] : " + proxyport);
		thislog.info("phase_getnodedata()...[sslproxy] : " + sslproxy);
		thislog.info("phase_getnodedata()...[proxyusername] : " + proxyusername);
		thislog.info("phase_getnodedata()...[secret] : successfully read.");
		thislog.info("phase_getnodedata()...[proxypassword] : successfully read.");

		String objecttype = coRequest.getParameter("np_param1");
		String resourcepath = coRequest.getParameter("np_param2");
		String uniqueattribute = coRequest.getParameter("np_param3");
		String select = coRequest.getParameter("np_param4");
		String filter = coRequest.getParameter("np_param5");
		String expand = coRequest.getParameter("np_param6");
		String resourcepathsubquery = coRequest.getParameter("np_param7");
		String resourcepathsubqueryselect = coRequest.getParameter("np_param8");
		String resourcepathsubqueryfilter = coRequest.getParameter("np_param9");
		String subqueryattributeprefix = coRequest.getParameter("np_param10");

		thislog.info("phase_getnodedata()...[objecttype] : " + objecttype);
		thislog.info("phase_getnodedata()...[resourcepath] : " + resourcepath);
		thislog.info("phase_getnodedata()...[uniqueattribute] : " + uniqueattribute);
		thislog.info("phase_getnodedata()...[select] : " + select);
		thislog.info("phase_getnodedata()...[filter] : " + filter);
		thislog.info("phase_getnodedata()...[expand] : " + expand);
		thislog.info("phase_getnodedata()...[resourcepathsubquery] : " + resourcepathsubquery);
		thislog.info("phase_getnodedata()...[resourcepathsubqueryselect] : " + resourcepathsubqueryselect);
		thislog.info("phase_getnodedata()...[resourcepathsubqueryfilter] : " + resourcepathsubqueryfilter);
		thislog.info("phase_getnodedata()...[subqueryattributeprefix] : " + subqueryattributeprefix);

		thislog.debug("phase_getnodedata()...whitelistrule: " + nodecollection.getWhitelistrule());
		thislog.debug("phase_getnodedata()...blacklistrule: " + nodecollection.getBlacklistrule());
		thislog.debug("phase_getnodedata()...skipproperties: " + nodecollection.getSkipproperties());

		MSGraphSession session = getOrCreateSession(coRequest);

		if (session.isReady()) {

			MSGraphRequest request = new MSGraphRequest(MSGraphRequest.type_GET, resourcepath);

			if (filter != null && !filter.equals("")) {
				request.addParameter("$filter", filter);
			}

			if (expand != null && !expand.equals("")) {
				request.addParameter("$expand", expand);
			}

			if (select != null && !select.equals("")) {
				request.addParameter("$select", select);
			}

			thislog.debug("phase_getnodedata()...Sending Request for page 1,");
			MSGraphResponse response = sendRequestWithRetry(session, request, coRequest, thislog);

			renderNodeResponse(thislog, session, response, nodecollection, coResponse, coRequest);

			int i = 1;
			while (response.getNexturi() != null && !response.getNexturi().equals("")) {

				i++;
				thislog.debug("phase_getnodedata()...Sending Request for page " + i +
					"...");
				String nexturl = response.getNexturi();
				nexturl = nexturl.replace("https://graph.microsoft.com/v1.0/", "");

				MSGraphRequest requestsub = new MSGraphRequest(MSGraphRequest.type_GET, nexturl);
				response = sendRequestWithRetry(session, requestsub, coRequest, thislog);

				renderNodeResponse(thislog, session, response, nodecollection, coResponse, coRequest);
			}

		} else {
			thislog.error("phase_getnodedata()...[ERROR] Unable to connect to Entra ID.");
			throw new Exception(session.getStatusmessage());
		}

		return true;
		// TODO Auto-generated method stub
	}

	private void renderNodeResponse(Logger thislog, MSGraphSession session, MSGraphResponse response, NodeCollection nodecollection,
			ConnectorResponse coResponse, ConnectorRequest coRequest) {

		// String objecttype = coRequest.getParameter("np_param1");
		String uniqueattribute = coRequest.getParameter("np_param3");
		String select = coRequest.getParameter("np_param4");
		// String filter = coRequest.getParameter("np_param5");
		// String expand = coRequest.getParameter("np_param6");
		String resourcepathsubquery = coRequest.getParameter("np_param7");
		String resourcepathsubqueryselect = coRequest.getParameter("np_param8");
		String resourcepathsubqueryfilter = coRequest.getParameter("np_param9");
		// String subqueryattributeprefix = coRequest.getParameter("np_param10");

		boolean hasRules = nodecollection.hasRules();

		XML responsexmlfull = response.getDataAsXML();

		// Count total nodes for logging
		long nodeCount = responsexmlfull.getElementStream().count();
		thislog.info("Processing " + nodeCount +
			" node elements");

		// Reset stream
		responsexmlfull = response.getDataAsXML();

		AtomicReference<RuleEvaluableString> ruleEvaluableRef = new AtomicReference<>();
		responsexmlfull.getElementStream().forEach((Element p) -> {

			XML test = new XML(p);

			String uniqueattributevalue = test.findNode(uniqueattribute).getText();

			if (uniqueattributevalue != null && !uniqueattributevalue.equals("")) {

				thislog.debug("Processing node: " + uniqueattribute +
					"=" +
					uniqueattributevalue);

				// a valid node was found
				if (hasRules)
					ruleEvaluableRef.set(new RuleEvaluableString(coRequest.getNodename()));

				// it's just one query
				// Create line and add uniqueattribute first
				String line = "\"" + uniqueattribute +
					":" +
					uniqueattributevalue +
					"\"";
				if (hasRules)
					ruleEvaluableRef.get().addAttribute(uniqueattribute, uniqueattributevalue);

				String selectarr[] = select.split(",");

				for (int i = 0; i < selectarr.length; i++) {

					if (!selectarr[i].equalsIgnoreCase(uniqueattribute)) {
						String value = test.findNode(selectarr[i]).getText();

						if (value == null)
							value = "";

						line = nodecollection.addDataToLine(selectarr[i], value, line, ruleEvaluableRef.get());

					}

				}

				thislog.debug("  Base line created: " + line);

				if (resourcepathsubquery != null && !resourcepathsubquery.equals("")) {

					String finalresourcepathsubquery = resourcepathsubquery.replace("$id", uniqueattributevalue);

					thislog.debug("  Executing subquery: " + finalresourcepathsubquery);

					MSGraphRequest subqueryrequest = new MSGraphRequest(MSGraphRequest.type_GET, finalresourcepathsubquery);

					if (resourcepathsubqueryselect != null && !resourcepathsubqueryselect.equals("")) {
						subqueryrequest.addParameter("$select", resourcepathsubqueryselect);
					}

					if (resourcepathsubqueryfilter != null && !resourcepathsubqueryfilter.equals("")) {
						subqueryrequest.addParameter("$filter", resourcepathsubqueryfilter);
					}

					try {
						MSGraphResponse subqueryresponse = sendRequestWithRetry(session, subqueryrequest, coRequest, thislog);
						line = renderNodeSubqueryResponse(thislog, subqueryresponse, line, nodecollection, coResponse, coRequest, ruleEvaluableRef);
						thislog.debug("  Line after subquery: " + line);

					} catch (Exception e) {
						thislog.error("Subquery failed for node, skipping subquery data: " + e.getMessage());
					}

				}

				// DEBUG: Log what's in the RuleEvaluable before filtering
				if (hasRules && ruleEvaluableRef.get() != null) {
					thislog.debug("  RuleEvaluable attributes: " + ruleEvaluableRef.get().getAttributes());
					for (String attr : ruleEvaluableRef.get().getAttributes()) {
						List<String> values = ruleEvaluableRef.get().getAttributeValues(attr);
						thislog.debug("    " + attr +
							" = " +
							(values != null ? values : "null"));
					}
				}

				try {

					if (!hasRules || nodecollection.isAllowed(thislog, ruleEvaluableRef.get())) {
						coResponse.writeDataLine(line);
						thislog.debug("  Line written: " + line);
					} else {
						thislog.debug("  Line filtered by rules: " + line);
					}

					// coResponse.writeDataLine(line);
				} catch (IOException e) {
					thislog.error("Error writing node line", e);
					e.printStackTrace();
				}

			}

		});

	}

//	private String renderNodeSubqueryResponse(Logger thislog, MSGraphResponse subqueryresponse, String line, NodeCollection nodecollection,
//			ConnectorResponse coResponse, ConnectorRequest coRequest, AtomicReference<RuleEvaluableString> ruleEvaluableRef) {
//
//		String subqueryattributeprefix = coRequest.getParameter("np_param10");
//
//		XML subqueryresponsexml = subqueryresponse.getDataAsXML();
//
//		// Count subquery elements
//		long subqueryElementCount = subqueryresponsexml.getElementStream().count();
//		thislog.debug("  Subquery returned " + subqueryElementCount +
//			" elements");
//
//		// Reset stream
//		subqueryresponsexml = subqueryresponse.getDataAsXML();
//
//		AtomicReference<String> lineRef = new AtomicReference<>();
//		lineRef.set(line);
//
//		subqueryresponsexml.getElementStream().forEach((Element p) -> {
//
//			XML xml = new XML(p);
//
//			// Check if this is a metadata element (like odata.context)
//			if (!xml.getElement().getName().equalsIgnoreCase("value")) {
//				thislog.debug("    Skipping metadata element: " + xml.getElement().getName());
//				return; // Skip metadata
//			}
//
//			if (xml.hasNodes()) {
//				String key = xml.getFirstChild().getElement().getName();
//				String value = xml.getFirstChild().getText();
//
//				thislog.debug("    Adding subquery attribute: " + subqueryattributeprefix +
//					key +
//					"=" +
//					value);
//
//				lineRef.set(nodecollection.addDataToLine(subqueryattributeprefix + key, value, lineRef.get(), ruleEvaluableRef.get()));
//			}
//		});
//
//		return lineRef.get();
//	}

	/**
	 * Renders subquery response data and adds it to the current CSV line
	 * Processes ALL child elements from subquery response (not just first)
	 * 
	 * @param thislog          Logger instance
	 * @param subqueryresponse Subquery response from MSGraph
	 * @param line             Current CSV line to append data to
	 * @param nodecollection   Node collection for addDataToLine
	 * @param coResponse       Connector response (not used)
	 * @param coRequest        Connector request for parameters
	 * @param ruleEvaluableRef Rule evaluable reference for filtering
	 * @return Updated CSV line with subquery data
	 */
	private String renderNodeSubqueryResponse(Logger thislog, MSGraphResponse subqueryresponse, String line, NodeCollection nodecollection,
			ConnectorResponse coResponse, ConnectorRequest coRequest, AtomicReference<RuleEvaluableString> ruleEvaluableRef) {

		String subqueryattributeprefix = coRequest.getParameter("np_param10");

		XML subqueryresponsexml = subqueryresponse.getDataAsXML();

		// Count subquery elements
		long subqueryElementCount = subqueryresponsexml.getElementStream().count();
		thislog.debug("  Subquery returned " + subqueryElementCount +
			" elements");

		// Reset stream
		subqueryresponsexml = subqueryresponse.getDataAsXML();

		AtomicReference<String> lineRef = new AtomicReference<>();
		lineRef.set(line);

		subqueryresponsexml.getElementStream().forEach((Element p) -> {

			XML xml = new XML(p);

			// Check if this is a metadata element (like odata.context)
			if (!xml.getElement().getName().equalsIgnoreCase("value")) {
				thislog.debug("    Skipping metadata element: " + xml.getElement().getName());
				return; // Skip metadata
			}

			// Process ALL child nodes (not just first!)
			if (xml.hasNodes()) {
				@SuppressWarnings("unchecked")
				List<Element> children = xml.getElement().elements();

				thislog.debug("    Processing " + children.size() +
					" child elements in this value node");

				for (Element child : children) {
					String key = child.getName();
					String value = child.getText();

					if (value == null) {
						value = "";
					}

					thislog.debug("      Adding subquery attribute: " + subqueryattributeprefix +
						key +
						"=" +
						value);

					lineRef.set(nodecollection.addDataToLine(subqueryattributeprefix + key, value, lineRef.get(), ruleEvaluableRef.get()));
				}
			}
		});

		return lineRef.get();
	}

	@Override
	public boolean getRelationData(Logger thislog, RelationCollection relationcollection, ConnectorRequest coRequest, ConnectorResponse coResponse)
			throws Exception {

		String authority = coRequest.getParameter("gp_param1");
		String tenant = coRequest.getParameter("gp_param2");
		String tenantname = coRequest.getParameter("gp_param3");
		String clientid = coRequest.getParameter("gp_param4");
		String scope = coRequest.getParameter("gp_param5");
		String connecteddomain = coRequest.getParameter("gp_param6");
		String proxyhost = coRequest.getParameter("gp_param7");
		String proxyport = coRequest.getParameter("gp_param8");
		String sslproxystring = coRequest.getParameter("gp_param9");
		boolean sslproxy = false;
		if (sslproxystring != null && sslproxystring.equalsIgnoreCase("true"))
			sslproxy = true;
		String proxyusername = coRequest.getParameter("gp_param10");
		String secret = coRequest.getParameter("gp_pwparam1");
		String proxypassword = coRequest.getParameter("gp_pwparam2");

		thislog.info("phase_getrelationdata()...[authority] : " + authority);
		thislog.info("phase_getrelationdata()...[tenant] : " + tenant);
		thislog.info("phase_getrelationdata()...[tenantname] : " + tenantname);
		thislog.info("phase_getrelationdata()...[clientid] : " + clientid);
		thislog.info("phase_getrelationdata()...[scope] : " + scope);
		thislog.info("phase_getrelationdata()...[connecteddomain] : " + connecteddomain);
		thislog.info("phase_getrelationdata()...[proxyhost] : " + proxyhost);
		thislog.info("phase_getrelationdata()...[proxyport] : " + proxyport);
		thislog.info("phase_getrelationdata()...[sslproxy] : " + sslproxy);
		thislog.info("phase_getrelationdata()...[proxyusername] : " + proxyusername);
		thislog.info("phase_getrelationdata()...[secret] : successfully read.");
		thislog.info("phase_getrelationdata()...[proxypassword] : successfully read.");

		thislog.debug("phase_getrelationdata()...whitelistrule: " + relationcollection.getWhitelistrule());
		thislog.debug("phase_getrelationdata()...blacklistrule: " + relationcollection.getBlacklistrule());
		thislog.debug("phase_getrelationdata()...skipproperties: " + relationcollection.getSkipproperties());

		String objecttype = coRequest.getParameter("rp_param1");
		String resourcepath = coRequest.getParameter("rp_param2");
		String select = coRequest.getParameter("rp_param3");
		String sourceuniqueattribute = coRequest.getParameter("rp_param4");
		String filter = coRequest.getParameter("rp_param5");
		String resourcepathsubquery = coRequest.getParameter("rp_param6");
		String resourcepathsubqueryselect = coRequest.getParameter("rp_param7");
		String resourcepathsubqueryfilter = coRequest.getParameter("rp_param8");

		thislog.info("phase_getrelationdata()...[objecttype] : " + objecttype);
		thislog.info("phase_getrelationdata()...[resourcepath] : " + resourcepath);
		thislog.info("phase_getrelationdata()...[select] : " + select);
		thislog.info("phase_getrelationdata()...[sourceuniqueattribute] : " + sourceuniqueattribute);
		thislog.info("phase_getrelationdata()...[filter] : " + filter);
		thislog.info("phase_getrelationdata()...[resourcepathsubquery] : " + resourcepathsubquery);
		thislog.info("phase_getrelationdata()...[resourcepathsubqueryselect] : " + resourcepathsubqueryselect);
		thislog.info("phase_getrelationdata()...[resourcepathsubqueryfilter] : " + resourcepathsubqueryfilter);

		MSGraphSession session = getOrCreateSession(coRequest);

		if (session.isReady()) {

			MSGraphRequest request = new MSGraphRequest(MSGraphRequest.type_GET, resourcepath);

			if (filter != null && !filter.equals("")) {
				request.addParameter("$filter", filter);
			}

//			if (expand != null && !expand.equals("")) {
//				request.addParameter("$expand", expand);
//			}

			if (select != null && !select.equals("")) {
				request.addParameter("$select", select);
			}

			MSGraphResponse response = sendRequestWithRetry(session, request, coRequest, thislog);

			thislog.debug("phase_getrelationdata()...Sending Request for page 1...");

			renderRelationResponse(thislog, session, response, relationcollection, coResponse, coRequest);

			int i = 1;
			while (response.getNexturi() != null && !response.getNexturi().equals("")) {
				i++;
				thislog.debug("phase_getrelationdata()...Sending Request for page " + i +
					"...");
				String nexturl = response.getNexturi();
				nexturl = nexturl.replace("https://graph.microsoft.com/v1.0/", "");

				MSGraphRequest requestsub = new MSGraphRequest(MSGraphRequest.type_GET, nexturl);
				response = sendRequestWithRetry(session, requestsub, coRequest, thislog);

				renderRelationResponse(thislog, session, response, relationcollection, coResponse, coRequest);
			}

		}

		return true;
		// TODO Auto-generated method stub
	}

	private void renderRelationResponse(Logger thislog, MSGraphSession session, MSGraphResponse response, RelationCollection relationcollection,
			ConnectorResponse coResponse, ConnectorRequest coRequest) {

		// Read parameters from request
		String sourceuniqueattribute = coRequest.getParameter("rp_param4");
		String select = coRequest.getParameter("rp_param3");
		String resourcepathsubquery = coRequest.getParameter("rp_param6");
		String resourcepathsubqueryselect = coRequest.getParameter("rp_param7");
		String resourcepathsubqueryfilter = coRequest.getParameter("rp_param8");
		String subqueryattributeprefix = coRequest.getParameter("rp_param9");

		boolean hasRules = relationcollection.hasRules();

		XML xmlResponseFull = response.getDataAsXML();
		AtomicReference<RuleEvaluableString> ruleEvaluableRef = new AtomicReference<>();

		// Count total source elements for logging
		long sourceElementCount = xmlResponseFull.getElementStream().count();
		thislog.info("Processing " + sourceElementCount +
			" source elements (users)");

		// Reset stream (count() consumes it)
		xmlResponseFull = response.getDataAsXML();

		xmlResponseFull.getElementStream().forEach((Element p) -> {

			XML xmlElement = new XML(p);

			// Get source unique attribute value (e.g. user id)
			String sourceuniqueattributevalue = xmlElement.findNode(sourceuniqueattribute).getText();

			if (sourceuniqueattributevalue != null && !sourceuniqueattributevalue.equals("")) {

				thislog.debug("Processing source element: " + sourceuniqueattribute +
					"=" +
					sourceuniqueattributevalue);

				ruleEvaluableRef.set(new RuleEvaluableString(coRequest.getNodename()));

				// Build base line with source attributes
				AtomicReference<String> lineRef = new AtomicReference<>();
				lineRef.set("\"" + sourceuniqueattribute +
					":" +
					sourceuniqueattributevalue +
					"\"");
				ruleEvaluableRef.get().addAttribute(sourceuniqueattribute, sourceuniqueattributevalue);

				// Add other selected attributes from main query
				String[] selectarr = select.split(",");
				for (int i = 0; i < selectarr.length; i++) {
					if (!selectarr[i].equalsIgnoreCase(sourceuniqueattribute)) {
						String value = xmlElement.findNode(selectarr[i]).getText();
						if (value == null)
							value = "";
						lineRef.set(relationcollection.addDataToLine(selectarr[i], value, lineRef.get(), ruleEvaluableRef.get()));
					}
				}

				thislog.debug("  Base line created: " + lineRef.get());

				// Execute subquery to get target data (e.g. groups)
				if (resourcepathsubquery != null && !resourcepathsubquery.equals("")) {

					// Replace $id placeholder with actual source id
					String finalresourcepathsubquery = resourcepathsubquery.replace("$id", sourceuniqueattributevalue);

					thislog.debug("  Executing subquery: " + finalresourcepathsubquery);

					// Build subquery request
					MSGraphRequest subqueryrequest = new MSGraphRequest(MSGraphRequest.type_GET, finalresourcepathsubquery);

					if (resourcepathsubqueryselect != null && !resourcepathsubqueryselect.equals("")) {
						subqueryrequest.addParameter("$select", resourcepathsubqueryselect);
					}

					if (resourcepathsubqueryfilter != null && !resourcepathsubqueryfilter.equals("")) {
						subqueryrequest.addParameter("$filter", resourcepathsubqueryfilter);
					}
					try {
						// Send subquery with retry
						MSGraphResponse subqueryresponse = sendRequestWithRetry(session, subqueryrequest, coRequest, thislog);

						// Process subquery response - create one line per target element (multiline!)
						XML xmlSubqueryResponse = subqueryresponse.getDataAsXML();

						// Count target elements
						long targetElementCount = xmlSubqueryResponse.getElementStream().count();
						thislog.debug("  Subquery returned " + targetElementCount +
							" target elements");

						// Reset stream
						xmlSubqueryResponse = subqueryresponse.getDataAsXML();

						// Create a map to hold all lines (one per target)
						Map<String, RuleEvaluableString> hmLines = new HashMap<String, RuleEvaluableString>();

						xmlSubqueryResponse.getElementStream().forEach((Element subElement) -> {

							XML xmlSubElement = new XML(subElement);

							// Only process <value> elements, skip metadata like <odata.context>
							if (!xmlSubElement.getElement().getName().equalsIgnoreCase("value")) {
								return; // Skip this element
							}

							// Start with a copy of the base line (source attributes)
							AtomicReference<String> targetLineRef = new AtomicReference<>();
							targetLineRef.set(lineRef.get());

							// ALWAYS create a new RuleEvaluableString for this specific relation line
							RuleEvaluableString targetRuleEvaluable = new RuleEvaluableString(coRequest.getNodename());

							// Copy source attributes from ruleEvaluableRef to target rule evaluable
							if (ruleEvaluableRef.get() != null) {
								for (String sourceAttributeKey : ruleEvaluableRef.get().getAttributes()) {
									List<String> sourceAttributeValues = ruleEvaluableRef.get().getAttributeValues(sourceAttributeKey);
									if (sourceAttributeValues != null) {
										targetRuleEvaluable.addAttribute(sourceAttributeKey, sourceAttributeValues);
									}
								}
							}

							// Add target attributes with prefix
							String[] subqueryselectarr = resourcepathsubqueryselect.split(",");
							for (int i = 0; i < subqueryselectarr.length; i++) {
								String value = xmlSubElement.findNode(subqueryselectarr[i]).getText();
								if (value == null)
									value = "";

								targetRuleEvaluable.addAttribute(subqueryattributeprefix + subqueryselectarr[i], value);

								// Then add to line (respects skipproperties)
								targetLineRef.set(
										relationcollection.addDataToLine(subqueryattributeprefix + subqueryselectarr[i], value, targetLineRef.get(), null));
							}

							// DEBUG: Log what's in the RuleEvaluable
							if (hasRules) {
								thislog.debug("    RuleEvaluable attributes: " + targetRuleEvaluable.getAttributes());
								for (String attr : targetRuleEvaluable.getAttributes()) {
									List<String> values = targetRuleEvaluable.getAttributeValues(attr);
									thislog.debug("      " + attr +
										" = " +
										(values != null ? values : "null"));
								}
							}

							// Store this line in the map
							hmLines.put(targetLineRef.get(), targetRuleEvaluable);
						});

						thislog.debug("  Created " + hmLines.size() +
							" relation lines for this source element");

						// Write all lines (with rule filtering)
						int writtenCount = 0;
						int filteredCount = 0;
						for (Map.Entry<String, RuleEvaluableString> entry : hmLines.entrySet()) {
							try {
								if (!hasRules || relationcollection.isAllowed(entry.getValue())) {
									coResponse.writeDataLine(entry.getKey());
									writtenCount++;
									thislog.debug("    Line written: " + entry.getKey());
								} else {
									filteredCount++;
									thislog.debug("    Line filtered by rules: " + entry.getKey());
								}
							} catch (IOException e) {
								thislog.error("Error writing relation line", e);
								e.printStackTrace();
							}
						}

						if (hasRules) {
							thislog.info("  Result for source " + sourceuniqueattributevalue +
								": " +
								writtenCount +
								" lines written, " +
								filteredCount +
								" lines filtered");
						} else {
							thislog.info("  Result for source " + sourceuniqueattributevalue +
								": " +
								writtenCount +
								" lines written");
						}

					} catch (Exception e) {
						thislog.error("Subquery failed for source " + sourceuniqueattributevalue +
							", skipping relations: " +
							e.getMessage());
						// Continue with next source element (no relations written for this source)
					}
				}
			}
		});
	}

	/**
	 * Extracts connection parameters from ConnectorRequest for connection pooling.
	 * Maps gp_param* keys directly (ConnectionManager uses these for key
	 * generation)
	 * 
	 * @param request The connector request containing parameters
	 * @return Map with connection parameters using gp_param* keys
	 */
	private Map<String, String> extractConnectionParams(ConnectorRequest request) {
		Map<String, String> params = new HashMap<>();

		// Use gp_param* keys directly (as ConnectionManager expects them)
		params.put("gp_param1", request.getParameter("gp_param1")); // authority
		params.put("gp_param2", request.getParameter("gp_param2")); // tenant
		params.put("gp_param3", request.getParameter("gp_param3")); // tenantname
		params.put("gp_param4", request.getParameter("gp_param4")); // clientid
		params.put("gp_param5", request.getParameter("gp_param5")); // scope
		params.put("gp_param6", request.getParameter("gp_param6")); // connecteddomain
		params.put("gp_param7", request.getParameter("gp_param7")); // proxyhost
		params.put("gp_param8", request.getParameter("gp_param8")); // proxyport
		params.put("gp_param9", request.getParameter("gp_param9")); // sslproxy
		params.put("gp_param10", request.getParameter("gp_param10")); // proxyusername
		params.put("gp_pwparam1", request.getParameter("gp_pwparam1")); // secret
		params.put("gp_pwparam2", request.getParameter("gp_pwparam2")); // proxypassword

		return params;
	}

	/**
	 * Gets an MSGraphSession from the connection pool or creates a new one.
	 * 
	 * @param request The connector request containing connection parameters
	 * @return MSGraphSession ready to use
	 * @throws Exception if session creation fails
	 */
	private MSGraphSession getOrCreateSession(ConnectorRequest request) throws Exception {
		// Extract parameters
		Map<String, String> params = extractConnectionParams(request);

		// Get connection from pool (or create new one)
		MSGraphManagedConnection managedConn =
				ConnectionManager.getInstance().getConnection(params, MSGraphManagedConnection.class, new MSGraphConnectionFactory());

		// Return the wrapped session
		return managedConn.getSession();
	}

	/**
	 * Sends MSGraph request with automatic retry on connection failure.
	 * <p>
	 * If the first attempt fails (token expired, network issue, etc.),
	 * this method automatically gets a fresh session from the connection pool
	 * and retries the request once.
	 * 
	 * @param session   The current MSGraph session
	 * @param request   The request to send
	 * @param coRequest The connector request (needed to get fresh session)
	 * @return MSGraphResponse from the API
	 * @throws Exception if both attempts fail
	 */
	private MSGraphResponse sendRequestWithRetry(MSGraphSession session, MSGraphRequest request, ConnectorRequest coRequest, Logger logger) throws Exception {
		try {
			// First attempt with current session
			return session.sendRequest(request);

		} catch (Exception firstAttempt) {
			// First attempt failed - log and retry
			logger.warn("Request failed on first attempt: {}. Retrying with fresh session...", firstAttempt.getMessage());

			try {
				// Get fresh session from pool (will create new if needed)
				MSGraphSession freshSession = getOrCreateSession(coRequest);

				// Retry with fresh session
				logger.debug("Retrying request with fresh session...");
				return freshSession.sendRequest(request);

			} catch (Exception secondAttempt) {
				// Both attempts failed - log and throw
				logger.error("Request failed after retry: {}", secondAttempt.getMessage());
				throw secondAttempt;
			}
		}
	}

	@Override
	public XML createAccount(Logger thislog, ConnectorRequest coRequest, ConnectorResponse coResponse) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public XML modifyAccount(Logger thislog, ConnectorRequest coRequest, ConnectorResponse coResponse) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public XML deleteAccount(Logger thislog, ConnectorRequest coRequest, ConnectorResponse coResponse) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public XML modifyAccountPassword(Logger thislog, ConnectorRequest coRequest, ConnectorResponse coResponse) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public XML modifyAccountStatus(Logger thislog, ConnectorRequest coRequest, ConnectorResponse coResponse) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public XML modifyAccountType(Logger thislog, ConnectorRequest coRequest, ConnectorResponse coResponse) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public XML renameAccount(Logger thislog, ConnectorRequest coRequest, ConnectorResponse coResponse) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public XML moveAccount(Logger thislog, ConnectorRequest coRequest, ConnectorResponse coResponse) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public XML reconcileAccount(Logger thislog, ConnectorRequest coRequest, ConnectorResponse coResponse) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public XML customAccountCommand(Logger thislog, ConnectorRequest coRequest, ConnectorResponse coResponse) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public XML createSecurityObject(Logger thislog, ConnectorRequest coRequest, ConnectorResponse coResponse) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public XML modifySecurityObject(Logger thislog, ConnectorRequest coRequest, ConnectorResponse coResponse) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public XML deleteSecurityObject(Logger thislog, ConnectorRequest coRequest, ConnectorResponse coResponse) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public XML renameSecurityObject(Logger thislog, ConnectorRequest coRequest, ConnectorResponse coResponse) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public XML moveSecurityObject(Logger thislog, ConnectorRequest coRequest, ConnectorResponse coResponse) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public XML reconcileSecurityObject(Logger thislog, ConnectorRequest coRequest, ConnectorResponse coResponse) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public XML customSecurityObjectCommand(Logger thislog, ConnectorRequest coRequest, ConnectorResponse coResponse) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public XML createPermission(Logger thislog, ConnectorRequest coRequest, ConnectorResponse coResponse) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public XML deletePermission(Logger thislog, ConnectorRequest coRequest, ConnectorResponse coResponse) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

}
