package de.guh.extension.connector.entraid;

import java.io.File;
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.guh.connector.core.ConnectorRequest;
import de.guh.connector.core.ConnectorResponse;
import de.guh.connector.core.NodeCollection;
import de.guh.connector.core.RelationCollection;
import de.guh.connector.core.connection.ConnectionManager;
import de.guh.connector.core.provisioning.ProvisioningHelper;
import de.guh.plugin.xml.XML;

public class Main {

	protected static final Logger logger = LogManager.getRootLogger();

	public static void main(String[] args) {

		String action = "";
		String generalparameterfile = "";
		String collectionparameterfile = "";
		String outputpath = "";

		if (args == null || args.length == 0 || args.length > 4) {

			printUsage();
			System.exit(1);

		}

		action					= args[0];
		generalparameterfile	= args[1];

		// Optional parameters depending on action
		if (args.length > 2) {
			collectionparameterfile = args[2];
		}
		if (args.length > 3) {
			outputpath = args[3];
		}

		// ConnectionManager initialisieren (einmal beim Start)
		logger.info("Initializing ConnectionManager...");
		ConnectionManager.getInstance();

		// Load general parameters
		HashMap<String, String> Config = new HashMap<String, String>();

		XML xmlGParamter = new XML("temp").fromFile(new File(generalparameterfile));
		System.out.println(xmlGParamter.toFormatedString());

		Config.put("gp_param1", xmlGParamter.getValue("gp_param1"));
		Config.put("gp_param2", xmlGParamter.getValue("gp_param2"));
		Config.put("gp_param3", xmlGParamter.getValue("gp_param3"));
		Config.put("gp_param4", xmlGParamter.getValue("gp_param4"));
		Config.put("gp_param5", xmlGParamter.getValue("gp_param5"));
		Config.put("gp_param6", xmlGParamter.getValue("gp_param6"));
		Config.put("gp_param7", xmlGParamter.getValue("gp_param7"));
		Config.put("gp_param8", xmlGParamter.getValue("gp_param8"));
		Config.put("gp_param9", xmlGParamter.getValue("gp_param9"));
		Config.put("gp_param10", xmlGParamter.getValue("gp_param10"));

		Config.put("gp_pwparam1", xmlGParamter.getValue("gp_pwparam1"));
		Config.put("gp_pwparam2", xmlGParamter.getValue("gp_pwparam2"));

		// Execute action
		switch (action.toLowerCase()) {
			// ========================================
			// DATA COLLECTION ACTIONS
			// ========================================
			case "connect":
				connect(Config);
				break;
			case "getnodepreview":
				getnodepreview(Config, collectionparameterfile);
				break;
			case "getrelationpreview":
				getrelationpreview(Config, collectionparameterfile);
				break;
			case "getnodedata":
				getnodedata(Config, collectionparameterfile, outputpath);
				break;
			case "getrelationdata":
				getrelationdata(Config, collectionparameterfile, outputpath);
				break;

			// ========================================
			// ACCOUNT PROVISIONING
			// ========================================
			case "createaccount":
				provisionAccount(Config, collectionparameterfile, "create");
				break;
			case "modifyaccount":
				provisionAccount(Config, collectionparameterfile, "modify");
				break;
			case "deleteaccount":
				provisionAccount(Config, collectionparameterfile, "delete");
				break;
			case "modifyaccountpassword":
				provisionAccount(Config, collectionparameterfile, "modifyPassword");
				break;
			case "modifyaccountstatus":
				provisionAccount(Config, collectionparameterfile, "modifyStatus");
				break;
			case "modifyaccounttype":
				provisionAccount(Config, collectionparameterfile, "modifyType");
				break;
			case "renameaccount":
				provisionAccount(Config, collectionparameterfile, "rename");
				break;
			case "moveaccount":
				provisionAccount(Config, collectionparameterfile, "move");
				break;
			case "reconcileaccount":
				provisionAccount(Config, collectionparameterfile, "reconcile");
				break;
			case "customaccountcommand":
				provisionAccount(Config, collectionparameterfile, "customcommand");
				break;

			// ========================================
			// SECURITYOBJECT PROVISIONING
			// ========================================
			case "createsecurityobject":
				provisionSecurityObject(Config, collectionparameterfile, "create");
				break;
			case "modifysecurityobject":
				provisionSecurityObject(Config, collectionparameterfile, "modify");
				break;
			case "deletesecurityobject":
				provisionSecurityObject(Config, collectionparameterfile, "delete");
				break;
			case "renamesecurityobject":
				provisionSecurityObject(Config, collectionparameterfile, "rename");
				break;
			case "movesecurityobject":
				provisionSecurityObject(Config, collectionparameterfile, "move");
				break;
			case "reconcilesecurityobject":
				provisionSecurityObject(Config, collectionparameterfile, "reconcile");
				break;
			case "customsecurityobjectcommand":
				provisionSecurityObject(Config, collectionparameterfile, "customcommand");
				break;

			// ========================================
			// PERMISSION PROVISIONING
			// ========================================
			case "createpermission":
				provisionPermission(Config, collectionparameterfile, "create");
				break;
			case "deletepermission":
				provisionPermission(Config, collectionparameterfile, "delete");
				break;

			default:
				logger.error("Unknown action: " + action);
				logger.error("Use 'java -jar extension.jar' without arguments to see all available actions.");
				break;
		}

		// ConnectionManager cleanup (am Ende)
		logger.info("Shutting down ConnectionManager...");
		ConnectionManager.getInstance().shutdown();
	}

	/**
	 * Provides access to the ConnectionManager for extensions.
	 * Extensions can use this to get pooled connections.
	 * 
	 * @return the ConnectionManager instance
	 */
	public static ConnectionManager getConnectionManager() {
		return ConnectionManager.getInstance();
	}

	// ========================================
	// DATA COLLECTION METHODS (unchanged)
	// ========================================

	/**
	 * Test connection to Entra ID
	 */
	private static Boolean connect(HashMap<String, String> config) {

		logger.info("Testing connection to Entra ID...");

		EntraIDJavaConnectorExtension extension = new EntraIDJavaConnectorExtension();

		try {
			// Build ConnectorRequest with dummy values for connection test
			String uniqueid = "1";
			String decryptionhash = "inchorus2012!";
			String nodename = "CONNECT";
			String callbackurl = "";

			ConnectorRequest coRequest = new ConnectorRequest(uniqueid, decryptionhash, nodename, callbackurl);
			coRequest.setParameters(config);

			ConnectorResponse coResponse = new ConnectorResponse("", coRequest);

			logger.info("Calling extension.checkConnection()...");
			XML xmlResponse = extension.checkConnection(logger, coRequest, coResponse);

			logger.info("Connection test result:");
			logger.info(xmlResponse.toFormatedString());

			String connected = xmlResponse.getValue("connected");
			if ("true".equalsIgnoreCase(connected)) {
				logger.info("Connection test: SUCCESS");
				return true;
			} else {
				logger.error("Connection test: FAILED");
				String error = xmlResponse.getValue("error");
				if (error != null && !error.isEmpty()) {
					logger.error("Error: " + error);
				}
				return false;
			}

		} catch (Exception e) {
			logger.error("Connection test failed with exception", e);
			e.printStackTrace();
			return false;
		}
	}

	/**
	 * Collect node data from Entra ID
	 */
	private static void getnodedata(HashMap<String, String> config, String collectionconfigfile, String outputpath) {

		if (!outputpath.endsWith(File.separator))
			outputpath = outputpath + File.separator;

		logger.info("Starting node data collection...");
		logger.info("Output path: " + outputpath);

		String uniqueid = "1";
		String decryptionhash = "inchorus2012!";
		String nodename = "NODE";
		String callbackurl = "";

		XML xmlConfig = new XML("temp").fromFile(new File(collectionconfigfile));
		XML xmlParameter = xmlConfig.findChildNode("np_params");
		logger.info("Node parameters:");
		logger.info(xmlParameter.toFormatedString());

		config.put("np_param1", xmlParameter.getValue("np_param1"));
		config.put("np_param2", xmlParameter.getValue("np_param2"));
		config.put("np_param3", xmlParameter.getValue("np_param3"));
		config.put("np_param4", xmlParameter.getValue("np_param4"));
		config.put("np_param5", xmlParameter.getValue("np_param5"));
		config.put("np_param6", xmlParameter.getValue("np_param6"));
		config.put("np_param7", xmlParameter.getValue("np_param7"));
		config.put("np_param8", xmlParameter.getValue("np_param8"));
		config.put("np_param9", xmlParameter.getValue("np_param9"));
		config.put("np_param10", xmlParameter.getValue("np_param10"));

		config.put("np_pwparam1", xmlParameter.getValue("np_pwparam1"));
		config.put("np_pwparam2", xmlParameter.getValue("np_pwparam2"));

		String whitelistrule = "";
		try {
			XML xmlWhitelist = xmlConfig.findChildNode("whitelist");
			XML whitelistNode = xmlWhitelist.getFirstChild();
			if (whitelistNode != null && whitelistNode.getElement() != null) {
				whitelistrule = whitelistNode.toString();
				logger.debug("Loaded whitelist rule");
			} else {
				logger.debug("No whitelist rule found");
			}
		} catch (Exception e) {
			logger.warn("Error loading whitelist rule: " + e.getMessage());
		}

		String blacklistrule = "";
		try {
			XML xmlBlacklist = xmlConfig.findChildNode("blacklist");
			XML blacklistNode = xmlBlacklist.getFirstChild();
			if (blacklistNode != null && blacklistNode.getElement() != null) {
				blacklistrule = blacklistNode.toString();
				logger.debug("Loaded blacklist rule");
			} else {
				logger.debug("No blacklist rule found");
			}
		} catch (Exception e) {
			logger.warn("Error loading blacklist rule: " + e.getMessage());
		}

		String skipproperties = xmlConfig.findChildNode("skipproperties").getText();
		if (skipproperties == null) {
			skipproperties = "";
		}

		EntraIDJavaConnectorExtension extension = new EntraIDJavaConnectorExtension();

		try {
			ConnectorRequest coRequest = new ConnectorRequest(uniqueid, decryptionhash, nodename, callbackurl);
			coRequest.setParameters(config);

			ConnectorResponse coResponse = new ConnectorResponse(outputpath, coRequest);
			coResponse.openDatafile();

			NodeCollection nodecollection = new NodeCollection(uniqueid, nodename, "", "DEBUG", decryptionhash, nodename, whitelistrule, blacklistrule,
					skipproperties, callbackurl, "", outputpath);

			logger.info("Calling extension.getNodeData()...");
			boolean success = extension.getNodeData(logger, nodecollection, coRequest, coResponse);

			coResponse.closeDatafile();

			if (success) {
				logger.info("Node data collection: SUCCESS");
			} else {
				logger.error("Node data collection: FAILED");
			}

		} catch (Exception e) {
			logger.error("Node data collection failed with exception", e);
			e.printStackTrace();
		}
	}

	/**
	 * Collect relation data from Entra ID
	 */
	private static void getrelationdata(HashMap<String, String> config, String collectionconfigfile, String outputpath) {

		if (!outputpath.endsWith(File.separator))
			outputpath = outputpath + File.separator;

		logger.info("Starting relation data collection...");
		logger.info("Output path: " + outputpath);

		String uniqueid = "1";
		String decryptionhash = "inchorus2012!";
		String relationname = "RELATION";
		String nodename = "NODE";
		String callbackurl = "";

		XML xmlConfig = new XML("temp").fromFile(new File(collectionconfigfile));
		XML xmlParameter = xmlConfig.findChildNode("rp_params");
		logger.info("Relation parameters:");
		logger.info(xmlParameter.toFormatedString());

		config.put("rp_param1", xmlParameter.getValue("rp_param1"));
		config.put("rp_param2", xmlParameter.getValue("rp_param2"));
		config.put("rp_param3", xmlParameter.getValue("rp_param3"));
		config.put("rp_param4", xmlParameter.getValue("rp_param4"));
		config.put("rp_param5", xmlParameter.getValue("rp_param5"));
		config.put("rp_param6", xmlParameter.getValue("rp_param6"));
		config.put("rp_param7", xmlParameter.getValue("rp_param7"));
		config.put("rp_param8", xmlParameter.getValue("rp_param8"));
		config.put("rp_param9", xmlParameter.getValue("rp_param9"));

		config.put("rp_pwparam1", xmlParameter.getValue("rp_pwparam1"));
		config.put("rp_pwparam2", xmlParameter.getValue("rp_pwparam2"));
		config.put("rp_pwparam3", xmlParameter.getValue("rp_pwparam3"));
		config.put("rp_pwparam4", xmlParameter.getValue("rp_pwparam4"));

		String whitelistrule = "";
		try {
			XML xmlWhitelist = xmlConfig.findChildNode("whitelist");
			XML whitelistNode = xmlWhitelist.getFirstChild();
			if (whitelistNode != null && whitelistNode.getElement() != null) {
				whitelistrule = whitelistNode.toString();
				logger.debug("Loaded whitelist rule");
			} else {
				logger.debug("No whitelist rule found");
			}
		} catch (Exception e) {
			logger.warn("Error loading whitelist rule: " + e.getMessage());
		}

		String blacklistrule = "";
		try {
			XML xmlBlacklist = xmlConfig.findChildNode("blacklist");
			XML blacklistNode = xmlBlacklist.getFirstChild();
			if (blacklistNode != null && blacklistNode.getElement() != null) {
				blacklistrule = blacklistNode.toString();
				logger.debug("Loaded blacklist rule");
			} else {
				logger.debug("No blacklist rule found");
			}
		} catch (Exception e) {
			logger.warn("Error loading blacklist rule: " + e.getMessage());
		}

		String skipproperties = xmlConfig.findChildNode("skipproperties").getText();
		if (skipproperties == null) {
			skipproperties = "";
		}

		EntraIDJavaConnectorExtension extension = new EntraIDJavaConnectorExtension();

		try {
			ConnectorRequest coRequest = new ConnectorRequest(uniqueid, decryptionhash, nodename, callbackurl);
			coRequest.setParameters(config);

			ConnectorResponse coResponse = new ConnectorResponse(outputpath, coRequest);
			coResponse.openDatafile();

			RelationCollection relationcollection = new RelationCollection(uniqueid, relationname, "", "DEBUG", decryptionhash, nodename, whitelistrule,
					blacklistrule, skipproperties, callbackurl, "", outputpath);

			logger.info("Calling extension.getRelationData()...");
			boolean success = extension.getRelationData(logger, relationcollection, coRequest, coResponse);

			coResponse.closeDatafile();

			if (success) {
				logger.info("Relation data collection: SUCCESS");
			} else {
				logger.error("Relation data collection: FAILED");
			}

		} catch (Exception e) {
			logger.error("Relation data collection failed with exception", e);
			e.printStackTrace();
		}
	}

	/**
	 * Get node preview data from Entra ID
	 */
	private static void getnodepreview(HashMap<String, String> config, String collectionconfigfile) {

		logger.info("Getting node preview data from Entra ID...");

		String uniqueid = "1";
		String decryptionhash = "inchorus2012!";
		String nodename = "PREVIEW";
		String callbackurl = "";

		XML xmlParameter = new XML("temp").fromFile(new File(collectionconfigfile));
		logger.info("Preview parameters:");
		logger.info(xmlParameter.toFormatedString());

		config.put("cp_param1", xmlParameter.getValue("cp_param1"));
		config.put("cp_param2", xmlParameter.getValue("cp_param2"));
		config.put("cp_param3", xmlParameter.getValue("cp_param3"));
		config.put("cp_param4", xmlParameter.getValue("cp_param4"));
		config.put("cp_param5", xmlParameter.getValue("cp_param5"));
		config.put("cp_param6", xmlParameter.getValue("cp_param6"));
		config.put("cp_param7", xmlParameter.getValue("cp_param7"));
		config.put("cp_param8", xmlParameter.getValue("cp_param8"));
		config.put("cp_param9", xmlParameter.getValue("cp_param9"));
		config.put("cp_param10", xmlParameter.getValue("cp_param10"));
		config.put("cp_param11", xmlParameter.getValue("cp_param11"));
		config.put("cp_param12", xmlParameter.getValue("cp_param12"));
		config.put("cp_param13", xmlParameter.getValue("cp_param13"));
		config.put("cp_param14", xmlParameter.getValue("cp_param14"));
		config.put("cp_param15", xmlParameter.getValue("cp_param15"));

		config.put("cp_pwparam1", xmlParameter.getValue("cp_pwparam1"));
		config.put("cp_pwparam2", xmlParameter.getValue("cp_pwparam2"));
		config.put("cp_pwparam3", xmlParameter.getValue("cp_pwparam3"));
		config.put("cp_pwparam4", xmlParameter.getValue("cp_pwparam4"));
		config.put("cp_pwparam5", xmlParameter.getValue("cp_pwparam5"));

		EntraIDJavaConnectorExtension extension = new EntraIDJavaConnectorExtension();

		try {
			ConnectorRequest coRequest = new ConnectorRequest(uniqueid, decryptionhash, nodename, callbackurl);
			coRequest.setParameters(config);

			ConnectorResponse coResponse = new ConnectorResponse("", coRequest);

			logger.info("Calling extension.getNodePreview()...");
			XML xmlResponse = extension.getNodePreview(logger, coRequest, coResponse);

			logger.info("Node preview data result:");
			logger.info(xmlResponse.toFormatedString());

			logger.info("Node preview data: SUCCESS");

		} catch (Exception e) {
			logger.error("Node preview data failed with exception", e);
			e.printStackTrace();
		}
	}

	/**
	 * Get relation preview data from Entra ID
	 */
	private static void getrelationpreview(HashMap<String, String> config, String collectionconfigfile) {

		logger.info("Getting relation preview data from Entra ID...");

		String uniqueid = "1";
		String decryptionhash = "inchorus2012!";
		String nodename = "PREVIEW_RELATION";
		String callbackurl = "";

		XML xmlParameter = new XML("temp").fromFile(new File(collectionconfigfile));
		logger.info("Relation preview parameters:");
		logger.info(xmlParameter.toFormatedString());

		// Load relation parameters (rp_param*)
		config.put("cp_param1", xmlParameter.getValue("cp_param1"));
		config.put("cp_param2", xmlParameter.getValue("cp_param2"));
		config.put("cp_param3", xmlParameter.getValue("cp_param3"));
		config.put("cp_param4", xmlParameter.getValue("cp_param4"));
		config.put("cp_param5", xmlParameter.getValue("cp_param5"));
		config.put("cp_param6", xmlParameter.getValue("cp_param6"));
		config.put("cp_param7", xmlParameter.getValue("cp_param7"));
		config.put("cp_param8", xmlParameter.getValue("cp_param8"));
		config.put("cp_param9", xmlParameter.getValue("cp_param9"));
		config.put("cp_param10", xmlParameter.getValue("cp_param10"));
		config.put("cp_param11", xmlParameter.getValue("cp_param11"));
		config.put("cp_param12", xmlParameter.getValue("cp_param12"));
		config.put("cp_param13", xmlParameter.getValue("cp_param13"));
		config.put("cp_param14", xmlParameter.getValue("cp_param14"));
		config.put("cp_param15", xmlParameter.getValue("cp_param15"));

		config.put("cp_pwparam1", xmlParameter.getValue("cp_pwparam1"));
		config.put("cp_pwparam2", xmlParameter.getValue("cp_pwparam2"));
		config.put("cp_pwparam3", xmlParameter.getValue("cp_pwparam3"));
		config.put("cp_pwparam4", xmlParameter.getValue("cp_pwparam4"));
		config.put("cp_pwparam5", xmlParameter.getValue("cp_pwparam5"));

		EntraIDJavaConnectorExtension extension = new EntraIDJavaConnectorExtension();

		try {
			ConnectorRequest coRequest = new ConnectorRequest(uniqueid, decryptionhash, nodename, callbackurl);
			coRequest.setParameters(config);

			ConnectorResponse coResponse = new ConnectorResponse("", coRequest);

			logger.info("Calling extension.getRelationPreview()...");
			XML xmlResponse = extension.getRelationPreview(logger, coRequest, coResponse);

			logger.info("Relation preview data result:");
			logger.info(xmlResponse.toFormatedString());

			logger.info("Relation preview data: SUCCESS");

		} catch (Exception e) {
			logger.error("Relation preview data failed with exception", e);
			e.printStackTrace();
		}
	}

	// ========================================
	// PROVISIONING METHODS (new)
	// ========================================

	/**
	 * Provision an account (all account operations)
	 * 
	 * @param config     HashMap with connection parameters (gp_param*)
	 * @param configfile Path to provisioning config XML file
	 * @param operation  Operation to perform (create, modify, delete,
	 *                       modifyPassword, etc.)
	 */
	private static void provisionAccount(HashMap<String, String> config, String configfile, String operation) {

		logger.info("==============================================");
		logger.info("Account Provisioning: " + operation.toUpperCase());
		logger.info("==============================================");

		try {
			// 1. Load provisioning configuration
			XML xmlConfig = new XML("temp").fromFile(new File(configfile));
			logger.info("Loaded provisioning config:");
			logger.info(xmlConfig.toFormatedString());

			// 2. Build xmlconnectionstring
			XML xmlConnection = new XML("parameters");
			config.forEach((key, value) -> {
				if (value != null && !value.isEmpty()) {
					XML param = new XML("parameter");
					param.appendAttribute("name", key);
					param.setText(value);
					xmlConnection.appendXML(param);
				}
			});

			String xmlconnectionstring = xmlConnection.toString();

			// 3. Get xmldatastring from config
			XML dataNode = xmlConfig.findNode("//data");
			String xmldatastring = dataNode != null ? dataNode.toString() : "";

			// 4. Get additional parameters
			HashMap<String, String> additionalParams = new HashMap<>();

			XML paramsNode = xmlConfig.findNode("//parameters");
			if (paramsNode != null) {
				paramsNode.findNodes("parameter").forEach(param -> {
					String name = param.getAttribute("name");
					String value = param.getText();
					if (name != null && value != null) {
						additionalParams.put(name, value);
						logger.info("Parameter: " + name +
							" = " +
							value);
					}
				});
			}

			// 5. Build ConnectorRequest using ProvisioningHelper
			String decryptionhash = "inchorus2012!";

			ConnectorRequest coRequest = ProvisioningHelper.buildConnectorRequest(logger, xmlconnectionstring, xmldatastring, additionalParams, decryptionhash);

			// 6. Build ConnectorResponse
			ConnectorResponse coResponse = new ConnectorResponse("", coRequest);

			// 7. Load Extension
			EntraIDJavaConnectorExtension extension = new EntraIDJavaConnectorExtension();

			// 8. Call appropriate method
			logger.info("Calling extension." + operation +
				"Account()...");

			XML result = null;
			switch (operation) {
				case "create":
					result = extension.createAccount(logger, coRequest, coResponse);
					break;
				case "modify":
					result = extension.modifyAccount(logger, coRequest, coResponse);
					break;
				case "delete":
					result = extension.deleteAccount(logger, coRequest, coResponse);
					break;
				case "modifyPassword":
					result = extension.modifyAccountPassword(logger, coRequest, coResponse);
					break;
				case "modifyStatus":
					result = extension.modifyAccountStatus(logger, coRequest, coResponse);
					break;
				case "modifyType":
					result = extension.modifyAccountType(logger, coRequest, coResponse);
					break;
				case "rename":
					result = extension.renameAccount(logger, coRequest, coResponse);
					break;
				case "move":
					result = extension.moveAccount(logger, coRequest, coResponse);
					break;
				case "reconcile":
					result = extension.reconcileAccount(logger, coRequest, coResponse);
					break;
				case "customcommand":
					result = extension.customAccountCommand(logger, coRequest, coResponse);
					break;
			}

			// 9. Display result
			displayProvisioningResult(result, "Account " + operation);

		} catch (Exception e) {
			logger.error("Account provisioning failed with exception", e);
			e.printStackTrace();
		}
	}

	/**
	 * Provision a security object (all security object operations)
	 */
	private static void provisionSecurityObject(HashMap<String, String> config, String configfile, String operation) {

		logger.info("==============================================");
		logger.info("SecurityObject Provisioning: " + operation.toUpperCase());
		logger.info("==============================================");

		try {
			XML xmlConfig = new XML("temp").fromFile(new File(configfile));
			logger.info("Loaded provisioning config:");
			logger.info(xmlConfig.toFormatedString());

			XML xmlConnection = new XML("parameters");
			config.forEach((key, value) -> {
				if (value != null && !value.isEmpty()) {
					XML param = new XML("parameter");
					param.appendAttribute("name", key);
					param.setText(value);
					xmlConnection.appendXML(param);
				}
			});

			String xmlconnectionstring = xmlConnection.toString();

			XML dataNode = xmlConfig.findNode("//data");
			String xmldatastring = dataNode != null ? dataNode.toString() : "";

			HashMap<String, String> additionalParams = new HashMap<>();
			XML paramsNode = xmlConfig.findNode("//parameters");
			if (paramsNode != null) {
				paramsNode.findNodes("parameter").forEach(param -> {
					String name = param.getAttribute("name");
					String value = param.getText();
					if (name != null && value != null) {
						additionalParams.put(name, value);
						logger.info("Parameter: " + name +
							" = " +
							value);
					}
				});
			}

			String decryptionhash = "inchorus2012!";
			ConnectorRequest coRequest = ProvisioningHelper.buildConnectorRequest(logger, xmlconnectionstring, xmldatastring, additionalParams, decryptionhash);

			ConnectorResponse coResponse = new ConnectorResponse("", coRequest);
			EntraIDJavaConnectorExtension extension = new EntraIDJavaConnectorExtension();

			logger.info("Calling extension." + operation +
				"SecurityObject()...");

			XML result = null;
			switch (operation) {
				case "create":
					result = extension.createSecurityObject(logger, coRequest, coResponse);
					break;
				case "modify":
					result = extension.modifySecurityObject(logger, coRequest, coResponse);
					break;
				case "delete":
					result = extension.deleteSecurityObject(logger, coRequest, coResponse);
					break;
				case "rename":
					result = extension.renameSecurityObject(logger, coRequest, coResponse);
					break;
				case "move":
					result = extension.moveSecurityObject(logger, coRequest, coResponse);
					break;
				case "reconcile":
					result = extension.reconcileSecurityObject(logger, coRequest, coResponse);
					break;
				case "customcommand":
					result = extension.customSecurityObjectCommand(logger, coRequest, coResponse);
					break;
			}

			displayProvisioningResult(result, "SecurityObject " + operation);

		} catch (Exception e) {
			logger.error("SecurityObject provisioning failed with exception", e);
			e.printStackTrace();
		}
	}

	/**
	 * Provision a permission (create/delete)
	 */
	private static void provisionPermission(HashMap<String, String> config, String configfile, String operation) {

		logger.info("==============================================");
		logger.info("Permission Provisioning: " + operation.toUpperCase());
		logger.info("==============================================");

		try {
			XML xmlConfig = new XML("temp").fromFile(new File(configfile));
			logger.info("Loaded provisioning config:");
			logger.info(xmlConfig.toFormatedString());

			XML xmlConnection = new XML("parameters");
			config.forEach((key, value) -> {
				if (value != null && !value.isEmpty()) {
					XML param = new XML("parameter");
					param.appendAttribute("name", key);
					param.setText(value);
					xmlConnection.appendXML(param);
				}
			});

			String xmlconnectionstring = xmlConnection.toString();

			XML dataNode = xmlConfig.findNode("//data");
			String xmldatastring = dataNode != null ? dataNode.toString() : "";

			HashMap<String, String> additionalParams = new HashMap<>();
			XML paramsNode = xmlConfig.findNode("//parameters");
			if (paramsNode != null) {
				paramsNode.findNodes("parameter").forEach(param -> {
					String name = param.getAttribute("name");
					String value = param.getText();
					if (name != null && value != null) {
						additionalParams.put(name, value);
						logger.info("Parameter: " + name +
							" = " +
							value);
					}
				});
			}

			String decryptionhash = "inchorus2012!";
			ConnectorRequest coRequest = ProvisioningHelper.buildConnectorRequest(logger, xmlconnectionstring, xmldatastring, additionalParams, decryptionhash);

			ConnectorResponse coResponse = new ConnectorResponse("", coRequest);
			EntraIDJavaConnectorExtension extension = new EntraIDJavaConnectorExtension();

			logger.info("Calling extension." + operation +
				"Permission()...");

			XML result = null;
			switch (operation) {
				case "create":
					result = extension.createPermission(logger, coRequest, coResponse);
					break;
				case "delete":
					result = extension.deletePermission(logger, coRequest, coResponse);
					break;
			}

			displayProvisioningResult(result, "Permission " + operation);

		} catch (Exception e) {
			logger.error("Permission provisioning failed with exception", e);
			e.printStackTrace();
		}
	}

	/**
	 * Helper method to display provisioning result
	 */
	private static void displayProvisioningResult(XML result, String operationName) {

		logger.info("==============================================");
		logger.info("RESULT:");
		logger.info("==============================================");

		if (result != null) {
			logger.info(result.toFormatedString());

			String success = result.findNode("success").getText();
			if (success != null && success.equalsIgnoreCase("true")) {
				logger.info("✅ " + operationName +
					": SUCCESS!");

				XML objectUniqueId = result.findNode("objectuniqueid");
				if (objectUniqueId != null && !objectUniqueId.getText().isEmpty()) {
					logger.info("Object Unique ID: " + objectUniqueId.getText());
				}
			} else {
				logger.error("❌ " + operationName +
					": FAILED!");
				XML errorNode = result.findNode("error");
				if (errorNode != null) {
					logger.error("Error: " + errorNode.getText());
				}
				XML errorTypeNode = result.findNode("errortype");
				if (errorTypeNode != null) {
					logger.error("Error Type: " + errorTypeNode.getText());
				}
			}
		} else {
			logger.error("❌ No result returned!");
		}

		logger.info("==============================================");
	}

	private static void printUsage() {
		System.out.println("Daccord Java Connector Extension for EntraID");
		System.out.println("=============================================");
		System.out.println("usage: java -Dlog4j2.level=DEBUG -jar extension.jar action generalparameterfile [collectionparameterfile] [outputpath]");
		System.out.println("-------------");
		System.out.println("-Dlog4j2.level: For test and troubleshooting set the loglevel to DEBUG");
		System.out.println("");
		System.out.println("=== DATA COLLECTION ACTIONS ===");
		System.out.println("  connect                     - Test connection to Entra ID");
		System.out.println("  getnodepreview              - Get node preview data");
		System.out.println("  getrelationpreview          - Get relation preview data");
		System.out.println("  getnodedata                 - Collect node data (users, etc.)");
		System.out.println("  getrelationdata             - Collect relation data (group memberships, etc.)");
		System.out.println("");
		System.out.println("=== ACCOUNT PROVISIONING ===");
		System.out.println("  createaccount               - Create a new account");
		System.out.println("  modifyaccount               - Modify an existing account");
		System.out.println("  deleteaccount               - Delete an account");
		System.out.println("  modifyaccountpassword       - Change account password");
		System.out.println("  modifyaccountstatus         - Enable/disable account");
		System.out.println("  modifyaccounttype           - Change account type");
		System.out.println("  renameaccount               - Rename an account");
		System.out.println("  moveaccount                 - Move account to different OU");
		System.out.println("  reconcileaccount            - Reconcile account");
		System.out.println("  customaccountcommand        - Trigger custom command");
		System.out.println("");
		System.out.println("=== SECURITYOBJECT PROVISIONING ===");
		System.out.println("  createsecurityobject        - Create a new security object (group)");
		System.out.println("  modifysecurityobject        - Modify an existing security object");
		System.out.println("  deletesecurityobject        - Delete a security object");
		System.out.println("  renamesecurityobject        - Rename a security object");
		System.out.println("  movesecurityobject          - Move security object");
		System.out.println("  reconcilesecurityobject     - Reconcile security object");
		System.out.println("  customsecurityobjectcommand - Trigger custom command");
		System.out.println("");
		System.out.println("=== PERMISSION PROVISIONING ===");
		System.out.println("  createpermission            - Add user to group");
		System.out.println("  deletepermission            - Remove user from group");
		System.out.println("");
		System.out.println("=== PARAMETERS ===");
		System.out.println("generalparameterfile: xml file with connection parameters (gp_param*)");
		System.out.println("collectionparameterfile: xml file with action-specific parameters");
		System.out.println("outputpath: folder for output files (data collection only)");
		System.out.println("");
		System.out.println("=== EXAMPLES ===");
		System.out.println("Data Collection:");
		System.out.println("  java -jar extension.jar connect c:\\temp\\entraid\\gpconnect.xml");
		System.out.println("  java -jar extension.jar getnodedata c:\\temp\\entraid\\gpconnect.xml c:\\temp\\entraid\\gpnodecollection.xml c:\\temp\\entraid");
		System.out.println("");
		System.out.println("Provisioning:");
		System.out.println("  java -jar extension.jar createaccount c:\\temp\\entraid\\gpconnect.xml c:\\temp\\entraid\\account-create.xml");
		System.out.println("  java -jar extension.jar createpermission c:\\temp\\entraid\\gpconnect.xml c:\\temp\\entraid\\permission-create.xml");
	}

}