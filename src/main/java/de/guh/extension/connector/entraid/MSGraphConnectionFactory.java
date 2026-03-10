package de.guh.extension.connector.entraid;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.guh.connector.core.connection.ConnectionFactory;
import de.guh.plugin.msgraph.MSGraphSession;

/**
 * Factory for creating MSGraphManagedConnection instances.
 * <p>
 * This factory creates Microsoft Graph API sessions from connection parameters
 * and wraps them in ManagedConnection objects for connection pooling.
 * <p>
 * <b>Parameter Mapping:</b>
 * <ul>
 * <li>gp_param1: authority (e.g., "https://login.microsoftonline.com/")</li>
 * <li>gp_param2: tenant (GUID)</li>
 * <li>gp_param3: tenantname (optional, for logging)</li>
 * <li>gp_param4: clientid (Application ID)</li>
 * <li>gp_param5: scope (e.g., "https://graph.microsoft.com/.default")</li>
 * <li>gp_param6: connecteddomain (optional)</li>
 * <li>gp_param7: proxyhost</li>
 * <li>gp_param8: proxyport</li>
 * <li>gp_param9: sslproxy ("true" or "false")</li>
 * <li>gp_param10: proxyusername</li>
 * <li>gp_pwparam1: secret (Client Secret)</li>
 * <li>gp_pwparam2: proxypassword</li>
 * </ul>
 * <p>
 * <b>Usage:</b>
 * 
 * <pre>
 * Map&lt;String, String&gt; params = new HashMap&lt;&gt;();
 * params.put("gp_param1", "https://login.microsoftonline.com/");
 * params.put("gp_param2", "tenant-guid");
 * params.put("gp_param4", "client-id");
 * params.put("gp_param5", "https://graph.microsoft.com/.default");
 * params.put("gp_pwparam1", "secret");
 * 
 * MSGraphManagedConnection conn = connectionManager.getConnection(params, MSGraphManagedConnection.class, new MSGraphConnectionFactory());
 * </pre>
 * 
 * @since 2.2.11-SNAPSHOT
 */
public class MSGraphConnectionFactory implements ConnectionFactory<MSGraphManagedConnection> {

	private static final Logger log = LoggerFactory.getLogger(MSGraphConnectionFactory.class);

	/**
	 * Creates a new MSGraphManagedConnection from the given parameters.
	 * <p>
	 * This method:
	 * <ol>
	 * <li>Extracts connection parameters from the map</li>
	 * <li>Creates a new MSGraphSession (which authenticates and gets token)</li>
	 * <li>Wraps the session in a MSGraphManagedConnection</li>
	 * </ol>
	 * <p>
	 * <b>Note:</b> Token acquisition happens in the MSGraphSession constructor
	 * and may take 500-1000ms. This is why connection pooling is so valuable!
	 * 
	 * @param parameters Connection parameters including authentication credentials
	 * @return A new MSGraphManagedConnection ready for use
	 * @throws Exception if session creation fails (invalid credentials, network
	 *                       issues, etc.)
	 */
	@Override
	public MSGraphManagedConnection createConnection(Map<String, String> parameters) throws Exception {
		log.debug("Creating new MSGraphSession...");

		// Extract required parameters
		String authority = getRequired(parameters, "gp_param1", "authority");
		String tenant = getRequired(parameters, "gp_param2", "tenant");
		String clientid = getRequired(parameters, "gp_param4", "clientid");
		String scope = getRequired(parameters, "gp_param5", "scope");
		String secret = getRequired(parameters, "gp_pwparam1", "secret");

		// Extract optional proxy parameters
		String proxyhost = parameters.get("gp_param7");
		String proxyport = parameters.get("gp_param8");
		String sslproxyStr = parameters.get("gp_param9");
		String proxyusername = parameters.get("gp_param10");
		String proxypassword = parameters.get("gp_pwparam2");

		boolean sslproxy = "true".equalsIgnoreCase(sslproxyStr);

		// Log connection attempt (without sensitive data)
		log.info("Creating MSGraph session - authority: {}, tenant: {}, clientid: {}", authority, tenant, clientid);

		if (proxyhost != null && !proxyhost.isBlank()) {
			log.debug("Using proxy: {}:{} (SSL: {})", proxyhost, proxyport, sslproxy);
		}

		// Create MSGraphSession (this acquires the OAuth token!)
		long startTime = System.currentTimeMillis();

		MSGraphSession session = new MSGraphSession(authority, tenant, clientid, secret, scope, proxyhost, proxyport, proxyusername, proxypassword, sslproxy);

		long duration = System.currentTimeMillis() - startTime;

		// Check if session creation was successful
		if (!session.isReady()) {
			String errorMsg = "Failed to create MSGraphSession: " + session.statusmessage();
			log.error(errorMsg);
			throw new Exception(errorMsg);
		}

		log.info("MSGraphSession created successfully (took {}ms)", duration);

		// Wrap in managed connection
		return new MSGraphManagedConnection(session);
	}

	/**
	 * Returns the connection type this factory creates.
	 * 
	 * @return MSGraphManagedConnection.class
	 */
	@Override
	public Class<MSGraphManagedConnection> getConnectionType() {
		return MSGraphManagedConnection.class;
	}

	/**
	 * Returns the parameter keys that are relevant for connection identity.
	 * <p>
	 * These parameters will be used to generate the connection key.
	 * Connections with the same values for these parameters will share
	 * the same pooled connection.
	 * <p>
	 * <b>Relevant parameters:</b>
	 * <ul>
	 * <li>gp_param1 (authority) - OAuth authority URL</li>
	 * <li>gp_param2 (tenant) - Tenant GUID</li>
	 * <li>gp_param4 (clientid) - Application/Client ID</li>
	 * <li>gp_param5 (scope) - Permission scope</li>
	 * <li>gp_pwparam1 (secret) - Client secret</li>
	 * <li>gp_param7 (proxyhost) - Proxy host</li>
	 * <li>gp_param8 (proxyport) - Proxy port</li>
	 * <li>gp_param9 (sslproxy) - SSL proxy flag</li>
	 * <li>gp_param10 (proxyusername) - Proxy username</li>
	 * <li>gp_pwparam2 (proxypassword) - Proxy password</li>
	 * </ul>
	 * <p>
	 * <b>NOT included:</b>
	 * <ul>
	 * <li>gp_param3 (tenantname) - Display name only</li>
	 * <li>gp_param6 (connecteddomain) - Not used for connection</li>
	 * </ul>
	 * 
	 * @return Array of parameter keys relevant for connection identity
	 */
	@Override
	public String[] getRelevantParameterKeys() {
		return new String[] { "gp_param1", // authority
				"gp_param2", // tenant
				"gp_param4", // clientid
				"gp_param5", // scope
				"gp_pwparam1", // secret
				"gp_param7", // proxyhost
				"gp_param8", // proxyport
				"gp_param9", // sslproxy
				"gp_param10", // proxyusername
				"gp_pwparam2" // proxypassword
		};
	}

	/**
	 * Helper method to extract a required parameter.
	 * Throws exception with clear message if parameter is missing.
	 */
	private String getRequired(Map<String, String> parameters, String key, String displayName) throws Exception {
		String value = parameters.get(key);

		if (value == null || value.isBlank()) {
			String errorMsg = String.format("Required parameter '%s' (%s) is missing or empty", displayName, key);
			log.error(errorMsg);
			throw new Exception(errorMsg);
		}

		return value;
	}
}