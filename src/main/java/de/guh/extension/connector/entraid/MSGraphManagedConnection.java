package de.guh.extension.connector.entraid;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.guh.connector.core.connection.ManagedConnection;
import de.guh.plugin.msgraph.MSGraphSession;

/**
 * Managed connection wrapper for MSGraphSession.
 * <p>
 * Implements the ManagedConnection interface to enable connection pooling
 * for Microsoft Graph API sessions. This wrapper allows MSGraphSession
 * instances
 * to be reused across multiple data collection operations.
 * <p>
 * <b>Usage:</b>
 * 
 * <pre>
 * MSGraphManagedConnection managedConn = connectionManager.getConnection(
 *     params,
 *     MSGraphManagedConnection.class,
 *     new MSGraphConnectionFactory()
 * );
 * 
 * MSGraphSession session = managedConn.getSession();
 * MSGraphResponse response = session.sendRequest(...);
 * 
 * // Don't close! Pool manages lifecycle
 * </pre>
 * 
 * @since 2.2.11-SNAPSHOT
 */
public class MSGraphManagedConnection implements ManagedConnection {

	private static final Logger log = LoggerFactory.getLogger(MSGraphManagedConnection.class);

	private final MSGraphSession session;
	private final long creationTime;

	/**
	 * Creates a new managed connection wrapping an MSGraphSession.
	 * 
	 * @param session The MSGraphSession to wrap
	 */
	public MSGraphManagedConnection(MSGraphSession session) {
		this.session		= session;
		this.creationTime	= System.currentTimeMillis();
		log.debug("MSGraphManagedConnection created");
	}

	/**
	 * Returns the wrapped MSGraphSession for use in extensions.
	 * 
	 * @return The MSGraphSession instance
	 */
	public MSGraphSession getSession() {
		return session;
	}

	/**
	 * Returns the underlying MSGraphSession object.
	 * <p>
	 * This is the same as {@link #getSession()} but implements
	 * the ManagedConnection interface requirement.
	 * 
	 * @return The MSGraphSession instance
	 */
	@Override
	public Object getUnderlyingConnection() {
		return session;
	}

	/**
	 * Checks if the connection is still valid.
	 * <p>
	 * A connection is considered valid if:
	 * <ul>
	 * <li>The session is ready (status == "ready")</li>
	 * <li>The session has no error status</li>
	 * </ul>
	 * 
	 * @return true if the connection is valid and can be reused
	 */
	@Override
	public boolean isValid() {
		boolean ready = session.isReady();
		boolean noError = !session.hasStatusError();

		boolean valid = ready && noError;

		if (!valid) {
			log.debug("MSGraphManagedConnection is invalid - ready: {}, hasError: {}", ready, session.hasStatusError());
		}

		return valid;
	}

	/**
	 * Closes the connection and releases resources.
	 * <p>
	 * <b>Note:</b> MSGraphSession is stateless (uses OAuth tokens with HTTP),
	 * so there's no actual connection to close. This method is primarily for
	 * logging and future extensibility.
	 * <p>
	 * If token refresh or explicit cleanup is needed in the future,
	 * it would be implemented here.
	 */
	@Override
	public void close() {
		long lifetime = System.currentTimeMillis() - creationTime;
		log.debug("MSGraphManagedConnection closed (lifetime: {} ms)", lifetime);

		// MSGraphSession is stateless - no explicit close needed
		// Token cleanup or resource release could be added here if needed
	}

	/**
	 * Returns debug information about this connection.
	 * 
	 * @return String representation with status and lifetime
	 */
	@Override
	public String toString() {
		long lifetime = System.currentTimeMillis() - creationTime;
		return String.format("MSGraphManagedConnection[status=%s, lifetime=%dms]", session.getStatus(), lifetime);
	}
}