package de.guh.extension.connector.entraid;

import java.util.HashMap;
import java.util.List;

import org.apache.logging.log4j.Logger;

import de.guh.gadget.Gadget;
import de.guh.plugin.security.Encryption;
import de.guh.plugin.xml.XML;

public class DataHelper {

	public static HashMap<String, String> getParameterHashMap(Logger thislog, XML xmlParameters, String xpath) {

		// if (hmParameters == null) hmParameters = new HashMap<String, String>();
		HashMap<String, String> hmParameters = new HashMap<String, String>();
		List<XML> listXml = xmlParameters.findNodesByXPath(xpath);
		if (listXml.size() > 0) {

			listXml.stream().forEach(p -> {

				String name = p.getAttribute("name");
				String type = p.getAttribute("type");
				String value = p.getText();
				thislog.debug("...reading parameter: " + name +
					" with type: " +
					type +
					" and value: " +
					value);

				if (type != null && type.equalsIgnoreCase("password")) {
					try {
						value = Encryption.decrypt(value, Gadget.gdConfig.getParameter("decryptionhash"));
					} catch (Exception e) {
						thislog.debug("ERROR_READING_PARAMETERS", e);
					}
				}
				hmParameters.put(name, value);
			});
		}

		return hmParameters;
	}

	public static HashMap<String, String> getParameterHashMapAttributes(Logger thislog, XML xmlParameters, String xpath) {

		// if (hmParameters == null) hmParameters = new HashMap<String, String>();
		HashMap<String, String> hmParameters = new HashMap<String, String>();
		List<XML> listXml = xmlParameters.findNodesByXPath(xpath);
		thislog.debug("...reading xpath: " + xpath);
		thislog.debug("...reading listXml: " + listXml);
		if (listXml.size() > 0) {

			listXml.stream().forEach(p -> {

				String name = p.getAttribute("name");
				String type = p.getAttribute("type");
				String value = p.getFirstChild().getText();
				thislog.debug("...reading parameter: " + name +
					" with type: " +
					type +
					" and value: " +
					value);

				if (type != null && type.equalsIgnoreCase("password")) {
					try {
						value = Encryption.decrypt(value, Gadget.gdConfig.getParameter("decryptionhash"));
					} catch (Exception e) {
						thislog.debug("ERROR_READING_PARAMETERS", e);
					}
				}
				hmParameters.put(name, value);
			});
		}

		return hmParameters;
	}

}
