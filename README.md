# daccord JavaConnector Extension – Microsoft Entra ID

A [daccord](https://www.daccord.de) JavaConnector Extension that reads identity data from **Microsoft Entra ID** (formerly Azure AD) via the **Microsoft Graph API** and imports it into daccord.

## What it does

- Reads **users** and **groups** from Entra ID via Microsoft Graph
- Supports **subqueries** – e.g., fetch user attributes and group memberships in one pass
- Handles **pagination** automatically (follows `@odata.nextLink`)
- Supports **whitelist / blacklist rules** to filter imported objects
- Authenticates via **OAuth2 Client Credentials** (MSAL4J)
- Uses a **connection pool** with automatic retry on token expiry

## Prerequisites

- Java 11
- Maven 3.9+
- daccord with JavaConnector enabled
- An **App Registration** in Microsoft Entra ID with:
  - Application (client) ID
  - Client secret
  - API permission: `User.Read.All`, `Group.Read.All` (or equivalent), granted as **application permissions**

## Building

```bash
mvn clean package -q
```

The runnable JAR is built to `target/daccord-javaconnector-entraid-extension-1.0.0-SNAPSHOT-runnable.jar`.

## Configuration

### Connection parameters (`gpconnect.xml`)

| Parameter | Description | Example |
|-----------|-------------|---------|
| `gp_param1` | Authority URL | `https://login.microsoftonline.com/` |
| `gp_param2` | Tenant ID | `YOUR_TENANT_ID` |
| `gp_param3` | Tenant name (domain) | `yourdomain.com` |
| `gp_param4` | Client ID | `YOUR_CLIENT_ID` |
| `gp_param5` | Scope | `https://graph.microsoft.com/.default` |
| `gp_param6` | Connected domain (AD domain name) | `YOURDOMAIN` |
| `gp_param7` | Proxy host (optional) | |
| `gp_param8` | Proxy port (optional) | |
| `gp_param9` | SSL proxy (optional) | `true` / `false` |
| `gp_param10` | Proxy username (optional) | |
| `gp_pwparam1` | **Client secret** (encrypted by daccord) | |
| `gp_pwparam2` | Proxy password (optional) | |

See `example/gpconnect.xml` for a template.

### Node collection parameters (`gpnodecollection.xml`)

Used when importing **node data** (users, groups, etc.).

| Parameter | Description | Example |
|-----------|-------------|---------|
| `np_param1` | Object type | `user` |
| `np_param2` | Graph resource path | `users` |
| `np_param3` | Unique attribute | `id` |
| `np_param4` | `$select` – attributes to import (comma-separated) | `id,displayName,mail,...` |
| `np_param5` | `$filter` (optional) | `accountEnabled eq true` |
| `np_param6` | `$expand` (optional) | |
| `np_param7` | Subquery resource path (optional) | `users/$id/memberOf/microsoft.graph.group` |
| `np_param8` | Subquery `$select` | `displayName` |
| `np_param9` | Subquery `$filter` (optional) | |
| `np_param10` | Subquery attribute prefix | `memberof-` |

The `$id` placeholder in the subquery path is replaced with the unique attribute value of each object.

See `example/gpnodecollection.xml` (users) and `example/gpnodecollection-group.xml` (groups).

### Relation collection parameters (`gprelationcollection.xml`)

Used when importing **relation data** (e.g., user-to-group memberships).

| Parameter | Description |
|-----------|-------------|
| `rp_param1` | Object type |
| `rp_param2` | Graph resource path (source objects) |
| `rp_param3` | `$select` for source objects |
| `rp_param4` | Source unique attribute |
| `rp_param5` | `$filter` (optional) |
| `rp_param6` | Subquery resource path (target objects) |
| `rp_param7` | Subquery `$select` |
| `rp_param8` | Subquery `$filter` (optional) |
| `rp_param9` | Subquery attribute prefix |

One relation line is written per source–target pair.

See `example/gprelationcollection.xml` and `example/gprelationpreview.xml`.

## Running (standalone test)

```bash
java -Dlog4j2.level=DEBUG \
  -jar target/daccord-javaconnector-entraid-extension-1.0.0-SNAPSHOT-runnable.jar \
  getnodedata \
  example/gpconnect.xml \
  example/gpnodecollection.xml \
  /output/path
```

> **Note:** Always include `-Dlog4j2.level=DEBUG`, otherwise there is no console output.

Available commands: `getnodedata`, `getnodepreview`, `getrelationdata`, `getrelationpreview`, `checkconnection`.

## Example: Import users with group memberships

`gpnodecollection.xml`:
```xml
<np_param2>users</np_param2>                           <!-- resource path -->
<np_param3>id</np_param3>                              <!-- unique attribute -->
<np_param4>id,displayName,mail,accountEnabled,...</np_param4>
<np_param7>users/$id/memberOf/microsoft.graph.group</np_param7>
<np_param8>displayName</np_param8>
<np_param10>memberof-</np_param10>
```

This fetches all users and, for each user, their group memberships. The group `displayName` is added as `memberof-displayName` to the node data.

## Whitelist / Blacklist

Node and relation collections support rule-based filtering:

```xml
<whitelist>
    <condition>
        <group_nexus>OR</group_nexus>
        <group_internal_nexus>AND</group_internal_nexus>
        <groups>
            <group>
                <rules>
                    <rule>
                        <attribute>memberof-displayName</attribute>
                        <operator>EQUALS</operator>
                        <value>dac-admin</value>
                    </rule>
                </rules>
            </group>
        </groups>
    </condition>
</whitelist>
```

## Project structure

```
src/main/java/de/guh/extension/connector/entraid/
├── EntraIDJavaConnectorExtension.java   – Main extension (implements JavaConnectorExtension)
├── MSGraphConnectionFactory.java        – Creates MSGraph connections for the pool
├── MSGraphManagedConnection.java        – Pooled connection wrapper
├── DataHelper.java                      – Utility methods
└── Main.java                            – Standalone test runner

example/
├── gpconnect.xml                        – Connection parameters template
├── gpnodecollection.xml                 – Node collection: users
├── gpnodecollection-group.xml           – Node collection: groups
├── gpnodepreview.xml                    – Node preview: users
├── gpnodepreview-group.xml              – Node preview: groups
├── gprelationcollection.xml             – Relation collection
├── gprelationpreview.xml                – Relation preview
├── account-create.xml                   – Account provisioning template
└── permission-create.xml                – Permission provisioning template
```

## License

Copyright © G+H Systems GmbH. All rights reserved.
