K2 Extractor
=============

K2 is an ERP information system for managing production, inventory, financials, and more

This component fetches defined data objects from the K2 API

**Table of contents:**

[TOC]

Prerequisites
=============

[Whitelist the Keboola IP adresses](https://help.keboola.com/components/ip-addresses/) if necessary, or setup an ssh
tunnel.

Configuration
=============

### K2 Authorization Configuration

- Username (username) - [REQ] Username for K2
- Password (#password) - [REQ] Password for K2
- Service name (service_name) - [REQ] Name of the K2 service  http://myk2server.com/{SERVICE_NAME}
- Use ssh (use_ssh) - [REQ] If True, an SSH tunnel will be used to call the API
- SSH Tunnel configuration (ssh) - [OPT] Dictionary holding the following
    - SSH Username (username) - [REQ] The SSH User for connecting to your SSH server
    - SSH Private Key (#private_key) - [REQ] The base64-encoded private key for the key pair associated with your SSH
      server
    - SSH Tunnel Host (tunnel_host) - [REQ] The host name or host IP associated with your SSH server (Note: Don't use
      load balancer as host)
    - SSH Remote Address (remote_address) - [REQ] The address that is used to query the K2 API e.g. k2api.myfirm.cz
    - SSH Remote Port (remote_port) - [REQ] The port of the K2 API e.g. 8080
- Source url (source_url) - [OPT] The address that is used to query the K2 API, e.g. https://myk2server.com, necessary
  when not using SSH

### Configuration row

- Data object (data_object) - [REQ] Name of object to download from K2
- Fields (fields) - [OPT] A list of the names of data fields that should be fetched, separated by comma
- Conditions (conditions) - [OPT] Conditions used for filtering data. Using the format defined in
  the <a href='https://help.k2.cz/k2ori/02/en/10023272.htm#o106273'>K2 documentation</a>
- Loading Options (loading_options) - [REQ] dictionary holding the following :
    - Load Type (load_type) - [REQ] "Full Load" or "Incremental load" If set to Incremental load, the result tables will
      be updated based on primary key and new records will be fetched. Full load overwrites the destination table each
      time.
    - Incremental Field (incremental_field) - [OPT] Field from the K2 Object that should be used for incremental
      fetching; e.g. Timestamp
    - Date From (date_from)- [OPT] What date to fetch data from using the incremental field; either exact date in
      YYYY-MM-DD format, relative date e.g. 3 days ago, or last run to fetch data since last run
    - Date To (date_to)- [OPT] What date to fetch data to using the incremental field; either exact date in YYYY-MM-DD
      format, relative date e.g. 3 days ago, or now to fetch data till the current time

Developer notes
===========

This component is used for downloading data objects from K2. Each configuration is for downloading a single data object
with some specified fields that should be downloaded. Fields that should be downloaded are either properties of an
object or child objects of the object you are downloading.

To explain this further, you are downloading data about cars from K2. You download the object name : "Car" with the
fields
"ID, Brand, Model, YearOfBuild, Components, Components.Manufacturer"

The Brand, Model and YearOfBuild are fields of the Car object and Components is a child object, and Manufacturer is a
child object of Components

you will then have 3 tables:
Car : [ID, Brand, Model, YearOfBuild]
Component : [Car_ID, ID, ComponentName, ComponentType, ComponentCost]
Component_Manufacturer : [Car_ID, Component_ID, ID, ManufacturerName, ManufacturerCountry]


Sample Configuration
=============

```json
{
  "parameters": {
    "username": "USERNAME",
    "#password": "SECRET_VALUE",
    "service_name": "API",
    "data_object": "OBJECT_IN_K2",
    "incremental": true,
    "fields": "",
    "conditions": "",
    "use_ssh": true,
    "ssh": {
      "username": "SSH_USERNAME",
      "#private_key": "BASE64 encoded private key",
      "tunnel_host": "tunnel-host.com",
      "remote_address": "k2api.myservice.com",
      "remote_port": "8080"
    },
    "source_url": "http://k2api.myapi.com:8080/API",
    "loading_options": {
      "load_type": "Incremental load",
      "incremental_field": "Timestamp",
      "date_from": "last",
      "date_to": "now"
    }
  },
  "action": "run"
}
```

Development
-----------

If required, change local data folder (the `CUSTOM_FOLDER` placeholder) path to your custom path in
the `docker-compose.yml` file:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Clone this repository, init the workspace and run the component with following command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
docker-compose build
docker-compose run --rm dev
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run the test suite and lint check using this command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
docker-compose run --rm test
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Integration
===========

For information about deployment and integration with KBC, please refer to the
[deployment section of developers documentation](https://developers.keboola.com/extend/component/deployment/)

