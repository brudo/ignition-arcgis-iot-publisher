# Ignition ArcGIS IoT Publisher

The Ignition ArcGIS IoT Publisher is a command line utility, capable of running as an unattended batch job, which performs a query against a Google BigQuery data source such as the Geotab Ignition datasets, and sends the results to corresponding ArcGIS Online or ArcGIS Server based Feature Services, and/or to an MQTT message broker for use with Analytics for IoT or GeoEvent Server.

    Usage: IgnitionArcGISPublisher [options]

    Options:
      -r|--bigquery-project    Project ID for BigQuery client
      -s|--account-json-file   Path to service account .json file used for authorization
      -q|--query-sql-file      Path to SQL query file - if specified, takes precedence over constructing a query from other
                               arguments. All listed fields must be listed in the defined SQL, aliased if necessary to match
                               exactly with the target layer, case insensitive
      -Q|--show-query          Output query to standard output without executing - in this mode no queries are executed and
                               all other queries are suppressed
      -d|--query-dataset       Fully-qualified name of the table being queried (if SQL query file is not specified); e.g.
                               geotab-public-intelligence.COVIDMobilityImpact.PortTrafficAnalysis
      -w|--query-where         An optional where clause for BigQuery - not applicable if SQL query file is specified
      -o|--query-order         An optional list of fields used as the order by clause for BigQuery - not applicable if SQL
                               query file is specified.
      -m|--query-limit         An optional limit on the number of records to be considered - not applicable if .
      -R|--arcgis-root-url     ArcGIS root URL (e.g. https://services.arcgis.com/.../arcgis)
      -U|--arcgis-user         ArcGIS user name. If not specified, this means connect with no credentials
      -P|--arcgis-password     ArcGIS password. If not specified, but user was specified, the password must be provided as
                               input
      -L|--arcgis-layer-path   Path of ArcGIS layer (relative to rest/services) where the incremental records, or all records
                               should be appended
      -C|--arcgis-recent-path  Path of an ArcGIS layer (relative to rest/services) to be maintained with only the most recent
                               record for each location
      -c|--base-on-recent      Use recent layer as base for incremental results; otherwise a statistical query against the
                               main output layer to find the largest / latest value on each track
      -S|--arcgis-server-auth  ArcGIS Server authentication indicator (otherwise, by default assume ArcGIS Online)
      -D|--arcgis-delete-all   ArcGIS option to delete all existing features - if not combined with append-all-features, only
                               perform the delete, and do not run the query
      -A|--arcgis-append-all   ArcGIS option to append all features matching the query - otherwise it will only append
                               features that are more recent than the last feature for that track
      -f|--attribute-fields    Comma delimited list of fields to transcribe from BigQuery result. For an externally defined
                               query --sql-file, it must match the case returned by BigQuery
      -x|--longitude-field     Name of the longitude or X field in BigQuery, used to construct Point geometry (only WGS84 is
                               supported)
      -y|--latitude-field      Name of the latitude or Y field in BigQuery, used to construct Point geometry (only WGS84 is
                               supported)
      -t|--track-field         Name of unique track ID field used for incremental results. Must be included in the attribute
                               fields
      -l|--latest-field        Name of field used for incremental results. Must be included in the listed attribute fields.
      -Z|--dry-run             Perform dry run, testing arguments and running the query, still reading all rows, but not
                               producing any output.
      --mqtt-topic             MQTT topic where JSON features should be published
      --mqtt-single            MQTT single message mode - messages will be sent individually; otherwise they are sent in
                               batches
      --mqtt-broker            MQTT broker URL, e.g. tcp://localhost:1883
      --mqtt-user              MQTT broker login user name - otherwise, if MQTT topic is specified, an anonymous connection
                               is attempted
      --mqtt-password          MQTT broker login password - otherwise, if MQTT topic and user are specified, you will be
                               prompted
      --mqtt-client-id         MQTT client ID - if not specified when MQTT topic is provided, an arbitrary
                               application-selected client ID is used
      --mqtt-clean-session     MQTT clean session option
      -?|-h|--help             Show help information
