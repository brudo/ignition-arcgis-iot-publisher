# Ignition ArcGIS IoT Publisher

The Ignition ArcGIS IoT Publisher is a command line utility, capable of running as an unattended batch job, which performs a query against a Google BigQuery data source such as the Geotab Ignition datasets, and sends the results to corresponding ArcGIS Online or ArcGIS Server based Feature Services, and/or to an MQTT message broker for use with Analytics for IoT or GeoEvent Server.

    Usage: IgnitionArcGISPublisher [options]

    Options:
      -r|--bigquery-project       Project ID for BigQuery client
      -s|--account-json-file      Path to service account .json file used for authorization
      -q|--query-sql-file         Path to SQL query file - if specified, takes precedence over constructing a query from
                                  other arguments. All listed fields must be included in the defined SQL, aliased if
                                  necessary to match exactly with the target layer, case insensitive
      -Q|--show-query             Write query to standard output without executing - in this mode no external connections
                                  are made
      -d|--query-dataset          Fully-qualified name of the table being queried (if SQL query file is not specified); e.g.
                                  geotab-public-intelligence.COVIDMobilityImpact.PortTrafficAnalysis
      -w|--query-where            An optional where clause for BigQuery - not applicable if SQL query file is specified
      -o|--query-order            An optional list of fields used as the order by clause for BigQuery - not applicable if
                                  SQL query file is specified.
      -m|--query-limit            An optional limit on the number of records to be considered - not applicable if SQL query
                                  file is specified.
                                  Default value is: 0.
      -R|--arcgis-root-url        ArcGIS root URL (e.g. https://<org>.maps.arcgis.com/)
      -U|--arcgis-user            ArcGIS user name. If not specified, this means connect with no credentials
      -P|--arcgis-password        ArcGIS password. If not specified, but user was specified, the password must be provided
                                  as input
      -L|--arcgis-layer-path      Path of ArcGIS layer (relative to rest/services) where the incremental records, or all
                                  records should be appended
      -C|--arcgis-recent-path     Path of an ArcGIS layer (relative to rest/services) to be maintained with only the most
                                  recent record for each location
      -c|--base-on-recent         Use recent layer as base for incremental results; otherwise a statistical query against
                                  the main output layer to find the largest / latest value on each track
      -S|--arcgis-server-auth     ArcGIS Server authentication indicator (otherwise, by default assume ArcGIS Online)
      -D|--arcgis-delete-all      ArcGIS option to delete all existing features - if not combined with append-all-features,
                                  only perform the delete, and do not run the query
      -A|--arcgis-append-all      ArcGIS option to append all features matching the query - otherwise it will only append
                                  features that are more recent than the last feature for that track
      -f|--attribute-fields       Comma delimited list of fields to transcribe from BigQuery result. For an externally
                                  defined query --sql-file, it must match the case returned by BigQuery. To work with
                                  Velocity gRPC Feeds and HTTP POST CSV support, the fields must be listed in the same order
                                  as the feed definition.
      -x|--longitude-field        Name of the longitude or X field in BigQuery, used to construct Point geometry (only WGS84
                                  is supported). If the outputs include GRPC, the derived geometry field is added as a
                                  single field containing Esri JSON.
      -y|--latitude-field         Name of the latitude or Y field in BigQuery, used to construct Point geometry (only WGS84
                                  is supported). If the outputs include GRPC, the derived geometry field is added as a
                                  single field containing Esri JSON.
      -t|--track-field            Name of unique track ID field used for incremental results. Must be included in the
                                  attribute fields
      -l|--latest-field           Name of field used for incremental results. Must be included in the listed attribute
                                  fields.
      -Z|--dry-run                Perform dry run, testing arguments and running the query, still reading all rows, but not
                                  producing any output (except to console, if specified).
      --http-output-url           HTTP output URL where JSON features should be posted. The features will be posted in
                                  batches, where records are newline-delimted, zero-whitespace JSON documents
      --http-use-arcgis-token     Indicates to pass the token obtained for the ArcGIS username and password in an
                                  Authorization: Bearer header. Assuming the endpoint is an HTTP Receiver Feed hosted by
                                  Velocity, this must be an ArcGIS Online token for the owner of the feed.
      --http-output-generic-json  Indicates to post generic JSON rather than Esri JSON for the events. In this case,
                                  Geometry is omitted but the latitude and longitude fields, if any, are always included.
      --grpc-feed-channel-url     URL (usually prefixed https:// and without a path) for the generic GRPC Channel in ArcGIS
                                  Velocity where features should be sent. Currently this functionality is not yet released
                                  in Velocity; the interface may be subject to change.
      --grpc-feed-header-path     Path provided in a gRPC header as published by the Velocity feed. Currently this
                                  functionality is in beta within Velocity, and may be subject to change. The order of the
                                  fields listed in '--attribute-fields' must match the order defined in the Feed in
                                  Velocity.
      --grpc-feed-auth-token      Optional token to include in an Authorization: Bearer header for authenticated GRPC
                                  communication. The special value 'arcgis' signals to use the token obtained for the ArcGIS
                                  username and password if provided. As the GRPC channel is hosted by an ArcGIS Velocity
                                  Feed, this must be a ArcGIS Online token for the authenticated user.
      --grpc-feed-omit-geometry   By default, if the latitude and longitude fields are specified for the purpose of deriving
                                  geometry, the geometry value is included in the gRPC output, as an additional field. This
                                  flag will suppress this behaviour. This is only useful if outputting to multiple targets
                                  and if the if the derived geometry is needed for one of the other targets, e.g. feature
                                  JSON.
      --mqtt-topic                MQTT topic where JSON features should be published
      --mqtt-broker               MQTT broker URL, e.g. tcp://localhost:1883
      --mqtt-user                 MQTT broker login user name - otherwise, if MQTT topic is specified, an anonymous
                                  connection is attempted
      --mqtt-password             MQTT broker login password - otherwise, if MQTT topic and user are specified, you will be
                                  prompted
      --mqtt-client-id            MQTT client ID - if not specified when MQTT topic is provided, an arbitrary
                                  application-selected client ID is used
      --mqtt-clean-session        MQTT clean session option
      --mqtt-retain-fanout        MQTT fanout and retain last message per track - messages will be sent individually, to a
                                  subtopic based on the incremental track field, with the retain flag set, such that the
                                  latest message for all tracks can be received on initial connection, when combined with a
                                  wildcard topic. If this option is not chosen, the messages are sent in batches to the root
                                  topic.
      --mqtt-generic-json         Use generic JSON rather than ESRI JSON for MQTT output
      -?|-h|--help                Show help information.
