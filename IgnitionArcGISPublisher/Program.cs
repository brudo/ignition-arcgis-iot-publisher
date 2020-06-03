﻿// Ignition ArcGIS IoT Publisher
// Copyright (c) 2020 Esri Canada

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;

using Anywhere.ArcGIS;
using Anywhere.ArcGIS.Operation;
using Anywhere.ArcGIS.Common;

using Google.Apis.Auth.OAuth2;
using Google.Apis.Bigquery.v2.Data;

using McMaster.Extensions.CommandLineUtils;

using MQTTnet;
using MQTTnet.Client.Options;
using MQTTnet.Client.Connecting;
using MQTTnet.Client.Publishing;
using MQTTnet.Extensions.ManagedClient;

using Newtonsoft.Json; // Anywhere.ArcGIS uses DataContract, not supported by System.Text.Json

using Google.Cloud.BigQuery.V2;

// Note that Anywhere.ArcGIS is not able to write directly to an Analytics for IoT hosted
// feature service, due to some incompatible URL path manipulation it does under the hood,
// resulting in an exception being thrown, which cryptically reports a JSON reader error
// (because the result being parsed is an HTML error page)
//
// One thing it does quite well is to request and refresh a token for Server or Online. If
// low-level JSON REST calls were used instead, this token facility could still be used.

namespace IgnitionIoTMessagePublisher
{
    class Program
    {
        #region CommandLineOptions
        [Option("-r|--bigquery-project", Description = "Project ID for BigQuery client")]
        public string ProjectId { get; set; }

        [Option("-s|--account-json-file", Description = "Path to service account .json file used for authorization")]
        public string AccountJsonFile { get; set; }

        [Option("-q|--query-sql-file", Description = "Path to SQL query file - if specified, takes precedence over constructing a query from other arguments. All listed fields must be listed in the defined SQL, aliased if necessary to match exactly with the target layer, case insensitive")]
        public string QuerySqlFile { get; set; }

        [Option("-Q|--show-query", Description = "Output query to standard output without executing - in this mode no external connections are made")]
        public bool OutputQuerySql { get; set; }

        [Option("-d|--query-dataset", Description = "Fully-qualified name of the table being queried (if SQL query file is not specified); e.g. geotab-public-intelligence.COVIDMobilityImpact.PortTrafficAnalysis")]
        public string QueryDataset { get; set; }

        [Option("-w|--query-where", Description = "An optional where clause for BigQuery - not applicable if SQL query file is specified")]
        public string QueryWhereClause { get; set; }

        [Option("-o|--query-order", Description = "An optional list of fields used as the order by clause for BigQuery - not applicable if SQL query file is specified.")]
        public string QueryOrderBy { get; set; }

        [Option("-m|--query-limit", Description = "An optional limit on the number of records to be considered - not applicable if .")]
        public int QueryLimit { get; set; }

        [Option("-R|--arcgis-root-url", Description = "ArcGIS root URL (e.g. https://services.arcgis.com/.../arcgis)")]
        public string ArcGISRootURL { get; set; }

        [Option("-U|--arcgis-user", Description = "ArcGIS user name. If not specified, this means connect with no credentials")]
        public string ArcGISUser { get; set; }

        [Option("-P|--arcgis-password", Description = "ArcGIS password. If not specified, but user was specified, the password must be provided as input")]
        public string ArcGISPassword { get; set; }

        [Option("-L|--arcgis-layer-path", Description = "Path of ArcGIS layer (relative to rest/services) where the incremental records, or all records should be appended")]
        public string ArcGISLayerPath { get; set; }

        [Option("-C|--arcgis-recent-path", Description = "Path of an ArcGIS layer (relative to rest/services) to be maintained with only the most recent record for each location")]
        public string ArcGISRecentPath { get; set; }

        [Option("-c|--base-on-recent", Description = "Use recent layer as base for incremental results; otherwise a statistical query against the main output layer to find the largest / latest value on each track")]
        public bool IncrementalBaseOnRecent { get; set; }

        [Option("-S|--arcgis-server-auth", Description = "ArcGIS Server authentication indicator (otherwise, by default assume ArcGIS Online)")]
        public bool IsArcGISServerAuth { get; set; }

        //Instead, map the fields within the SQL itself, or hard code for now
        //[Option('F', "arcgis-map-fields", Description = "Field mapping from BigQuery results to ArcGIS output")]
        //public string FieldMapDefinition { get; set; }

        [Option("-D|--arcgis-delete-all", Description = "ArcGIS option to delete all existing features - if not combined with append-all-features, only perform the delete, and do not run the query")]
        public bool ArcGISDeleteAll { get; set; }

        [Option("-A|--arcgis-append-all", Description = "ArcGIS option to append all features matching the query - otherwise it will only append features that are more recent than the last feature for that track")]
        public bool ArcGISAppendAll { get; set; }

        [Option("-f|--attribute-fields", Description = "Comma delimited list of fields to transcribe from BigQuery result. For an externally defined query --sql-file, it must match the case returned by BigQuery")]
        public string AttributeFields { get; set; }

        [Option("-x|--longitude-field", Description = "Name of the longitude or X field in BigQuery, used to construct Point geometry (only WGS84 is supported)")]
        public string LongitudeField { get; set; }

        [Option("-y|--latitude-field", Description = "Name of the latitude or Y field in BigQuery, used to construct Point geometry (only WGS84 is supported)")]
        public string LatitudeField { get; set; }

        [Option("-t|--track-field",  Description = "Name of unique track ID field used for incremental results. Must be included in the attribute fields")]
        public string IncrementalTrackField { get; set; }

        [Option("-l|--latest-field", Description = "Name of field used for incremental results. Must be included in the listed attribute fields.")]
        public string IncrementalLatestField { get; set; }

        //[Option("--scale-fields", Description = "Comma delimited list of fields that require rescaling to line up with previous or latest record for the track" )]
        //public string ScaleFields { get; set; }

        [Option("-Z|--dry-run", Description = "Perform dry run, testing arguments and running the query, still reading all rows, but not producing any output.")]
        public bool DryRun { get; set; }

        //[Option("--http-output-url", Description = "HTTP output URL where JSON features should be posted")]
        //public string HttpOutputUrl { get; set; }

        //[Option("--http-output-auth", Description = "HTTP output URL where JSON features should be posted")]
        //public bool HttpOutputToken { get; set; }

        [Option("--mqtt-topic", Description = "MQTT topic where JSON features should be published")]
        public string BrokerTopic { get; set; }

        [Option("--mqtt-broker", Description = "MQTT broker URL, e.g. tcp://localhost:1883")]
        public string BrokerUrl
        {
            get => $@"{(BrokerScheme?.Length > 0 ?
                BrokerScheme + "://" : null)}{BrokerHost}{(
                    BrokerPort > 0 ? $":{BrokerPort}" : null)}";

            set
            {
                if (Uri.TryCreate(value, UriKind.Absolute, out var uri))
                {
                    switch (uri.Scheme)
                    {
                        case "mqtt":
                        case "tcp":
                            BrokerScheme = "mqtt";
                            break;
                        case "mqtts":
                        case "ssl":
                        case "tls":
                            BrokerScheme = "mqtts";
                            break;
                        default:
                            Console.WriteLine("Error - unsupported broker scheme {0}", uri.Scheme);
                            BrokerScheme = null;
                            break;
                    }

                    BrokerHost = uri.Host;
                    BrokerPort = (ushort)(!uri.IsDefaultPort && uri.Port > 0 ?
                        uri.Port : BrokerScheme == "mqtts" ? 8883 : 1883);
                }
                else
                {
                    BrokerScheme = "mqtt";
                    BrokerHost = "localhost";
                    BrokerPort = 1883;
                }
            }
        }
        public string BrokerScheme { get; set; }

        public string BrokerHost { get; set; }

        public ushort BrokerPort { get; set; }

        [Option("--mqtt-user", Description = "MQTT broker login user name - otherwise, if MQTT topic is specified, an anonymous connection is attempted")]
        public string BrokerUser { get; set; }

        [Option("--mqtt-password", Description = "MQTT broker login password - otherwise, if MQTT topic and user are specified, you will be prompted")]
        public string BrokerPassword { get; set; }

        [Option("--mqtt-client-id", Description = "MQTT client ID - if not specified when MQTT topic is provided, an arbitrary application-selected client ID is used")]
        public string MqttClientId { get; set; }

        [Option("--mqtt-clean-session", Description = "MQTT clean session option")]
        public bool MqttCleanSession{ get; set; }

        [Option("--mqtt-retain-fanout", Description = "MQTT fanout and retain last message per track - messages will be sent individually, to a subtopic based on the incremental track field, with the retain flag set. If this option is not chosen, the messages are sent in batches to the root topic.")]
        public bool MqttRetainFanout { get; set; }

        [Option("--mqtt-generic-json", Description = "Use generic JSON rather than ESRI JSON for MQTT output")]
        public bool MqttGenericJson { get; set; }

        #endregion


        internal async Task<int> OnExecuteAsync(CommandLineApplication _, CancellationToken ct = default)
        {
            bool deleteOnly = ArcGISDeleteAll && !ArcGISAppendAll;

            // If MQTT will be required, initialize it up-front.
            // Same for the token for ArcGIS Online, now initialized through PreStorm

            if (OutputQuerySql || File.Exists(AccountJsonFile) && !string.IsNullOrEmpty(ProjectId))
            {
                // disposed in finally block
                IManagedMqttClient mqttClient = null;
                TokenProvider tokenProvider = null;
                PortalGateway portalGateway = null;

                ServiceLayerDescriptionResponse layerDesc = null, recentDesc = null;

                HashSet<string> mainLayerFields = null, recentLayerFields = null;

                var attributeFieldSet = AttributeFields?
                    .Split(',').Select(s => s.Trim()).Where(s => s.Length > 0)
                    .ToHashSet(StringComparer.OrdinalIgnoreCase);

                var queryFieldSet = attributeFieldSet;

                try
                {
                    if (!OutputQuerySql)
                    {
                        if (!string.IsNullOrWhiteSpace(BrokerTopic))
                        {
                            Console.WriteLine("Attempting connection MQTT hub...");
                            mqttClient = await ConnectMqttAsync().ConfigureAwait(false);
                            if (mqttClient == null)
                            {
                                return 5; // error message already written
                            }

                        }

                        if (!string.IsNullOrWhiteSpace(ArcGISRootURL))
                        {
                            Console.WriteLine("Attempting connection to specified ArcGIS URL");
                            if (string.IsNullOrWhiteSpace(ArcGISUser))
                            {
                                // anonymous - e.g. for local ArcGIS Server
                                portalGateway = new PortalGateway(ArcGISRootURL);
                            }
                            else
                            {
                                if (string.IsNullOrWhiteSpace(ArcGISPassword))
                                {
                                    //Console.Write("Supply password for ArcGIS user {0}: ", ArcGISUser);
                                    //ArcGISPassword = ReadPassword();
                                    ArcGISPassword = Prompt.GetPassword($"Supply password for ArcGIS {(IsArcGISServerAuth ? "Server" : "Online")} user {ArcGISUser}: ");
                                }

                                tokenProvider = IsArcGISServerAuth ?
                                    new TokenProvider(ArcGISRootURL, ArcGISUser, ArcGISPassword) :
                                    new ArcGISOnlineTokenProvider(ArcGISUser, ArcGISPassword);

                                portalGateway = new PortalGateway(ArcGISRootURL, tokenProvider: tokenProvider);
                            }

                            portalGateway.HttpRequestTimeout = TimeSpan.FromSeconds(90);

                            if (!string.IsNullOrWhiteSpace(ArcGISLayerPath))
                            {
                                try
                                {
                                    // Credentials are not tested by constructor, but on demand on first use
                                    layerDesc = await portalGateway.DescribeLayer(ArcGISLayerPath.AsEndpoint()).ConfigureAwait(false);

                                    if (layerDesc.Error != null)
                                    {
                                        Console.WriteLine("Error - could not connect to 'main' ArcGIS {0} layer {1}. Aborting",
                                            IsArcGISServerAuth ? "Server" : "Online", ArcGISLayerPath);

                                        Console.WriteLine("Error message: {0}", layerDesc.Error.Description);
                                        return 2;
                                    }
                                }
                                catch (Exception e)
                                {
                                    Console.WriteLine("Error - could not connect to 'main' ArcGIS {0} layer {1}. Aborting",
                                        IsArcGISServerAuth ? "Server" : "Online", ArcGISLayerPath);

                                    Console.WriteLine("Error message: {0}", e.Message);
                                    return 2;
                                }
                            }

                            if (!string.IsNullOrEmpty(ArcGISRecentPath))
                            {
                                try
                                {
                                    // Credentials are not tested by constructor, but on demand on first use
                                    recentDesc = await portalGateway.DescribeLayer(ArcGISRecentPath.AsEndpoint()).ConfigureAwait(false);

                                    if (recentDesc.Error != null)
                                    {
                                        Console.WriteLine("Error - could not connect to 'recent' ArcGIS {0} layer {1}. Aborting",
                                            IsArcGISServerAuth ? "Server" : "Online", ArcGISLayerPath);
                                        Console.WriteLine("Error message: {0}", layerDesc.Error.Description);
                                        return 2;
                                    }
                                }
                                catch (Exception e)
                                {
                                    Console.WriteLine("Error - could not connect to 'recent' ArcGIS {0} layer {1}. Aborting",
                                        IsArcGISServerAuth ? "Server" : "Online", ArcGISLayerPath);

                                    Console.WriteLine("Error message: {0}", e.Message);
                                    return 2;
                                }
                            }
                            else if (IncrementalBaseOnRecent)
                            {
                                Console.WriteLine("Error - recent layer path must be specified when --base-on-recent is specified");
                                return 1;
                            }

                        }

                        ////var results = client.ExecuteQuery(
                        ////    @"SELECT Port, YearWeek, WeekEndDate, VehicleVolumeChangePerc, TripVolumeChangePerc
                        ////    FROM `geotab-public-intelligence.COVIDMobilityImpact.PortTrafficAnalysis`", null);

                        if (!deleteOnly && (attributeFieldSet == null || attributeFieldSet.Count == 0))
                        {
                            Console.WriteLine("Error - attribute fields must be specified, except in delete-only mode.");
                            return 1;
                        }

                        mainLayerFields = SubsetFieldNames(layerDesc?.Fields, attributeFieldSet);
                        recentLayerFields = SubsetFieldNames(recentDesc?.Fields, attributeFieldSet);

                        if (!(string.IsNullOrEmpty(IncrementalTrackField) || string.IsNullOrEmpty(IncrementalLatestField)))
                        {
                            if (ArcGISDeleteAll)
                            {
                                Console.WriteLine("Warning - incremental fields are not applicable in delete-all mode");
                            }
                            else
                            {
                                if (attributeFieldSet?.Contains(IncrementalTrackField) != true)
                                {
                                    Console.WriteLine("Error - incremental track field '{0}' must be included in attribute fields",
                                        IncrementalTrackField);
                                    return 1;
                                }
                                if (attributeFieldSet?.Contains(IncrementalLatestField) != true)
                                {
                                    Console.WriteLine("Error - incremental latest field '{0}' must be included in attribute fields",
                                        IncrementalLatestField);
                                    return 1;
                                }


                                if (recentLayerFields?.Contains(IncrementalTrackField) == false)
                                {
                                    Console.WriteLine("Error - incremental track field '{0}' not found in target layer for most recent data",
                                        IncrementalTrackField);
                                    return 2;
                                }

                                if (IncrementalBaseOnRecent)
                                {
                                    if (recentLayerFields == null)
                                    {
                                        Console.WriteLine("Error - recent layer is required when 'base on recent' option is specified");
                                        return 1;
                                    }

                                    if (!recentLayerFields.Contains(IncrementalLatestField))
                                    {
                                        Console.WriteLine("Error - incremental latest field '{0}' not found in target layer for most recent data - required when incremental based on ",
                                            IncrementalTrackField);
                                        return 2;
                                    }
                                }
                                else
                                {
                                    if (mainLayerFields?.Contains(IncrementalTrackField) == false)
                                    {
                                        Console.WriteLine("Error - incremental track field '{0}' not found in target layer for historical records",
                                            IncrementalTrackField);
                                        return 2;
                                    }
                                    if (mainLayerFields?.Contains(IncrementalLatestField) == false)
                                    {
                                        Console.WriteLine("Error - incremental latest field '{0}' not found in target layer for historical records",
                                            IncrementalTrackField);
                                        return 2;
                                    }
                                }
                            }
                        }
                    }




                    #region BigQuery request

                    BigQueryResults results = null;
                    ulong totalRows = 0;

                    if (!OutputQuerySql && ArcGISDeleteAll && !ArcGISAppendAll)
                    {
                        Console.WriteLine("Delete-only mode - skipping query of BigQuery");
                    }
                    else
                    {
                        if (!(string.IsNullOrEmpty(LatitudeField) || string.IsNullOrEmpty(LongitudeField)))
                        {
                            if (!(attributeFieldSet.Contains(LongitudeField) && attributeFieldSet.Contains(LatitudeField)))
                            {
                                queryFieldSet = new HashSet<string>(attributeFieldSet);
                                queryFieldSet.Add(LongitudeField);
                                queryFieldSet.Add(LatitudeField);
                            }
                        }

                        string sqlQuery = null;
                        if (string.IsNullOrEmpty(QuerySqlFile))
                        {
                            string fieldsToQuery = string.Join(", ", queryFieldSet);

                            sqlQuery = $"SELECT {fieldsToQuery} FROM `{QueryDataset}`";

                            if (!string.IsNullOrWhiteSpace(QueryWhereClause))
                            {
                                sqlQuery += $" WHERE {QueryWhereClause}";
                            }

                            if (string.IsNullOrWhiteSpace(QueryOrderBy))
                            {
                                if (string.IsNullOrEmpty(IncrementalTrackField))
                                {
                                    if (!string.IsNullOrEmpty(IncrementalLatestField))
                                    {
                                        Console.WriteLine("Defaulting to incremental 'latest' field for ordering");
                                        QueryOrderBy = IncrementalLatestField;
                                    }
                                    else
                                    {
                                        Console.WriteLine("Warning - No order by clause was specified");
                                    }
                                }
                            }

                            if (!string.IsNullOrWhiteSpace(QueryOrderBy))
                            {
                                sqlQuery += $" ORDER BY {QueryOrderBy}";
                            }

                            if (QueryLimit > 0)
                            {
                                sqlQuery += $" LIMIT {QueryLimit}";
                            }
                        }
                        else
                        {
                            if (!string.IsNullOrEmpty(QueryDataset))
                            {
                                Console.WriteLine("Warning - query dataset / table name is ignored when SQL file is specified.");
                            }

                            if (!string.IsNullOrEmpty(QueryWhereClause))
                            {
                                Console.WriteLine("Warning - query where clause is ignored when SQL file is specified");
                            }

                            try
                            {
                                sqlQuery = File.ReadAllText(QuerySqlFile);
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine("Error - could not read query SQL file - {0}", e.Message);
                                Console.WriteLine("Query SQL filename: {0}", QuerySqlFile);
                                return 2;
                            }
                        }

                        if (OutputQuerySql)
                        {
                            Console.WriteLine(sqlQuery);
                            return 0;
                        }

                        Console.WriteLine("Connecting to BigQuery...");

                        var credentials = GoogleCredential.FromFile(AccountJsonFile);
                        var client = await BigQueryClient.CreateAsync(ProjectId, credentials).ConfigureAwait(false);

                        Console.WriteLine("Connected");

                        try
                        {
                            Console.WriteLine("Performing SQL query:");
                            Console.WriteLine(sqlQuery);

                            results = await client.ExecuteQueryAsync(sqlQuery, null, null, null, ct).ConfigureAwait(false);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("Error - could not complete query - {0}", e.Message);
                            return 2;
                        }

                        totalRows = (results?.SafeTotalRows).GetValueOrDefault();
                        Console.WriteLine("Total rows retrieved: {0}", totalRows);
                    }

                    // collect the correct matching field names

                    var resultFields = SubsetFieldNames(results?.Schema.Fields, queryFieldSet);

                    if (resultFields?.IsProperSupersetOf(queryFieldSet) ?? false)
                    {
                        Console.WriteLine("Error - some of the listed fields were missing in the result data - stopping without producing any output");

                        Console.WriteLine("* Missing / misnamed fields from attribute list: " +
                            string.Join(", ", queryFieldSet.Except(resultFields, StringComparer.OrdinalIgnoreCase)));

                        Console.WriteLine("* Fields present in the results and not in the attribute list: " +
                            string.Join(", ", results.Schema.Fields.Select(f => f.Name)
                                .Except(resultFields, StringComparer.OrdinalIgnoreCase)));

                        return 2;
                    }
                    #endregion

                    if (portalGateway != null || mqttClient != null)
                    {
                        if (portalGateway != null && ArcGISDeleteAll && !DryRun)
                        {
                            var errorStatus = await DeleteAllFromTargetLayer(portalGateway).ConfigureAwait(false);
                            if (errorStatus != 0)
                            {
                                return errorStatus;
                            }
                        }

                        // find out which rows were already retrieved so they can be skipped.
                        // only look back as far as the last record present for each track / port
                        // - not applicable in append all mode, or if delete all was performed

                        // need to allow for additional tracks / ports / etc to be
                        // brought into the dataset, such that the dates are staggered

                        if (totalRows > 0)
                        {
                            Dictionary<object, object> latestByTrack = null;

                            if ((layerDesc != null || recentDesc != null) && !(deleteOnly || ArcGISAppendAll))
                            {
                                if (recentLayerFields != null && IncrementalBaseOnRecent)
                                {
                                    latestByTrack = await RetrieveLatestByTrack(portalGateway,
                                        ArcGISRecentPath, recentLayerFields, true).ConfigureAwait(false);
                                }
                                else if (mainLayerFields != null)
                                {
                                    latestByTrack = await RetrieveLatestByTrack(portalGateway,
                                        ArcGISLayerPath, mainLayerFields, false).ConfigureAwait(false);
                                }

                                if (latestByTrack == null)
                                {
                                    Console.WriteLine("Error - a suitable basis for incremental updates could not be determined. Aborting without any changes.");
                                    return 5;
                                }
                            }

                            ulong skippedExistingRecords = 0;

                            if (!deleteOnly)
                            {
                                const int batchSize = 1000;

                                if (layerDesc != null)
                                {
                                    Console.WriteLine("Appending {0} records in batches of up to {1}",
                                        ArcGISAppendAll ? "filtered incremental" : "all", batchSize);

                                    if (recentDesc != null)
                                    {
                                        Console.WriteLine("Records will be added or replaced in the recent layer only when all processing on the main layer is complete is complete");
                                    }
                                }

                                if (mqttClient != null && MqttRetainFanout)
                                {
                                    Console.WriteLine("Individual messages will be sent to broker topic {0}/track-value", BrokerTopic);
                                }

                                int totalAppended = 0;
                                int totalAttempted = 0;

                                int batchCount = 0;

                                var recentFeatures = recentLayerFields != null ?
                                    new Dictionary<object, Feature<Point>>() : null;

                                var features = mainLayerFields != null || (mqttClient != null && !MqttRetainFanout) ? new List<Feature<Point>>() : null;
                                // TODO - keep a separate buffer of MQTT features; always use source field names with exact case as specified

                                if (resultFields.TryGetValue(IncrementalLatestField, out var resultLatest) && resultFields.TryGetValue(IncrementalTrackField, out var resultTrack))
                                {
                                    foreach (var row in results)
                                    {
                                        var currentTrack = row[resultTrack];
                                        var currentValue = row[resultLatest];

                                        if (latestByTrack == null
                                            || !latestByTrack.TryGetValue(currentTrack, out var latestValue)
                                            || CompareIncrementalValues(currentValue, latestValue) > 0)
                                        {
                                            Feature<Point> recent = null;
                                            if (recentFeatures?.TryGetValue(currentTrack, out recent) == false)
                                            {
                                                recent = new Feature<Point>();
                                                recentFeatures[currentTrack] = recent;
                                            }

                                            var feat = new Feature<Point>();

                                            foreach (var fieldName in attributeFieldSet)
                                            {
                                                // should be true for all fields
                                                if (resultFields.TryGetValue(fieldName, out var resultField))
                                                {
                                                    if (mainLayerFields != null && mainLayerFields.TryGetValue(fieldName, out var mainOutputCase))
                                                    {
                                                        feat.Attributes[mainOutputCase] = row[resultField];
                                                    }
                                                    else
                                                    {
                                                        feat.Attributes[fieldName] = row[resultField]; // for MQTT only
                                                    }

                                                    if (recent != null && recentLayerFields.TryGetValue(fieldName, out var recentOutputCase))
                                                    {
                                                        recent.Attributes[recentOutputCase] = row[resultField];
                                                    }
                                                }
                                            }

                                            if (!string.IsNullOrEmpty(LongitudeField) && !string.IsNullOrEmpty(LatitudeField)
                                                && (mqttClient != null 
                                                    || layerDesc?.GeometryTypeString == "esriGeometryPoint"
                                                    || recentDesc?.GeometryTypeString == "esriGeometryPoint") 
                                                && resultFields.TryGetValue(LongitudeField, out var resultLon) 
                                                && resultFields.TryGetValue(LatitudeField, out var resultLat))
                                            {
                                                var pointGeom = new Point()
                                                {
                                                    X = Convert.ToDouble(row[resultLon], CultureInfo.InvariantCulture),
                                                    Y = Convert.ToDouble(row[resultLat], CultureInfo.InvariantCulture),

                                                    SpatialReference = SpatialReference.WGS84
                                                };

                                                if (feat != null && (mqttClient != null || layerDesc?.GeometryTypeString == "esriGeometryPoint"))
                                                {
                                                    feat.Geometry = pointGeom;
                                                }

                                                if (recent != null && recentDesc.GeometryTypeString == "esriGeometryPoint")
                                                {
                                                    recent.Geometry = pointGeom;
                                                }
                                            }

                                            // retain and fanout are coupled, because retain would not
                                            // make sense for most purposes unless fanned out
                                            if (feat != null && mqttClient != null && MqttRetainFanout && !DryRun)
                                            {
                                                // send results through to MQTT - warn but don't stop if it fails
                                                try
                                                {
                                                    var result = await mqttClient.PublishAsync(
                                                        new MqttApplicationMessageBuilder()
                                                            .WithTopic($"{BrokerTopic}/{currentTrack}")
                                                            .WithAtMostOnceQoS()
                                                            .WithRetainFlag(true)
                                                            .WithPayload(SerializeEntity(feat, MqttGenericJson))
                                                                .Build()).ConfigureAwait(false);

                                                    if (result.ReasonCode != MqttClientPublishReasonCode.Success)
                                                    {
                                                        Console.WriteLine("Warning - failed to publish message to MQTT");
                                                    }
                                                }
                                                catch (Exception e)
                                                {
                                                    Console.WriteLine("Warning - failed to publish MQTT message");
                                                    Console.WriteLine("Internal message - " + e.Message);
                                                    // continue
                                                }
                                            }

                                            //Console.WriteLine("Feature {0} : {1} would be WRITTEN (latest: {2})", currentTrack, currentValue, latestValue);
                                            if (features != null)
                                            {
                                                features.Add(feat);

                                                if (features.Count >= batchSize)
                                                {
                                                    if (!DryRun)
                                                    {
                                                        if (mqttClient != null && !MqttRetainFanout)
                                                        {
                                                            // fire and forget
                                                            try
                                                            {
                                                                string payload = string.Join("\n",
                                                                    features.Select(f => SerializeEntity(f, MqttGenericJson)));

                                                                var result = await mqttClient.PublishAsync(
                                                                    new MqttApplicationMessageBuilder()
                                                                        .WithTopic(BrokerTopic)
                                                                        .WithAtMostOnceQoS()
                                                                        .WithRetainFlag(false)
                                                                        .WithPayload(payload)
                                                                            .Build()).ConfigureAwait(false);

                                                                if (result.ReasonCode != MqttClientPublishReasonCode.Success)
                                                                {
                                                                    Console.WriteLine("Warning - failed to publish batch message to MQTT");
                                                                }
                                                            }
                                                            catch (Exception e)
                                                            {
                                                                Console.WriteLine("Warning - failed to publish MQTT message");
                                                                Console.WriteLine("Internal message - " + e.Message);
                                                                // continue
                                                            }
                                                        }

                                                        if (portalGateway != null && layerDesc != null)
                                                        {
                                                            // clear geometry as it was only included on the features for the MQTT output
                                                            // TODO the extra memory it would take to keep a duplicate copy would be a
                                                            // worthwhile trade to save this book keeping.
                                                            if (mqttClient != null && layerDesc.GeometryTypeString != "esriGeometryPoint")
                                                            {
                                                                foreach (var f in features)
                                                                {
                                                                    f.Geometry = null;
                                                                }
                                                            }

                                                            try
                                                            {
                                                                var batchAppendResult = await portalGateway.ApplyEdits(
                                                                    new ApplyEdits<Point>(ArcGISLayerPath.AsEndpoint())
                                                                    {
                                                                        Adds = features,
                                                                        RollbackOnFailure = true
                                                                        
                                                                    }).ConfigureAwait(false);

                                                                totalAppended += batchAppendResult.ActualAddsThatSucceeded;
                                                                totalAttempted += batchAppendResult.ActualAdds;
                                                            }
                                                            catch (JsonReaderException e) when (IsInvalidJsonResponse(e))
                                                            {
                                                                Console.WriteLine("Invalid JSON response - {0}", e.Message);
                                                                totalAttempted += features.Count;
                                                            }
                                                        }
                                                    }

                                                    Console.WriteLine("Batch {0} completed...{1}", ++batchCount,
                                                        DryRun ? " (dry run)" : null);

                                                    features.Clear();
                                                }
                                            }
                                        }
                                        else
                                        {
                                            skippedExistingRecords++;
                                            //    Console.WriteLine("  Feature {0} : {1} would be skipped (latest: {2})", currentTrack, currentValue, latestValue);
                                        }

                                        // TODO could also look at pre-filtering via the where
                                        // clause, using the results from the statistics query,
                                        // to reduce the number of throw-away records that are
                                        // retrieved but not used. However, for the initial
                                        // use case, for the Geotab ignition data, it is assumed
                                        // that the have a flat-rate plan, or are not concerned
                                        // about the costs incurred. It is not known whether a
                                        // tighter where clause would actually reduce costs (as
                                        // it still needs to scan the data in order to filter it
                                        // out) but there is a query analyzer that can estimate
                                        // that. There would also be the challenge of merging a
                                        // system-generated where clause in a user-defined query
                                    }
                                }

                                if (features?.Count > 0)
                                {
                                    if (!DryRun)
                                    {
                                        if (mqttClient != null && !MqttRetainFanout)
                                        {
                                            // fire and forget
                                            string payload = string.Join("\n",
                                                features.Select(f => SerializeEntity(f, MqttGenericJson)));

                                            try
                                            {
                                                var result = await mqttClient.PublishAsync(
                                                    new MqttApplicationMessageBuilder()
                                                        .WithTopic(BrokerTopic)
                                                        .WithAtMostOnceQoS()
                                                        .WithRetainFlag(false)
                                                        .WithPayload(payload)
                                                            .Build()).ConfigureAwait(false);

                                                if (result.ReasonCode != MqttClientPublishReasonCode.Success)
                                                {
                                                    Console.WriteLine("Warning - failed to publish final batch message to MQTT");
                                                }
                                            }
                                            catch (Exception e)
                                            {
                                                Console.WriteLine("Warning - failed to publish final batch MQTT message");
                                                Console.WriteLine("Internal message - " + e.Message);
                                                // continue
                                            }
                                        }

                                        if (portalGateway != null && layerDesc != null)
                                        {
                                            // clear geometry as it was only included on the features for the MQTT output
                                            if (mqttClient != null && layerDesc.GeometryTypeString != "esriGeometryPoint")
                                            {
                                                foreach (var f in features)
                                                {
                                                    f.Geometry = null;
                                                }
                                            }

                                            try
                                            {
                                                var batchAppendResult = await portalGateway.ApplyEdits(
                                                    new ApplyEdits<Point>(ArcGISLayerPath)
                                                    {
                                                        Adds = features,
                                                        RollbackOnFailure = false
                                                    }).ConfigureAwait(false);

                                                totalAppended += batchAppendResult.ActualAddsThatSucceeded;
                                                totalAttempted += batchAppendResult.ActualAdds;
                                            }
                                            catch (JsonReaderException e) when (IsInvalidJsonResponse(e))
                                            {
                                                Console.WriteLine("Invalid JSON response - {0}", e.Message);
                                                totalAttempted += features.Count;
                                            }
                                        }

                                    }

                                    Console.WriteLine("Final batch ({0}) completed...{1}", ++batchCount,
                                        DryRun ? " (dry run)" : null);

                                    features.Clear();
                                }

                                if (skippedExistingRecords > 0)
                                {
                                    Console.WriteLine("Skipped {0} existing records with {1} remaining, based on incremental query results.",
                                        skippedExistingRecords, totalRows - skippedExistingRecords);
                                }

                                if (portalGateway != null && recentFeatures?.Count > 0)
                                {
                                    Console.WriteLine("Applying edits to 'recent' features layer{0}",
                                        DryRun ? " (dry run)" : null);

                                    // need to get the object IDs for the existing records with same track field
                                    // we will delete those object IDs and insert replacements in the same edit

                                    Console.WriteLine("Checking for existing records to update in 'recent' table...");

                                    if (recentLayerFields != null &&
                                        recentLayerFields.TryGetValue(IncrementalTrackField, out var recentTrack) &&
                                        recentLayerFields.TryGetValue(IncrementalLatestField, out var recentLatest))
                                    {
                                        // Extent is used as a dummy indicator for no geometry - IGeometry did not work
                                        var currentRecordsResult = await portalGateway.Query<Extent>(
                                            new Query(ArcGISRecentPath)
                                            {
                                                OutFields = new List<string> {
                                                recentDesc.ObjectIdField,
                                                recentTrack,
                                                recentLatest
                                                }
                                            }).ConfigureAwait(false);

                                        var recentUpdates = new List<Feature<Point>>();

                                        if (currentRecordsResult.Features?.Count() > 0)
                                        {
                                            foreach (var prevFeat in currentRecordsResult.Features)
                                            {
                                                var prevTrack = prevFeat.Attributes[recentTrack];
                                                if (recentFeatures.TryGetValue(prevTrack, out var updateFeat))
                                                {
                                                    updateFeat.Attributes[recentDesc.ObjectIdField] = prevFeat.Attributes[recentDesc.ObjectIdField];
                                                    recentUpdates.Add(updateFeat);
                                                    recentFeatures.Remove(prevTrack); // moved to updates
                                                }
                                            }
                                        }

                                        if (!DryRun)
                                        {
                                            // If there are more tracks than the batch size, chop it up.
                                            var recentAdds = recentFeatures.Values.ToList();

                                            var recentAddsTotal = recentAdds.Count;
                                            var recentUpdatesTotal = recentUpdates.Count;
                                            int recentAddsResult = 0;
                                            int recentUpdatesResult = 0;

                                            int recentBatch = 0;

                                            while (recentAdds.Count + recentUpdates.Count > 0)
                                            {
                                                var adds = recentAdds.Count > batchSize ?
                                                recentAdds.Take(batchSize).ToList() : recentAdds;

                                                var updates = recentUpdates.Count > batchSize ?
                                                    recentUpdates.Take(batchSize).ToList() : recentUpdates;

                                                try
                                                {
                                                    var recentEditsResult = await portalGateway.ApplyEdits(
                                                        new ApplyEdits<Point>(ArcGISRecentPath)
                                                        {
                                                            Adds = adds,
                                                            Updates = updates,
                                                            RollbackOnFailure = false
                                                        }).ConfigureAwait(false);

                                                    recentAddsResult += recentEditsResult.ActualAddsThatSucceeded;
                                                    recentUpdatesResult += recentEditsResult.ActualUpdatesThatSucceeded;

                                                    Console.WriteLine("Recent features batch {0} ({1} adds, {2} updates) completed...",
                                                        ++recentBatch, adds.Count, updates.Count);
                                                }
                                                catch (JsonReaderException e) when (IsInvalidJsonResponse(e))
                                                {
                                                    Console.WriteLine("Invalid JSON response for recent features batch {0} - {1}", ++recentBatch, e.Message);
                                                }

                                                recentAdds.RemoveRange(0, adds.Count);
                                                recentUpdates.RemoveRange(0, updates.Count);
                                            }

                                            if (recentAddsResult < recentAddsTotal ||
                                                recentUpdatesResult < recentUpdatesTotal)
                                            {
                                                Console.WriteLine("Warning - some additions or updates to the 'recent' layer failed");
                                                Console.WriteLine(" * edits succesful for {0} of {1} adds, {2} of {3} updates",
                                                    recentAddsResult, recentAddsTotal,
                                                    recentUpdatesResult, recentUpdatesTotal);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    Console.WriteLine("All output completed");
                    return 0;
                }
                finally
                {
                    mqttClient?.Dispose();
                    portalGateway?.Dispose();
                    tokenProvider?.Dispose();
                }

            }
            else
            {
                Console.WriteLine("Please specify all required options.");
            }

            Console.WriteLine("Press any key to continue...");
            Console.ReadKey();
            return 1;
        }

        private static bool IsInvalidJsonResponse(JsonReaderException e)
        {
            return e.Message.StartsWith("Unexpected character encountered while parsing ",
                StringComparison.OrdinalIgnoreCase);
        }

        private async Task<Dictionary<object,object>> RetrieveLatestByTrack(PortalGateway portalGateway,
            string layerPath, HashSet<string> layerFields, bool assumeLayerHasMostRecentOnly)
        {
            if (layerFields.TryGetValue(IncrementalLatestField, out var latestField)
                && layerFields.TryGetValue(IncrementalTrackField, out var trackField))
            {
                if (assumeLayerHasMostRecentOnly)
                {
                    var queryResult = await portalGateway.Query<Extent>(
                        new Query(layerPath)
                        {
                            OutFields = new List<string> { trackField, latestField }
                        }).ConfigureAwait(false);

                    return queryResult.Features?.ToDictionary(
                        feature => feature.Attributes[trackField],
                        feature => feature.Attributes[latestField]);
                }
                else
                {
                    //Extent can be used as a dummy indicator for non-geometric queries
                    var queryResult = await portalGateway.Query<Extent>(new Query(layerPath)
                    {
                        GroupByFields = new List<string>(1) { trackField },
                        OutputStatistics = new List<OutputStatistic>(1)
                        {
                            new OutputStatistic()
                            {
                                OnField = latestField,
                                OutField = "RangeMax",
                                StatisticType = "max"
                            }
                        }
                    }).ConfigureAwait(false);

                    return queryResult.Features?.ToDictionary(
                        feature => feature.Attributes[trackField],
                        feature => feature.Attributes["RangeMax"]);
                }
            }
            else
            {
                return null;
            }
        }

        private async Task<int> DeleteAllFromTargetLayer(PortalGateway portalGateway)
        {
            Console.WriteLine("Deleting existing content from target layer");

            // todo consider first checking count - beyond a certain point
            // it is not worth trying the where clause approach - it would
            // not work, or could impose an excessive burden on the server

            string errorDesc = null;

            try
            {
                var deleteAllResult = await portalGateway.DeleteFeatures(
                    new DeleteFeatures(ArcGISLayerPath) { Where = "1=1" }).ConfigureAwait(false);

                errorDesc = deleteAllResult.Error?.Description;
            }
            catch (Exception e)
            {
                errorDesc = $"Exception - {e.Message}";
            }

            bool hadSuccess = errorDesc == null;

            if (!hadSuccess)
            {
                int batchCount = 0;
                int successCount = 0;
                const int batchSize = 2000;

                // delete by attributes did not work on a 70,000 record table in ArcGIS Online,
                // due to a server-side timeout - not the Anywhere.ArcGIS HttpRequestTimeout
                // let's see if we can do better with an OID query and batch deletion

                Console.WriteLine(errorDesc);
                Console.WriteLine("Deletion of old data failed using catch-all where clause - trying in batches by OID instead");

                var queryAllOIDsResult = await portalGateway.QueryForIds(
                    new QueryForIds(ArcGISLayerPath) { Where = "1=1" }).ConfigureAwait(false);

                if (queryAllOIDsResult?.ObjectIds?.Length > 0)
                {
                    for (int offset = 0; offset < queryAllOIDsResult.ObjectIds.Length; offset += batchSize)
                    {
                        batchCount++;

                        Console.WriteLine("Deleting batch {0} by OID...", batchCount);

                        var deleteBatchResult = await portalGateway.DeleteFeatures(
                            new DeleteFeatures(ArcGISLayerPath)
                            {
                                ObjectIds = queryAllOIDsResult.ObjectIds.Skip(offset).Take(batchSize).ToList(),
                                RollbackOnFailure = false
                            }).ConfigureAwait(false);

                        //This test gave a false negative; the success flag is not set on a successful delete operation
                        if (deleteBatchResult.Error?.Description == null)
                        {
                            successCount++;
                        }
                        else
                        {
                            Console.WriteLine("Warning - deletion by OID did not succeed for batch {0}", batchCount);
                        }
                    }
                }

                if (successCount < batchCount)
                {
                    Console.WriteLine("Exiting due to failed deletions in fallback method.{0}",
                        successCount > 0 ? " Some batches were successful." : null);
                    return 3;
                }

                hadSuccess = successCount > 0;
            }

            var queryCountResult = await portalGateway.QueryForCount(
                new QueryForCount(ArcGISLayerPath) { Where = "1=1" }).ConfigureAwait(false);

            if (queryCountResult.NumberOfResults > 0)
            {
                Console.WriteLine("Error - records were still present after attempting to delete all. {0}Please try again.",
                    hadSuccess ? "Some deletions were successful. " : null);
                return 4;
            }

            return 0;
        }

        static int CompareIncrementalValues(object v1, object v2)
        {
            if (v1 is DateTime dt && v2 is long epochMillis2)
            {
                // statistics on a datetime value are returned by AGS as unix millis since epoch
                var epochMillis1 = new DateTimeOffset(dt, TimeSpan.Zero).ToUnixTimeMilliseconds();
                return epochMillis1.CompareTo(epochMillis2);
            }
            else if (v1 is IComparable cmp && v1.GetType() == v2.GetType())
            {
                return cmp.CompareTo(v2); // works for date, string, etc.
            }
            else
            {
                return Convert.ToDouble(v1, CultureInfo.InvariantCulture).CompareTo(Convert.ToDouble(v2, CultureInfo.InvariantCulture)); // works for most mismatched numeric types
            }
        }

        static HashSet<string> SubsetFieldNames(IEnumerable<Field> fields, HashSet<string> includeNames, params string[] extraNames)
            => fields != null ? new HashSet<string>((includeNames != null 
                ? fields.Where(f =>
                    includeNames.Contains(f.Name) || extraNames.Contains(f.Name, StringComparer.OrdinalIgnoreCase)
                ) : fields).Select(f => f.Name), StringComparer.OrdinalIgnoreCase) : null;

        static HashSet<string> SubsetFieldNames(IEnumerable<TableFieldSchema> fields, HashSet<string> includeNames, params string[] extraNames)
            => fields != null ? new HashSet<string>((includeNames != null
                ? fields.Where(f =>
                    includeNames.Contains(f.Name) || extraNames.Contains(f.Name, StringComparer.OrdinalIgnoreCase)
                ) : fields).Select(f => f.Name), StringComparer.OrdinalIgnoreCase) : null;


        JsonSerializerSettings jsonOmitNull;
        string SerializeEntity(Feature<Point> feat, bool generic = false, bool indent = false)
        {

            if (generic)
            {
                if (feat.Geometry is Point point)
                {
                    var attribs = new Dictionary<string, object>(feat.Attributes);
                    attribs[LongitudeField] = point.X;
                    attribs[LatitudeField] = point.Y;
                    return JsonConvert.SerializeObject(attribs, indent ? Formatting.Indented :
                        Formatting.None);
                }
                else
                {
                    return JsonConvert.SerializeObject(feat.Attributes, indent ? Formatting.Indented :
                        Formatting.None);
                }
            }
            else
            {
                // default serialization attributes supplied by Anywhere.ArcGIS result in a
                // nearly compliant esri JSON format accepted by Server, Portal, and Online,
                // converting DateTime values to an ISO string instead of epoch milliseconds
                // - applying NullValueHandling.Ignore, as Anywhere.ArcGIS does internally,
                //   declutters the spatialReference and omits Geometry if null, since
                //   EmitDefaultValue was not specified in the DataContract attributes

                return JsonConvert.SerializeObject(feat,
                    indent ? Formatting.Indented : Formatting.None,
                        jsonOmitNull ??= new JsonSerializerSettings
                        {
                            NullValueHandling = NullValueHandling.Ignore
                        });
            }
        }

        private async Task<IManagedMqttClient> ConnectMqttAsync()
        {
            var clientOptionsBuilder = new MqttClientOptionsBuilder()
                .WithTcpServer(BrokerHost, BrokerPort)
                .WithCleanSession(MqttCleanSession);

            if (!string.IsNullOrEmpty(BrokerUser))
            {
                if (string.IsNullOrWhiteSpace(BrokerPassword))
                {
                    BrokerPassword = Prompt.GetPassword($"Supply password for MQTT broker user {BrokerUser}: ");
                }

                if (string.IsNullOrEmpty(BrokerPassword))
                {
                    Console.WriteLine("Aborting as no password was provided for MQTT broker");
                    return null;
                }

                clientOptionsBuilder = clientOptionsBuilder.WithCredentials(BrokerUser, BrokerPassword);
            }

            if (MqttClientId != null)
            {
                clientOptionsBuilder = clientOptionsBuilder.WithClientId(MqttClientId);
            }

            if (BrokerScheme == "mqtts")
            {
                clientOptionsBuilder = clientOptionsBuilder.WithTls(
                    new MqttClientOptionsBuilderTlsParameters {
                        UseTls = true,
                        CertificateValidationHandler = context => {
                            // TODO: Check conditions of certificate by using above parameters.
                            return true;
                        }
                    });
            }

            // Setup and start a managed MQTT client.
            var options = new ManagedMqttClientOptionsBuilder()
                .WithAutoReconnectDelay(TimeSpan.FromSeconds(5))
                .WithClientOptions(clientOptionsBuilder).Build();

            var mqttClient = new MqttFactory().CreateManagedMqttClient();

            try
            {
                var connected = new TaskCompletionSource<bool>();
                mqttClient.ConnectedHandler = new MqttClientConnectedHandlerDelegate(e => connected.SetResult(true));

                await mqttClient.StartAsync(options).ConfigureAwait(false);

                await connected.Task.ConfigureAwait(false);
                mqttClient.ConnectedHandler = null;

                return mqttClient;
            }
            catch
            {
                mqttClient.Dispose();
                throw;
            }
        }
        static async Task<int> Main(string[] args)
        {
            var statusCode = await CommandLineApplication.ExecuteAsync<Program>(args).ConfigureAwait(false);
            return statusCode;  // set breakpoint here to see messages for bad args
        }
        //was static Task<int> Main(string[] args) => CommandLineApplication.ExecuteAsync<Program>(args);
    }
}
