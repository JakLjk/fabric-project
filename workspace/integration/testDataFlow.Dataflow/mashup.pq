[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "sales factsales_gold_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "File", TransformationQueryName = "sales factsales_gold_DataDestinationTransform"]]]}]
shared #"sales factsales_gold" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "eb214067-ee9c-4976-af9c-5473fc3619d4"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "95b3916e-8035-4db2-828e-bccac81b3271"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "sales.factsales_gold", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared #"sales factsales_gold_DataDestination" = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "412e9e96-5a6e-48f9-afeb-a02917a58965"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "39744a72-13a7-4928-92c7-16a2a86c96c7"]}[Data],
  Navigation_3 = Navigation_2{[Id = "Files", ItemKind = "Folder"]}[Data],
  FileNavigation = Navigation_3{[Name = "factsalesImported.csv"]}?[Content]?
in
  FileNavigation;
shared #"sales factsales_gold_DataDestinationTransform" = (binaryStream, columnNameAndTypePairs) => let
  Pattern = Csv.Document(binaryStream, [Columns = List.Transform(columnNameAndTypePairs, each _{0}), CsvStyle = CsvStyle.QuoteAfterDelimiter, IncludeByteOrderMark = true, ExtraValues = ExtraValues.Ignore, Delimiter = ",", Encoding = 65001]),
  PromoteHeaders = Table.PromoteHeaders(Pattern),
  TransformColumnTypes = Table.TransformColumnTypes(PromoteHeaders, columnNameAndTypePairs, [MissingField = MissingField.Ignore])
in
  TransformColumnTypes;
