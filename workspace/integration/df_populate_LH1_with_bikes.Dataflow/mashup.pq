[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Query_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "File", TransformationQueryName = "Query_DataDestinationTransform"]]]}]
shared Query = let
  Source = Csv.Document(Web.Contents("https://raw.githubusercontent.com/vanctate/Bike-Data-Analysis-/refs/heads/master/BikeData.csv"), [Delimiter = ",", Columns = 9, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"user_id", Int64.Type}, {"age", Int64.Type}, {"gender", type text}, {"student", Int64.Type}, {"employed", Int64.Type}, {"cyc_freq", type text}, {"distance", type number}, {"time", Int64.Type}, {"speed", type number}})
in
  #"Changed column type";
shared Query_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "fcfc2303-83f0-4cb4-9718-0b0ead9604bf"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "6f8572d3-b55c-47e0-af35-e75808ee65e7"]}[Data],
  Navigation_3 = Navigation_2{[Id = "Files", ItemKind = "Folder"]}[Data],
  FileNavigation = Navigation_3{[Name = "bikes.csv"]}?[Content]?
in
  FileNavigation;
shared Query_DataDestinationTransform = (binaryStream, columnNameAndTypePairs) => let
  Pattern = Csv.Document(binaryStream, [Columns = List.Transform(columnNameAndTypePairs, each _{0}), CsvStyle = CsvStyle.QuoteAfterDelimiter, IncludeByteOrderMark = true, ExtraValues = ExtraValues.Ignore, Delimiter = ",", Encoding = 65001]),
  PromoteHeaders = Table.PromoteHeaders(Pattern),
  TransformColumnTypes = Table.TransformColumnTypes(PromoteHeaders, columnNameAndTypePairs, [MissingField = MissingField.Ignore])
in
  TransformColumnTypes;
