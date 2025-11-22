[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "train csv_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "File", TransformationQueryName = "train csv_DataDestinationTransform"]]]}]
shared #"train csv" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4fded33b-57f7-484a-bbb8-ca7c649419ae"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "07d36f62-15d6-459a-bced-769b73ff994e"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "train.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 3", [Delimiter = ",", Columns = 2, QuoteStyle = QuoteStyle.None])
in
  #"Imported CSV";
shared #"train csv_DataDestination" = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "412e9e96-5a6e-48f9-afeb-a02917a58965"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "39744a72-13a7-4928-92c7-16a2a86c96c7"]}[Data],
  Navigation_3 = Navigation_2{[Id = "Files", ItemKind = "Folder"]}[Data],
  FileNavigation = Navigation_3{[Name = "train.csv"]}?[Content]?
in
  FileNavigation;
shared #"train csv_DataDestinationTransform" = (binaryStream, columnNameAndTypePairs) => let
  Pattern = Csv.Document(binaryStream, [Columns = List.Transform(columnNameAndTypePairs, each _{0}), CsvStyle = CsvStyle.QuoteAfterDelimiter, IncludeByteOrderMark = true, ExtraValues = ExtraValues.Ignore, Delimiter = ",", Encoding = 65001]),
  PromoteHeaders = Table.PromoteHeaders(Pattern),
  TransformColumnTypes = Table.TransformColumnTypes(PromoteHeaders, columnNameAndTypePairs, [MissingField = MissingField.Ignore])
in
  TransformColumnTypes;
