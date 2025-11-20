[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dimension_city_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dimension_city = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4fded33b-57f7-484a-bbb8-ca7c649419ae"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "07d36f62-15d6-459a-bced-769b73ff994e"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dimension_city", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dimension_customer_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dimension_customer = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4fded33b-57f7-484a-bbb8-ca7c649419ae"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "07d36f62-15d6-459a-bced-769b73ff994e"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dimension_customer", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dimension_date_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dimension_date = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4fded33b-57f7-484a-bbb8-ca7c649419ae"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "07d36f62-15d6-459a-bced-769b73ff994e"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dimension_date", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared dimension_city_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "412e9e96-5a6e-48f9-afeb-a02917a58965"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "39744a72-13a7-4928-92c7-16a2a86c96c7"]}[Data],
  TableNavigation = Navigation_2{[Id = "dimension_city", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared dimension_customer_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "412e9e96-5a6e-48f9-afeb-a02917a58965"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "39744a72-13a7-4928-92c7-16a2a86c96c7"]}[Data],
  TableNavigation = Navigation_2{[Id = "dimension_customer", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared dimension_date_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "412e9e96-5a6e-48f9-afeb-a02917a58965"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "39744a72-13a7-4928-92c7-16a2a86c96c7"]}[Data],
  TableNavigation = Navigation_2{[Id = "dimension_date", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
