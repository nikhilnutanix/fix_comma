package util

import (
	"time"

	mapset "github.com/deckarep/golang-set"
	"github.com/nutanix-core/ntnx-api-categories/src/go/proto3/prism/v4/config"
)

const (
	LogConfigFile          string = "config.properties"
	LogDir                 string = "/home/nutanix/data/logs/"
	CategoryConfigFilePath string = "/home/nutanix/config/categories"
	AplosConfigFilePath    string = "/home/nutanix/aplos/config"
	PreSeededFile          string = "preseeded_categories.cfg"
	SlashSeparator         string = "/"
	// IDF entity category field constants
	CategoryIdfEntity          = "category"
	ParentExtIDAttribute       = "parent_ext_id"
	CategoryTypeAttribute      = "category_type"
	ExtIDAttribute             = "ext_id"
	KeyAttribute               = "key"
	NameAttribute              = "name"
	FqNameAttribute            = "fq_name"
	TypeAttribute              = "type"
	DescriptionAttribute       = "description"
	ImmutableAttribute         = "immutable"
	InternalAttribute          = "internal"
	UserSpecifiedNameAttribute = "user_specified_name"
	OwnerUUIDAttribute         = "owner_uuid"
	Created                    = "_created_timestamp_usecs_"
	CARDINALITY                = "cardinality"
	ZPROTO                     = "__zprotobuf__"

	// IDF entity category_association related constants
	CategoryAssociationIdfEntity = "category_association"
	CategoryCountsIdfEntity      = "category_counts"
	CategoryIDAttribute          = "category_id"
	CountAttribute               = "count"

	AbacCategoryKeyAttribute = "abac_category_key"
	UUIDAttribute            = "uuid"
	ValueAttribute           = "value"

	// KINDs
	ImagePlacementPolicyKind    = "image_placement_policy"
	NetworkSecurityPolicyKind   = "network_security_policy"
	NetworkSecurityRuleKind     = "network_security_rule"
	NgtPolicyKind               = "ngt_policy"
	QosPolicyKind               = "qos_policy"
	ProtectionRuleKind          = "protection_rule"
	AccessControlPolicyKind     = "access_control_policy"
	StoragePolicyKind           = "storage_policy"
	ImageRateLimitKind          = "image_rate_limit"
	RecoveryPlanKind            = "recovery_plan"
	VMKind                      = "vm"
	MhVMKind                    = "mh_vm"
	ClusterKind                 = "cluster"
	SubnetKind                  = "subnet"
	HostKind                    = "host"
	ReportKind                  = "report"
	ImageKind                   = "image"
	MarketplaceItemKind         = "marketplace_item"
	BlueprintKind               = "blueprint"
	AppKind                     = "app"
	VolumeGroupKind             = "volumegroup"
	AffinityRuleKind            = "vm_host_affinity_policy"
	VMAntiAffinityPolicyKind    = VMAntiAffinityPolicy
	TemplatePlacementPolicyKind = TemplatePlacementPolicy
	BundleKind                  = "bundle"
	HostNicKind                 = "host_nic"
	PolicySchemaKind            = "policy_schema"
	ActionRuleKind              = "action_rule"
	VirtualNicKind              = "virtual_nic"
	VMTemplateKind              = "vm_template"
	VirtualNetworkKind          = "virtual_network"
	NetworkEntityGroupKind      = "network_entity_group"

	// Other IDF related constants
	AbacCategoryType                = "abac_category"
	AbacCategoryKeyType             = "abac_category_key"
	FilterType                      = "filter"
	AbacEntityCapabilityType        = "abac_entity_capability"
	VolumeGroupEntityCapabilityType = "volume_group_entity_capability"
	VMHostAffinityPolicyType        = "vm_host_affinity_policy"
	VMAntiAffinityPolicyType        = "vm_anti_affinity_policy"
	ContentPlacementPolicyType      = "content_placement_policy"
	FilterExpLhsEntityType          = "filter_expressions.lhs_entity_type"
	FilterExpRhsEntityUuids         = "filter_expressions.rhs_entity_uuids"
	VMCategoryUuids                 = "vm_category_uuids"
	HostCategoryUuids               = "host_category_uuids"
	CategoryUuids                   = "category_uuids"
	ContentFilterCategoryExtIds     = "content_filter.category_ext_ids"
	ClusterFilterCategoryExtIds     = "cluster_filter.category_ext_ids"

	VolumeGroupEntityCapability = "volume_group_entity_capability"
	Filter                      = "filter"
	Kind                        = "kind"
	KindID                      = "kind_id"
	CategoryIDList              = "category_id_list"
	VMHostAffinityPolicy        = "vm_host_affinity_policy"
	VMAntiAffinityPolicy        = "vm_vm_anti_affinity_policy"
	TemplatePlacementPolicy     = "template_placement_policy"
	EntityKind                  = "entity_kind"
	EntityUUID                  = "entity_uuid"

	TimeZone               = "TZ"
	Utc                    = "Utc"
	GlobalEntityTenantUUID = "00000000-0000-0000-0000-000000000000"
	ZkFullsyncOperation    = "FullSync:CreateZkNode:CreateZkSession"
	ZkPreSeededOperation   = "Pre-seeded:CreateZkNode:CreateZkSession"

	LOCALE           = "en_US"
	ArtifactBasePath = "/home/nutanix/api_artifacts"
	Namespace        = "prism"
	NamespacePrefix  = "CTGRS"

	Message = "message"

	CategoryKeyKind       = "category_key"
	CATEGORY              = "category"
	CheckDuplicate        = "CHECK_DUPLICATE"
	CreateAbacCategory    = "CREATE_ABAC_CATEGORY"
	CreateAbacAPI         = "CREATE_CATEGORY_API"
	UpdateCategoryAPI     = "UPDATE_CATEGORY_API"
	GetCategoryAPI        = "GET_CATEGORY_API"
	IsNewKey              = "isNewKey"
	IanaLinkRelationsSelf = "self"
	CategoryObjectType    = "prism.v4.config.Category"
	RelativePath          = "/prism/%s/config/categories"
	Hyphen                = "-"
	EmptyString           = ""
	MockErrorHandler      = "mock_error_handler"
	MockRelativePath      = "../../generated-messages"
	EmptyUser             = ""
	ApplicationName       = "categories-microservice"
	EntityList            = "entity_list"
	Status                = "status"
	ComponentName         = "component_name"
	UserName              = "user_name"
	MaxRetry              = 5
	HeaderXUser           = "X-User"
	HeaderIfMatch         = "if-match"
	Location              = "Location"
	UserUUID              = "userUUID"
	TokenClaims           = "token_claims"
	ExtIDErrorMessage     = "extId"
	Stage                 = "stage"
	IDFErrorCode          = "idfErrorCode"
	AuditOperationType    = "OperationType"
	Operation             = "operation"
	RbacLegacyRoles       = "LEGACY_ROLES"
	CreateCategoryNotif   = "Create Category"
	UpdateCategoryNotif   = "Update Category"
	AuthorizationHeader   = "authorization"

	And                          = "and"
	ODataJoinExpression          = "(%s) %s (%s)"
	DefaultLimit                 = 50
	MaxLimit                     = 100
	KeyNotNull                   = "key ne null"
	KeyAndValue                  = "key,value"
	OrderbyParam                 = "$orderby"
	FilterParam                  = "$filter"
	PageParam                    = "$page"
	LimitParam                   = "$limit"
	SelectParam                  = "$select"
	ExpandParam                  = "$expand"
	Equals                       = "="
	URLParamSeparator            = "&"
	ResourcePath                 = "/categories"
	DetailedAssociationExpandKey = "detailedAssociations"
	AssociationsExpandKey        = "associations"
	InvalidExpandAttrs           = "INVALID_EXPAND_ATTRS"
	Comma                        = ","
	ListCategories               = "List Categories"
	ListAPIQueryBuilding         = "LIST_API_QUERY_BUILDING"
	FetchCategoryListData        = "FETCH_CATEGORIES_LIST_DATA"
	StatsGWFailure               = "STATS_GATEWAY_FAILURE"
	GraphQLUnmarshallingFailure  = "GRAPHQL_UNMARSHALLING_FAILURE"
	QueryParamDelimiter          = "?"
	PaginatedFlag                = "isPaginated"
	CountSubGraphQlQuery         = `filtered_entity_count, total_entity_count}`
	ZkNodeDataSyncInProgress     = "/appliance/logical/categories_data_sync_in_progress"
	DataSyncInProgressStage      = "DATA_SYNC_IN_PROGRESS"
	DefaultStatsGWPort           = 8084
	ParseOdataParams             = "PARSE_ODATA_PARAMS"

	// Error codes
	ODataParsingError               = "50019"
	ExpandDetailedAssocNotSupported = "50033"
	DataSyncInProcess               = "50035"
	DependentComponentFailure       = "50022"
	CategoryAlreadyPresent          = "50023"
	Unhandled                       = "50024"
	InternalCategoryError           = "50021"
	EtagMismatchError               = "50020"
	SystemCategoryError             = "50032"
	EraseOwnerError                 = "50029"
	UpdateKeyNotSupportedError      = "50034"
	UpdateValueNotSupportedError    = "50038"
	UpdateOwnerFailedError          = "50026"
	InvalidOwnerUUIDError           = "50028"
	NoSuchCategoryError             = "50006"
)

var HttpToGrpcCodes = map[int]int{
	200: 0,
	500: 13,
	400: 3,
	504: 4,
	404: 5,
	409: 6,
	403: 7,
	429: 8,
	412: 10,
	501: 12,
	503: 14,
	401: 16,
}

var CategoryAttrs = []string{
	ExtIDAttribute,
	ParentExtIDAttribute,
	NameAttribute,
	DescriptionAttribute,
	UserSpecifiedNameAttribute,
	OwnerUUIDAttribute,
	KeyAttribute,
	CategoryTypeAttribute,
}

var CategoryAssociationAttrs = []string{
	CategoryIDAttribute,
	Kind,
	KindID,
}

var CategoryCountsAttrs = []string{
	CategoryIDAttribute,
	Kind,
	CountAttribute,
}

var AllowedPolicyKinds = mapset.NewSetFromSlice(ToSliceOfInterface([]string{
	ImagePlacementPolicyKind,
	NetworkSecurityPolicyKind,
	NetworkSecurityRuleKind,
	AffinityRuleKind,
	NgtPolicyKind,
	QosPolicyKind,
	ProtectionRuleKind,
	AccessControlPolicyKind,
	StoragePolicyKind,
	ImageRateLimitKind,
	RecoveryPlanKind,
	PolicySchemaKind,
	VMAntiAffinityPolicyKind,
	TemplatePlacementPolicyKind,
	ActionRuleKind,
	NetworkEntityGroupKind,
}))

var AllowedEntityKinds = mapset.NewSetFromSlice(ToSliceOfInterface([]string{
	VMKind,
	MhVMKind,
	VolumeGroupKind,
	ClusterKind,
	SubnetKind,
	HostKind,
	ReportKind,
	ImageKind,
	MarketplaceItemKind,
	BlueprintKind,
	AppKind,
	BundleKind,
	HostNicKind,
	VirtualNicKind,
	VMTemplateKind,
	VirtualNetworkKind,
}))

var KindAlias = map[string]string{
	MhVMKind: VMKind,
}
var AllowedKinds = AllowedPolicyKinds.Union(AllowedEntityKinds)

const (
	BatchQueryItemsLimit               = 1000
	FullSyncRetryDelay                 = 5 * time.Minute
	QueryExecutionBackoffDelay         = 1 * time.Second
	QueryExecutionMaxAttempts          = 15
	WatchClientInitRetryDelay          = 10 * time.Second
	WatchClientInitRetryMaxDelay       = 1 * time.Hour
	WatchClientInitBackoffFactor       = 2
	WatchClientSetWatchInfoDelay       = 5 * time.Minute
	WatchClientSetWatchInfoMaxAttempts = 12
	CategoryBatchSize                  = BatchQueryItemsLimit
)

const (
	CountsIDSeparator = "|"
)

const (
	ZkNodeFullSyncInProgressMarker  = "/appliance/logical/categories_data_sync_in_progress"
	ZkSessionCreateRetryDelay       = 1 * time.Minute
	ZkNodePreSeededInProgressMarker = "/appliance/logical/categories_pre_seeding_in_progress"
	ZkSoftMarkerPath                = "/appliance/logical/category"
)

// =================================== INTERNAL GRPC SERVER RELATED ===================================
// server setup
const (
	IrpcPort              = 8085
	IrpcDelay             = 10 * time.Second
	IrpcBackoff           = 2
	IrpcMaxDelay          = time.Hour
	IrpcBatchLimit        = 1000
	IrpcAttrSvcMeta       = "service_metadata"
	IrpcMetadataSizeLimit = 512
)

var SupportedEntityTypes = mapset.NewSetFromSlice(ToSliceOfInterface([]config.ResourceTypeMessage_ResourceType{
	config.ResourceTypeMessage_VM,
	config.ResourceTypeMessage_MH_VM,
	config.ResourceTypeMessage_IMAGE,
	config.ResourceTypeMessage_SUBNET,
	config.ResourceTypeMessage_CLUSTER,
	config.ResourceTypeMessage_HOST,
	config.ResourceTypeMessage_REPORT,
	config.ResourceTypeMessage_MARKETPLACE_ITEM,
	config.ResourceTypeMessage_BLUEPRINT,
	config.ResourceTypeMessage_APP,
	config.ResourceTypeMessage_VOLUMEGROUP,
	config.ResourceTypeMessage_HOST_NIC,
	config.ResourceTypeMessage_VIRTUAL_NIC,
	config.ResourceTypeMessage_VM_TEMPLATE,
	config.ResourceTypeMessage_VIRTUAL_NETWORK,
}))

var SupportedPolicyTypes = mapset.NewSetFromSlice(ToSliceOfInterface([]config.ResourceTypeMessage_ResourceType{
	config.ResourceTypeMessage_IMAGE_PLACEMENT_POLICY,
	config.ResourceTypeMessage_NETWORK_SECURITY_POLICY,
	config.ResourceTypeMessage_NETWORK_SECURITY_RULE,
	config.ResourceTypeMessage_VM_HOST_AFFINITY_POLICY,
	config.ResourceTypeMessage_VM_VM_ANTI_AFFINITY_POLICY,
	config.ResourceTypeMessage_TEMPLATE_PLACEMENT_POLICY,
	config.ResourceTypeMessage_QOS_POLICY,
	config.ResourceTypeMessage_NGT_POLICY,
	config.ResourceTypeMessage_PROTECTION_RULE,
	config.ResourceTypeMessage_ACCESS_CONTROL_POLICY,
	config.ResourceTypeMessage_STORAGE_POLICY,
	config.ResourceTypeMessage_RECOVERY_PLAN,
	config.ResourceTypeMessage_IMAGE_RATE_LIMIT,
	config.ResourceTypeMessage_POLICY_SCHEMA,
	config.ResourceTypeMessage_ACTION_RULE,
	config.ResourceTypeMessage_NETWORK_ENTITY_GROUP,
}))
