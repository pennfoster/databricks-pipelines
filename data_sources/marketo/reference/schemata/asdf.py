# from pyspark.sql.types import *


# form_fields = StructType(
#     List(
#         StructField("dataType", StringType, True),
#         StructField("id", StringType, True),
#         StructField("isRequired", BooleanType, True),
#         StructField("isSensitive", BooleanType, True),
#         StructField("maxLength", LongType, True),
#         StructField("picklistValues", StringType, True),
#         StructField("visibleRows", LongType, True),
#     )
# )

# forms = StructType(
#     List(
#         StructField("buttonLabel", StringType, True),
#         StructField("buttonLocation", LongType, True),
#         StructField("createdAt", StringType, True),
#         StructField("description", StringType, True),
#         StructField("
#             folder",
#             StructType(
#                 List(
#                     StructField("folderName", StringType, True),
#                     StructField("type", StringType, True),
#                     StructField("value", LongType, True),
#                 )
#             ),
#             True,
#         ),
#         StructField("fontFamily", StringType, True),
#         StructField("fontSize", StringType, True),
#         StructField("id", LongType, True),
#         StructField("
#             knownVisitor",
#             StructType(
#                 List(
#                     StructField("template", StringType, True),
#                     StructField("type", StringType, True),
#                 )
#             ),
#             True,
#         ),
#         StructField("labelPosition", StringType, True),
#         StructField("language", StringType, True),
#         StructField("locale", StringType, True),
#         StructField("name", StringType, True),
#         StructField("progressiveProfiling", BooleanType, True),
#         StructField("status", StringType, True),
#         StructField("
#             thankYouList",
#             ArrayType(
#                 StructType(
#                     List(
#                         StructField("default", BooleanType, True),
#                         StructField("followupType", StringType, True),
#                         StructField("followupValue", DoubleType, True),
#                         StructField("operator", StringType, True),
#                         StructField("subjectField", StringType, True),
#                         StructField("values", ArrayType(StringType, True), True),
#                     )
#                 ),
#                 True,
#             ),
#             True,
#         ),
#         StructField("theme", StringType, True),
#         StructField("updatedAt", StringType, True),
#         StructField("url", StringType, True),
#         StructField("waitingLabel", StringType, True),
#         StructField("workSpaceId", LongType, True),
#     )
# )

# landing_pages = StructType(
#     List(
#         StructField("URL", StringType, True),
#         StructField("computedUrl", StringType, True),
#         StructField("createdAt", StringType, True),
#         StructField("customHeadHTML", StringType, True),
#         StructField("description", StringType, True),
#         StructField("facebookOgTags", StringType, True),
#         StructField("
#             folder",
#             StructType(
#                 List(
#                     StructField("folderName", StringType, True),
#                     StructField("type", StringType, True),
#                     StructField("value", LongType, True),
#                 )
#             ),
#             True,
#         ),
#         StructField("formPrefill", BooleanType, True),
#         StructField("id", LongType, True),
#         StructField("keywords", StringType, True),
#         StructField("mobileEnabled", BooleanType, True),
#         StructField("name", StringType, True),
#         StructField("programId", LongType, True),
#         StructField("robots", StringType, True),
#         StructField("status", StringType, True),
#         StructField("template", LongType, True),
#         StructField("title", StringType, True),
#         StructField("updatedAt", StringType, True),
#         StructField("workspace", StringType, True),
#     )
# )

# programs = StructType(
#     List(
#         StructField("channel", StringType, True),
#         StructField("createdAt", StringType, True),
#         StructField("description", StringType, True),
#         StructField("
#             folder",
#             StructType(
#                 List(
#                     StructField("folderName", StringType, True),
#                     StructField("type", StringType, True),
#                     StructField("value", LongType, True),
#                 )
#             ),
#             True,
#         ),
#         StructField("headStart", BooleanType, True),
#         StructField("id", LongType, True),
#         StructField("name", StringType, True),
#         StructField("sfdcId", StringType, True),
#         StructField("sfdcName", StringType, True),
#         StructField("status", StringType, True),
#         StructField("type", StringType, True),
#         StructField("updatedAt", StringType, True),
#         StructField("url", StringType, True),
#         StructField("workspace", StringType, True),
#     )
# )

# smart_campaigns = StructType(
#     List(
#         StructField("computedUrl", StringType, True),
#         StructField("createdAt", StringType, True),
#         StructField("description", StringType, True),
#         StructField("flowId", LongType, True),
#         StructField("
#             folder",
#             StructType(
#                 List(
#                     StructField("id", LongType, True), StructField(type, StringType, True)
#                 )
#             ),
#             True,
#         ),
#         StructField("id", LongType, True),
#         StructField("isActive", BooleanType, True),
#         StructField("isCommunicationLimitEnabled", BooleanType, True),
#         StructField("isRequestable", BooleanType, True),
#         StructField("isSystem", BooleanType, True),
#         StructField("name", StringType, True),
#         StructField("parentProgramId", LongType, True),
#         StructField("qualificationRuleInterval", LongType, True),
#         StructField("qualificationRuleType", StringType, True),
#         StructField("qualificationRuleUnit", StringType, True),
#         StructField("
#             recurrence",
#             StructType(
#                 List(
#                     StructField("interval", LongType, True),
#                     StructField("intervalType", StringType, True),
#                     StructField("startAt", StringType, True),
#                     StructField("weekdayMask", ArrayType(StringType, True), True),
#                     StructField("weekdayOnly", BooleanType, True),
#                 )
#             ),
#             True,
#         ),
#         StructField("smartListId", LongType, True),
#         StructField("status", StringType, True),
#         StructField("type", StringType, True),
#         StructField("updatedAt", StringType, True),
#         StructField("workspace", StringType, True),
#     )
# )

# smart_lists = StructType(
#     List(
#         StructField("createdAt", StringType, True),
#         StructField("description", StringType, True),
#         StructField("
#             folder",
#             StructType(
#                 List(
#                     StructField("id", LongType, True), StructField(type, StringType, True)
#                 )
#             ),
#             True,
#         ),
#         StructField("id", LongType, True),
#         StructField("name", StringType, True),
#         StructField("updatedAt", StringType, True),
#         StructField("url", StringType, True),
#         StructField("workspace", StringType, True),
#     )
# )

# snippets = StructType(
#     List(
#         StructField("createdAt", StringType, True),
#         StructField("description", StringType, True),
#         StructField("
#             folder",
#             StructType(
#                 List(
#                     StructField("folderName", StringType, True),
#                     StructField("type", StringType, True),
#                     StructField("value", LongType, True),
#                 )
#             ),
#             True,
#         ),
#         StructField("id", LongType, True),
#         StructField("name", StringType, True),
#         StructField("status", StringType, True),
#         StructField("updatedAt", StringType, True),
#         StructField("url", StringType, True),
#         StructField("workspace", StringType, True),
#     )
# )

# static_lists = StructType(
#     List(
#         StructField("computedUrl", StringType, True),
#         StructField("createdAt", StringType, True),
#         StructField("description", StringType, True),
#         StructField("
#             folder",
#             StructType(
#                 List(
#                     StructField("id", LongType, True),
#                     StructField("name", StringType, True),
#                     StructField("type", StringType, True),
#                 )
#             ),
#             True,
#         ),
#         StructField("id", LongType, True),
#         StructField("name", StringType, True),
#         StructField("updatedAt", StringType, True),
#         StructField("workspace", StringType, True),
#     )
# )
