# Databricks notebook source
# !curl 'https://api.meaningcloud.com/deepcategorization-1.0' \
#   -F 'key=a37a21ff7ee1551d81e564711edea76f' \
#   -F 'model=IAB_2.0-tier4_en' \
#   -F 'txt=Zara is great, lots of stylish and affordable clothes, shoes, and accessories.'

# COMMAND ----------

# d = {
# "category_list":[
# {
# "abs_relevance":"1",
# "code":"596",
# "label":"Technology & Computing",
# "polarity":"NONE",
# "relevance":"100",
# "term_list":[
# {
# "abs_relevance":"1",
# "form":"softwares",
# "offset_list":[
# {
# "endp":"50",
# "inip":"42"
# }
# ]
# }
# ]
# }
# ],
# "status":{
# "code":"0",
# "msg":"OK",
# "credits":"1",
# "remaining_credits":"19998",
# }
# }

# COMMAND ----------

# import pprint

# pprint.pprint(d)