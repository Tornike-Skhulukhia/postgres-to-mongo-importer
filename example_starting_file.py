from importer.copy.basic_copy_importer import do_basic_import

#####################################################
# define connection parameters
#####################################################

pg_params = dict(
    database="",
    host="",
    port=5432,
    user="",
    password="",
)

mongo_params = dict(
    host="localhost",
    port=27017,
    username="",
    password="",
)


#####################################################
# download data
#####################################################
do_basic_import(
    postgres_params=pg_params,
    mongo_params=mongo_params,
    destination_db_name_in_mongo="",
)

# #####################################################
# # optional - change structure of downloaded data.
# #####################################################

# # only use this functionality if you are new to MongoDB
# # or transformations you want to do is not too complex.

# # if data is big, creating local backup of it can also
# # be helpful as changes currently can not be undone.
# # $out aggregation stage or mongodump & mongiomport 
# # can be useful in this case.

# from importer.denormalize.denormalizer import denormalize_mongo
# from pymongo import MongoClient

# local_mongo_client = MongoClient(**mongo_params)


# denormalize_mongo(
#     mongo_client=local_mongo_client,
#     database="news_quotes_data",
#     collection="news_quote",
#     other_collection="news_person",
#     field_name="person_id",
#     other_field_name="_id",
#     new_field_name="person",
#     as_array=False,
#     delete_source_field_name_after_lookup=True,
#     delete_other_collection=False,
# )
