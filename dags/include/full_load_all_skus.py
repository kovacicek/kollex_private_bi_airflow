import json
from logging import getLogger
import pandas as pd

from airflow.models import Variable

from include.db import prepare_mysql_akeneo_connection, prepare_pg_connection


def run_full_load():
    logger = getLogger()
    logger.setLevel("INFO")

    chunk_size = 1000

    mysql_engine = prepare_mysql_akeneo_connection()
    pg_engine = prepare_pg_connection()

    pg_raw_schema = Variable.get("PG_RAW_SCHEMA")
    pg_info_schema = Variable.get("PG_INFO_SCHEMA")
    pg_tables_to_use = Variable.get("PG_ALL_SKUS")

    # Reading the product tables from Akeneo
    df_product = pd.read_sql(
        f"""                             
            select distinct
            pcp.identifier
            , gfghproduct.sku as "sku_gfghdata"
            , base_unit_content
            , base_unit_content_uom
            , no_of_base_units
            , gtin
            , kollex_active
            , manufacturer
            , refund_value
            , sales_unit_pkgg
            , name
            , active
            , category_code
            , direct_shop_release


            ,cast(replace(coalesce(  json_extract( pcpm.raw_values , '$.title."<all_channels>"."<all_locales>"' ) ,
                                     json_extract( pcpm2.raw_values , '$.title."<all_channels>"."<all_locales>"' )
                            ,        json_extract( pcp.raw_values , '$.title."<all_channels>"."<all_locales>"' )  ),'"','') as char) as title
        , cast(replace(coalesce( pcpm.code , pcpm2.code ),'"','')as char) as l1_code

        , cast(replace(coalesce( json_extract( pcp.raw_values , '$.brand."<all_channels>"."<all_locales>"' ),
                                  json_extract( pcpm.raw_values , '$.brand."<all_channels>"."<all_locales>"' ) ,
                                  json_extract( pcpm2.raw_values , '$.brand."<all_channels>"."<all_locales>"' ) )      ,'"',''            )as char) as brand



        , cast(replace(coalesce( json_extract( pcp.raw_values , '$.manufacturer_name."<all_channels>"."<all_locales>"' )
                                ,json_extract( pcpm.raw_values , '$.manufacturer_name."<all_channels>"."<all_locales>"' ) ,
                                 json_extract( pcpm2.raw_values , '$.manufacturer_name."<all_channels>"."<all_locales>"' ) ) ,'"',''       )as char) as manufacturer_name

        , cast(replace(coalesce( json_extract( pcpm.raw_values , '$.detail_type_single_unit."<all_channels>"."<all_locales>"' ) ,
                            json_extract( pcpm2.raw_values ,'$.detail_type_single_unit."<all_channels>"."<all_locales>"' )
                    ,       json_extract( pcp.raw_values ,  '$.detail_type_single_unit."<all_channels>"."<all_locales>"' )   ),'"','' )as char)  as detail_type_single_unit

         , cast(replace(coalesce( json_extract( pcpm.raw_values , '$.type_single_unit."<all_channels>"."<all_locales>"' ) ,
                            json_extract( pcpm2.raw_values ,'$.type_single_unit."<all_channels>"."<all_locales>"' )
                    ,       json_extract( pcp.raw_values ,  '$.type_single_unit."<all_channels>"."<all_locales>"' )   ),'"','' )as char)  as type_single_unit


        , cast(replace(coalesce( json_extract( pcpm.raw_values ,  '$.net_content."<all_channels>"."<all_locales>"' ) ,
                            json_extract( pcpm2.raw_values , '$.net_content."<all_channels>"."<all_locales>"' )
                        ,   json_extract( pcp.raw_values ,    '$.net_content."<all_channels>"."<all_locales>"' ) ),'"','' )as char) as net_content
        ,  cast(replace(coalesce(
                    json_extract( pcpm.raw_values ,  '$.golden_record_level1."<all_channels>"."<all_locales>"' ) ,
                    json_extract( pcpm2.raw_values , '$.golden_record_level1."<all_channels>"."<all_locales>"' )
                ,   json_extract( pcp.raw_values ,   '$.golden_record_level1."<all_channels>"."<all_locales>"' )  ),'"','' )as char) as release_l1

        ,  cast(replace(coalesce(  json_extract( pcpm.raw_values , '$.foto_release_hash."<all_channels>"."<all_locales>"' ) ,
                            json_extract( pcpm2.raw_values ,'$.foto_release_hash."<all_channels>"."<all_locales>"' )
                ,             json_extract( pcp.raw_values ,  '$.foto_release_hash."<all_channels>"."<all_locales>"' ) ),'"','' )as char) as foto_release_hash

            , cast(replace(coalesce(  json_extract( pcpm.raw_values , '$.amount_single_unit."<all_channels>"."<all_locales>"' ) ,
                                json_extract( pcpm2.raw_values ,'$.amount_single_unit."<all_channels>"."<all_locales>"' )
                ,                json_extract( pcp.raw_values ,  '$.amount_single_unit."<all_channels>"."<all_locales>"' )),'"','' )as char) as amount_single_unit

        , cast(replace(coalesce(
                            json_extract( pcp.raw_values ,  '$.status_base."<all_channels>"."<all_locales>"' ) ,
                            json_extract( pcpm.raw_values ,  '$.status_base."<all_channels>"."<all_locales>"' ) ,
                            json_extract( pcpm2.raw_values , '$.status_base."<all_channels>"."<all_locales>"' )  ),'"','' )as char) as status_base

        ,  cast(replace(coalesce( json_extract( pcpm.raw_values ,  '$.net_content_uom."<all_channels>"."<all_locales>"' ) ,
                            json_extract( pcpm2.raw_values , '$.net_content_uom."<all_channels>"."<all_locales>"' ),
                            json_extract( pcp.raw_values ,   '$.net_content_uom."<all_channels>"."<all_locales>"' ) ),'"','' )as char) as net_content_uom

        , cast(replace(coalesce(
                            json_extract( pcp.raw_values ,  '$.net_content_liter."<all_channels>"."<all_locales>"' ),
                            json_extract( pcpm.raw_values ,  '$.net_content_liter."<all_channels>"."<all_locales>"' ) ,
                            json_extract( pcpm2.raw_values , '$.net_content_liter."<all_channels>"."<all_locales>"' ) ),'"','' )as char) as net_content_liter

            , cast(replace(coalesce(
                                json_extract( pcp.raw_values ,  '$.contact_info."<all_channels>"."<all_locales>"' ) ,
                                json_extract( pcpm.raw_values ,  '$.contact_info."<all_channels>"."<all_locales>"' ) ,
                                json_extract( pcpm2.raw_values , '$.contact_info."<all_channels>"."<all_locales>"' ) ),'"','' )as char) as contact_info

        , cast(replace(coalesce( json_extract( pcp.raw_values ,  '$.golden_record_level1."<all_channels>"."<all_locales>"' ),
                                json_extract( pcpm.raw_values ,  '$.golden_record_level1."<all_channels>"."<all_locales>"' ) ,
                            json_extract( pcpm2.raw_values , '$.golden_record_level1."<all_channels>"."<all_locales>"' )),'"','' )as char) as golden_record_level1

        , cast(replace(coalesce( json_extract( pcpm.raw_values ,  '$.type_packaging_unit."<all_channels>"."<all_locales>"' ) ,
                            json_extract( pcpm2.raw_values , '$.type_packaging_unit."<all_channels>"."<all_locales>"' ) ,
                            json_extract( pcp.raw_values ,   '$.type_packaging_unit."<all_channels>"."<all_locales>"' )  ),'"','' )as char) as type_packaging_unit

        ,  cast(replace(coalesce( json_extract( pcpm.raw_values , '$.structure_packaging_unit."<all_channels>"."<all_locales>"' ) ,
                            json_extract( pcpm2.raw_values ,'$.structure_packaging_unit."<all_channels>"."<all_locales>"' ) ,
                            json_extract( pcp.raw_values ,  '$.structure_packaging_unit."<all_channels>"."<all_locales>"' )  ),'"','' )as char) as structure_packaging_unit

        ,  cast(replace(coalesce( json_extract( pcpm.raw_values , '$.shop_enabled."<all_channels>"."<all_locales>"' ) ,
                            json_extract( pcpm2.raw_values ,'$.shop_enabled."<all_channels>"."<all_locales>"' ) ,
                            json_extract( pcp.raw_values ,  '$.shop_enabled."<all_channels>"."<all_locales>"' )  ),'"','' )as char) as shop_enabled

        ,  cast(replace(coalesce( json_extract( pcpm.raw_values , '$.gtin_single_unit."<all_channels>"."<all_locales>"' ) ,
                            json_extract( pcpm2.raw_values ,'$.gtin_single_unit."<all_channels>"."<all_locales>"' ) ,
                            json_extract( pcp.raw_values ,  '$.gtin_single_unit."<all_channels>"."<all_locales>"' )  ),'"','' )as char) as gtin_single_unit

                 ,  cast(replace(coalesce( json_extract( pcpm.raw_values , '$.gtin_packaging_unit."<all_channels>"."<all_locales>"' ) ,
                            json_extract( pcpm2.raw_values ,'$.gtin_packaging_unit."<all_channels>"."<all_locales>"' ) ,
                            json_extract( pcp.raw_values ,  '$.gtin_packaging_unit."<all_channels>"."<all_locales>"' )  ),'"','' )as char) as gtin_packaging_unit



        ,  cast(replace(coalesce( json_extract( pcpm.raw_values , '$.detail_type_packaging_unit."<all_channels>"."<all_locales>"' ) ,
                            json_extract( pcpm2.raw_values ,'$.detail_type_packaging_unit."<all_channels>"."<all_locales>"' ) ,
                            json_extract( pcp.raw_values ,  '$.detail_type_packaging_unit."<all_channels>"."<all_locales>"' )  ),'"','' )as char) as detail_type_packaging_unit
        ,pcp.raw_values as raw_values_product
        ,pcp.updated
       ,cast(case
           when pcpm2.code is null
               then pcpm.code
           else pcpm2.code
           end as char) as base_code
    , case when coalesce(pcpm.code,pcpm2.code) like 'm-%' then true else false end  is_manual

       ,pcp.created

        from akeneo.pim_catalog_product                     pcp
                left join akeneo.pim_catalog_product_model pcpm
                            on pcpm.id = pcp.product_model_id
                left join akeneo.pim_catalog_product_model pcpm2
                            on pcpm.parent_id = pcpm2.id


                left join (select max(sku)                   as sku,
                                MAX(base_unit_content)     as base_unit_content,
                                MAX(base_unit_content_uom) as base_unit_content_uom,
                                MAX(no_of_base_units)         no_of_base_units,
                                MAX(gtin)                  as gtin,

                                MAX(kollex_active)         as kollex_active,
                                MAX(manufacturer)          as manufacturer,


                                MAX(refund_value)          as refund_value,


                                MAX(sales_unit_pkgg)       as sales_unit_pkgg,
                                MAX(name)                  as name,
                                MAX(active)                as active,
                                MAX(category_code)            category_code,

                                MAX(direct_shop_release)      direct_shop_release

                            from gfghdata.product
                            where sku is not null
                            group by sku) as gfghproduct on gfghproduct.sku = pcp.identifier
                left join akeneo.pim_catalog_family_translation pcft on pcp.family_id = pcft.foreign_key
            """,
        con=mysql_engine,
        chunksize=chunk_size,
    )

    connection = pg_engine.connect()

    connection.execute(
        f"drop table if exists {pg_raw_schema}.{pg_tables_to_use};"
    )
    count = 0

    # Extracting Active Merchants

    merchants_active = pd.read_sql(
        """
                                    SELECT
                                        merchant_key
                                    FROM
                                        prod_raw_layer.merchants_new
                                    UNION
                                    SELECT
                                        merchant_key
                                    FROM
                                        prod_raw_layer.merchants_active

                                  """,
        con=pg_engine,
    )
    merchants_active = merchants_active[
        merchants_active["merchant_key"] != "trinkkontor"
        ]
    merchants_active = merchants_active[
        merchants_active["merchant_key"] != "trinkkontor_trr"
        ]
    merchants_active = merchants_active[
        merchants_active["merchant_key"] != "merchant_key"
        ]

    for df_chunk in df_product:
        chunk = df_chunk
        print(
            "######################################Processing Chunk Number "
            + str(count)
        )
        chunk["manufacturer_gfgh_data"] = "|" + chunk["manufacturer"] + "|"

        chunk.drop(
            [
                "title_2",
                "net_content_2",
                "brand_2",
                "net_content_liter_2",
                "contact_info_2",
            ],
            axis=1,
            inplace=True,
            errors="ignore",
        )

        # adding Pim categories
        print("extracting SKU Fact Consolidating Those columns")
        sku_category_fact = pd.read_sql_table(
            "sku_category_fact",
            con=pg_engine,
            schema=pg_info_schema,
        )
        chunk = chunk.merge(
            sku_category_fact,
            how="inner",
            left_on="identifier",
            right_on="sku",
            suffixes=("", "_y"),
        )
        chunk.drop(
            chunk.filter(regex="_y$").columns.tolist(),
            axis=1,
            inplace=True,
            errors="ignore",
        )

        # Translating attributes through attribute  values  in Akeneo

        attribute_options = pd.read_sql(
            """select distinct code,value from pim_catalog_attribute_option 
                left join pim_catalog_attribute_option_value   
                    on pim_catalog_attribute_option.id = pim_catalog_attribute_option_value.option_id""",
            con=mysql_engine,
        )

        chunk = chunk.merge(
            attribute_options,
            left_on="net_content_liter",
            right_on="code",
            how="left",
        )
        chunk["net_content_liter"] = (
            chunk["value"].combine_first(chunk["net_content_liter"]).astype(str)
        )
        chunk.drop(
            attribute_options.columns, inplace=True, axis=1, errors="ignore"
        )

        chunk = chunk.merge(
            attribute_options,
            left_on="net_content_uom",
            right_on="code",
            how="left",
        )
        chunk["net_content_uom"] = (
            chunk["value"].combine_first(chunk["net_content_uom"]).astype(str)
        )
        chunk.drop(
            attribute_options.columns, inplace=True, axis=1, errors="ignore"
        )

        chunk = chunk.merge(
            attribute_options,
            left_on="type_single_unit",
            right_on="code",
            how="left",
        )
        chunk["type_single_unit"] = chunk["value"].combine_first(
            chunk["type_single_unit"]
        )
        chunk.drop(
            attribute_options.columns, inplace=True, axis=1, errors="ignore"
        )

        chunk = chunk.merge(
            attribute_options,
            left_on="amount_single_unit",
            right_on="code",
            how="left",
        )
        chunk["amount_single_unit"] = (
            chunk["value"]
            .combine_first(chunk["amount_single_unit"])
            .astype(str)
        )
        chunk.drop(
            attribute_options.columns, inplace=True, axis=1, errors="ignore"
        )

        chunk = chunk.merge(
            attribute_options,
            left_on="structure_packaging_unit",
            right_on="code",
            how="left",
        )
        chunk["structure_packaging_unit"] = (
            chunk["value"]
            .combine_first(chunk["structure_packaging_unit"])
            .astype(str)
        )
        chunk.drop(
            attribute_options.columns, inplace=True, axis=1, errors="ignore"
        )

        chunk = chunk.merge(
            attribute_options,
            left_on="type_packaging_unit",
            right_on="code",
            how="left",
        )
        chunk["type_packaging_unit"] = (
            chunk["value"]
            .combine_first(chunk["type_packaging_unit"])
            .astype(str)
        )
        chunk.drop(
            attribute_options.columns, inplace=True, axis=1, errors="ignore"
        )

        chunk = chunk.merge(
            attribute_options,
            left_on="golden_record_level1",
            right_on="code",
            how="left",
        )
        chunk["golden_record_level1"] = (
            chunk["value"]
            .combine_first(chunk["golden_record_level1"])
            .astype(str)
        )
        chunk.drop(
            attribute_options.columns, inplace=True, axis=1, errors="ignore"
        )

        chunk = chunk.merge(
            attribute_options,
            left_on="status_base",
            right_on="code",
            how="left",
        )
        chunk["status_base"] = (
            chunk["value"].combine_first(chunk["status_base"]).astype(str)
        )
        chunk.drop(
            attribute_options.columns, inplace=True, axis=1, errors="ignore"
        )

        chunk = chunk.merge(
            attribute_options, left_on="brand", right_on="code", how="left"
        )
        chunk["brand"] = (
            chunk["value"].combine_first(chunk["brand"]).astype(str)
        )
        chunk.drop(
            attribute_options.columns, inplace=True, axis=1, errors="ignore"
        )

        chunk = chunk.merge(
            attribute_options,
            left_on="amount_single_unit",
            right_on="code",
            how="left",
        )
        chunk["amount_single_unit"] = (
            chunk["value"]
            .combine_first(chunk["amount_single_unit"])
            .astype(str)
        )
        chunk.drop(
            attribute_options.columns, inplace=True, axis=1, errors="ignore"
        )

        chunk = chunk.merge(
            attribute_options,
            left_on="net_content_uom",
            right_on="code",
            how="left",
        )
        chunk["net_content_uom"] = (
            chunk["value"].combine_first(chunk["net_content_uom"]).astype(str)
        )
        chunk.drop(
            attribute_options.columns, inplace=True, axis=1, errors="ignore"
        )

        # Extracting Merchant Info
        sorted_merchants = merchants_active.sort_values("merchant_key")
        sorted_merchants = sorted_merchants[
            sorted_merchants["merchant_key"] != "merchant_key"
            ]

        for merchant in sorted_merchants["merchant_key"]:
            chunk[str(merchant) + "_enabled"] = (
                chunk["raw_values_product"]
                .apply(
                    lambda x: json.loads(x)[
                        "gfgh_" + str(merchant) + "_enabled"
                        ]["<all_channels>"]["<all_locales>"]
                    if "gfgh_" + str(merchant) + "_enabled" in json.dumps(x)
                    else False
                )
                .astype(str)
            )
            chunk[str(merchant) + "_id"] = (
                chunk["raw_values_product"]
                .apply(
                    lambda x: json.loads(x)["gfgh_" + str(merchant) + "_id"][
                        "<all_channels>"
                    ]["<all_locales>"]
                    if "gfgh_" + str(merchant) + "_id" in json.dumps(x)
                    else None
                )
                .astype(str)
            )
            chunk[str(merchant) + "_freigabe"] = (
                chunk["raw_values_product"]
                .apply(
                    lambda x: json.loads(x)[
                        "freigabe_" + str(merchant) + "_id"
                        ]["<all_channels>"]["<all_locales>"]
                    if "freigabe_" + str(merchant) + "_id" in json.dumps(x)
                    else None
                )
                .astype(str)
            )

        # Counting Enabled SKUs
        enabelment_columns = [col for col in chunk.columns if "_enabled" in col]
        enabled_df = chunk[enabelment_columns]
        enabled_df["enablement"] = (
                enabled_df[enabled_df == "True"].count(axis=1) - 2
        )
        chunk["enablement"] = enabled_df["enablement"]

        # Drop the JSON columns
        print("Droping JSON Columns")
        chunk.drop(
            ["raw_values_model", "raw_values", "raw_values_product"],
            axis=1,
            inplace=True,
            errors="ignore",
        )
        chunk.drop(
            ["family_id", "product_model_id", "family_variant_id"],
            axis=1,
            inplace=True,
            errors="ignore",
        )

        # Writing the results in DWH
        # print("Writing to the DWH")

        chunk.drop("is_enabled", axis=1, inplace=True, errors="ignore")
        chunk.drop("merchant_key_id", axis=1, inplace=True, errors="ignore")
        chunk.drop(
            "merchant_key_enabled", axis=1, inplace=True, errors="ignore"
        )
        chunk.drop_duplicates(subset=["identifier"], inplace=True)

        pg_tables_to_use = pg_tables_to_use

        chunk.to_sql(
            pg_tables_to_use,
            pg_engine,
            schema=pg_raw_schema,
            if_exists="append",
            index=False,
        )

        print("##############Finished Writing to the DWH#############")
        # print(chunk['title'])
        count += 1
    pg_engine.dispose()
    mysql_engine.dispose()
