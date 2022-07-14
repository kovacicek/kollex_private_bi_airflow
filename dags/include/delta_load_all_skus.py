

def run_delta_load():
    import json
    from os import environ

    from base64 import b64decode
    from datetime import datetime
    from logging import getLogger, INFO, WARN
    from os import environ
    from sys import exit as sys_exit

    import io
    import psycopg2
    import csv


    
    from pandas import read_sql_table
    from sqlalchemy import create_engine, types
    from sqlalchemy.exc import SQLAlchemyError
    from psycopg2.extensions import register_adapter
    from psycopg2.extras import Json
    from psycopg2 import connect
    import pandas as pd
    import warnings
    import os
    from dotenv import load_dotenv
    from airflow.models import Variable

    warnings.filterwarnings("ignore")
    # Logging
    logger = getLogger()
    logger.setLevel('INFO')
    # load_dotenv('enviroment_variables.env')

    pg_host = Variable.get("PG_HOST")
    pg_user =Variable.get("PG_USERNAME_WRITE")
    pg_password =Variable.get("PG_PASSWORD_WRITE")

    
    pg_database =Variable.get("PG_DATABASE")
    pg_schema = Variable.get("PG_RAW_SCHEMA")
    pg_tables_to_use =Variable.get("PG_ALL_SKUS")
    
    pg_connect_string = f"postgresql://{pg_user}:{pg_password}@{pg_host}/{pg_database}/{pg_schema}"
    pg_engine = create_engine(f"{pg_connect_string}", echo=False)
    chunk_size = 2000  # environ.get('CHUNK_SIZE')


    mysql_host = Variable.get("MYSQL_HOST")
    mysql_port = Variable.get("MYSQL_PORT")
    mysql_schema = Variable.get("MYSQL_DATABASE_akeneo")
    mysql_user =Variable.get("MYSQL_USERNAME")
    
    mysql_password =  environ.get('MYSQL_PASSWORD')
    
    # mysql_tables_to_copy = 
    mysql_connect_string = f"mysql+mysqlconnector://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_schema}"
    mysql_engine = create_engine(f"{mysql_connect_string}", echo=False)



    ########################################################################
    ################################ Reading the product tables from Akeneo
    df_product = pd.read_sql("""
                                        select distinct pcp.*
                                                , pcp.raw_values                                     raw_values_product
                                                , pcpm.raw_values                                    raw_values_model
                                                , pcft.label                       as                family
                                                , sku
                                                , base_unit_content
                                                , base_unit_content_uom
                                                , no_of_base_units
                                                , gtin
                                                , kollex_active
                                                , manufacturer
                                                , sales_unit_pkgg
                                                , name
                                                ,pcpm.code as l1_code
                                                , case when active is null then 0 else active end as "active"


                                    from (select * from akeneo.pim_catalog_product where updated >=curdate()) pcp
                                            left join akeneo.pim_catalog_product_model pcpm
                                                    on pcp.product_model_id = pcpm.id
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
                                    left join akeneo.pim_catalog_family_translation pcft on pcp.family_id =  pcft.foreign_key
    """, con=mysql_engine)

    if df_product.size ==0 :
        print('no changed SKUs, finishing processing')
        return
    chunk = pd.DataFrame()
    print("finished reading Akeneo Data and will Start processing now")





    ################################################################
    ######################### dropping table of all skus for cleanup





    # pg_schema = environ.get('PG_SCHEMA_Junk')
    # pg_user = environ.get('PG_USERNAME_WRITE')
   
    # pg_password =  environ.get('PG_PASSWORD_WRITE')
    pg_tables_to_use =Variable.get("PG_ALL_SKUS")

    pg_connect_string = f"postgresql://{pg_user}:{pg_password}@{pg_host}/{pg_database}"
    pg_engine = create_engine(f"{pg_connect_string}", echo=False)

    connection = pg_engine.connect()
    identifiers_old= df_product['identifier'].iloc[:-1].apply(lambda x :"'"+str(x)+"',".strip()).to_list()
    identifiers_old.append(" '"+str(df_product['identifier'].iloc[-1])+"'")
    identifiers_old.insert(0,"(")
    identifiers_old.insert(len(identifiers_old)+1,')')
    identifiers=" "
    for x in identifiers_old:
        identifiers =identifiers+str(x)+r' '
    identifiers= identifiers.strip('\n')
    identifiers=identifiers.replace("\nName: identifier, Length: 710, dtype: object'","")
    identifiers=identifiers.replace("\n","")







    connection.execute(f"delete from  {pg_schema}.{pg_tables_to_use} where identifier  in {identifiers} ;")

    print("Finished cleaning the table in the DWH")
    #df_product = pd.read_sql("select  * from akeneo.pim_catalog_product limit 100", con=mysql_engine)
    count = 0

    ###################################################
    ###################### Extracting Active Merchants  

    pg_connect_string = f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}/{pg_database}"
    pg_engine = create_engine(f"{pg_connect_string}", echo=False)



    merchants_active= pd.read_sql_table('merchants_all', con=pg_engine,schema=Variable.get("PG_RAW_SCHEMA"))
    merchants_active = merchants_active[~merchants_active["merchant_key"].str.contains('test',na=False)]
    merchants_active = merchants_active[merchants_active["merchant_key"]!='trinkkontor']
    merchants_active = merchants_active[merchants_active["merchant_key"]!='trinkkontor_trr']


    # for df_chunk in df_product:
    chunk = df_product
    print("######################################Processing Chunk Number "+str(count))
    count+=1




    #chunk['raw_values_json'] =  chunk['raw_values'].apply(json.dumps)


    ####################################################################################################
    # extracting values from the JSON for each SKU from product and product model JSON fields raw_values


    chunk['manufacturer_name'] = chunk['raw_values_model'].apply(lambda x: '|'+json.loads(x)
                                                                            .get('manufacturer_name')
                                                                            .get('<all_channels>')
                                                                            .get('<all_locales>')+"|" if x is not None and json.loads(x)
                                                                                                        .get('manufacturer_name') is not None 
                                                                            else None)
    chunk['manufacturer_name_2'] = chunk['raw_values_product'].apply(lambda x: '|'+json.loads(x)
                                                                            .get('manufacturer_name')
                                                                            .get('<all_channels>')
                                                                            .get('<all_locales>')+"|" if x is not None and json.loads(x)
                                                                                                        .get('manufacturer_name') is not None 
                                                                            else None)
    chunk['manufacturer_name_fixtest'] = chunk['raw_values_model'].apply(lambda x: '|'+json.loads(x)
                                                                            .get('manufacturer_name')
                                                                            .get('<all_channels>')
                                                                            .get('<all_locales>')+"|" if x is not None and json.loads(x)
                                                                                                        .get('manufacturer_name') is not None 
                                                                            else None)
    chunk['amount_single_unit'] = chunk['raw_values_model'].apply(lambda x: json.loads(x)
                                                                            .get('amount_single_unit')
                                                                            .get('<all_channels>')
                                                                            .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                        .get('amount_single_unit') is not None 
                                                                            else None)
    chunk['type_single_unit'] = chunk['raw_values_model'].apply(lambda x: json.loads(x)
                                                                            .get('type_single_unit')
                                                                            .get('<all_channels>')
                                                                            .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                        .get('type_single_unit') is not None 
                                                                            else None)
    chunk['golden_record_level1'] = chunk['raw_values_model'].apply(lambda x: json.loads(x)
                                                                            .get('golden_record_level1')
                                                                            .get('<all_channels>')
                                                                            .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                        .get('golden_record_level1') is not None 
                                                                            else None)
    chunk['shop_enabled'] = chunk['raw_values_product'].apply(lambda x: json.loads(x)
                                                                            .get('shop_enabled')
                                                                            .get('<all_channels>')
                                                                            .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                        .get('shop_enabled') is not None 
                                                                            else None)
    chunk['base_code'] = chunk['raw_values_model'].apply(lambda x: json.loads(x)
                                                                            .get('shop_enabled')
                                                                            .get('<all_channels>')
                                                                            .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                        .get('code') is not None 
                                                                            else None)
    chunk['gtin_single_unit'] = chunk['raw_values_model'].apply(lambda x: json.loads(x)
                                                                            .get('gtin_single_unit')
                                                                            .get('<all_channels>')
                                                                            .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                        .get('gtin_single_unit') is not None 
                                                                            else None)
    chunk['gtin_packaging_unit'] = chunk['raw_values_product'].apply(lambda x: json.loads(x)
                                                                            .get('gtin_packaging_unit')
                                                                            .get('<all_channels>')
                                                                            .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                        .get('gtin_packaging_unit') is not None 
                                                                            else None)
    chunk['detail_type_single_unit'] = chunk['raw_values_model'].apply(lambda x: json.loads(x)
                                                                            .get('detail_type_single_unit')
                                                                            .get('<all_channels>')
                                                                            .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                        .get('detail_type_single_unit') is not None 
                                                                            else None)
    chunk['detail_type_packaging_unit'] = chunk['raw_values_model'].apply(lambda x: json.loads(x)
                                                                            .get('detail_type_packaging_unit')
                                                                            .get('<all_channels>')
                                                                            .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                        .get('detail_type_packaging_unit') is not None 
                                                                            else None)

    chunk['release_l1'] = chunk['raw_values_model'].apply(lambda x: json.loads(x)
                                                                            .get('release_l1')
                                                                            .get('<all_channels>')
                                                                            .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                        .get('release_l1') is not None 
                                                                            else None)
    chunk['foto_release_hash'] = chunk['raw_values_model'].apply(lambda x: json.loads(x)
                                                                            .get('foto_release_hash')
                                                                            .get('<all_channels>')
                                                                            .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                        .get('foto_release_hash') is not None 
                                                                            else None)


    chunk['shop_enabled'] = chunk['raw_values_product'].apply(lambda x: json.loads(x)
                                                                            .get('shop_enabled')
                                                                            .get('<all_channels>')
                                                                            .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                        .get('shop_enabled') is not None 
                                                                            else None)


    chunk['status_base'] = chunk['raw_values_model'].apply(lambda x: json.loads(x)
                                                                            .get('status_base')
                                                                            .get('<all_channels>')
                                                                            .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                        .get('status_base') is not None 
                                                                            else None)
    chunk['is_manual'] = chunk['raw_values_model'].apply(lambda x: json.loads(x)
                                                                            .get('code')
                                                                            .get('<all_channels>')
                                                                            .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                        .get('code') is not None 
                                                                            else None)



    chunk['is_manual'] = chunk['is_manual'].apply(lambda x: True if x is not None and 'm-' in x else False)



    chunk['contact_info'] = chunk['raw_values_model'].apply(lambda x: json.loads(x)
                                                                            .get('contact_info')
                                                                            .get('<all_channels>')
                                                                            .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                        .get('contact_info') is not None 
                                                                            else None)

    chunk['net_content_uom'] = chunk['raw_values_product'].apply(lambda x: json.loads(x)
                                                                            .get('net_content_uom')
                                                                            .get('<all_channels>')
                                                                            .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                        .get('net_content_uom') is not None 
                                                                            else None)
    chunk['structure_packaging_unit'] = chunk['raw_values_model'].apply(lambda x: json.loads(x)
                                                                            .get('structure_packaging_unit')
                                                                            .get('<all_channels>')
                                                                            .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                        .get('structure_packaging_unit') is not None 
                                                                            else None)

    chunk['title'] = chunk['raw_values_product'].apply(lambda x: json.loads(x)
                                                                            .get('brand_and_title')
                                                                            .get('<all_channels>')
                                                                            .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                        .get('brand_and_title') is not None 
                                                                            else None)

    chunk['brand'] = chunk['raw_values_product'].apply(lambda x: json.loads(x)
                                                                            .get('brand_and_title')
                                                                            .get('<all_channels>')
                                                                            .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                        .get('brand_and_title') is not None 
                                                                            else None)
    chunk['net_content'] = chunk['raw_values_product'].apply(lambda x: json.loads(x)
                                                                            .get('net_content')
                                                                            .get('<all_channels>')
                                                                            .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                        .get('net_content') is not None 
                                                                            else None)


    chunk['net_content_liter'] = chunk['raw_values_product'].apply(lambda x: json.loads(x)
                                                                            .get('net_content_liter')
                                                                            .get('<all_channels>')
                                                                            .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                        .get('net_content_liter') is not None 
                                                                            else None)


    chunk['contact_info_2'] = chunk['raw_values_product'].apply(lambda x: json.loads(x)
                                                                            .get('contact_info')
                                                                            .get('<all_channels>')
                                                                            .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                        .get('contact_info') is not None 
                                                                            else None)


    chunk['net_content_uom_2'] = chunk['raw_values_model'].apply(lambda x: json.loads(x)
                                                                            .get('net_content_uom')
                                                                            .get('<all_channels>')
                                                                            .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                        .get('net_content_uom') is not None 
                                                                            else None)


    chunk['structure_packaging_unit_2'] = chunk['raw_values_product'].apply(lambda x: json.loads(x)
                                                                            .get('structure_packaging_unit')
                                                                            .get('<all_channels>')
                                                                            .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                        .get('structure_packaging_unit') is not None 
                                                                            else None)
    chunk['title_2'] = chunk['raw_values_product'].apply(lambda x: json.loads(x)
                                                                            .get('title')
                                                                            .get('<all_channels>')
                                                                            .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                        .get('title') is not None 
                                                                            else None)


    chunk['brand_2'] = chunk['raw_values_model'].apply(lambda x: json.loads(x)
                                                                            .get('brand')
                                                                            .get('<all_channels>')
                                                                            .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                        .get('brand') is not None 
                                                                            else None)
    chunk['net_content_2'] = chunk['raw_values_model'].apply(lambda x: json.loads(x)
                                                                            .get('net_content')
                                                                            .get('<all_channels>')
                                                                            .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                        .get('net_content') is not None 
                                                                            else None)
    chunk['net_content_liter_2'] = chunk['raw_values_model'].apply(lambda x: json.loads(x)
                                                                            .get('net_content_liter')
                                                                            .get('<all_channels>')
                                                                            .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                        .get('net_content_liter') is not None 
                                                                            else None)                                                                            




    chunk['type_packaging_unit'] = chunk['raw_values_product'].apply(lambda x: json.loads(x)
                                                                            .get('type_packaging_unit')
                                                                            .get('<all_channels>')
                                                                            .get('<all_locales>') if json.loads(x)
                                                                                                        .get('type_packaging_unit') is not None 
                                                                            else None)
    print("Finished Extracting JSON Values from table")
    # print(chunk['type_packaging_unit'])
    # chunk.to_excel('chunk_dump.xlsx')




    ###################################################################################################################
    ############################## Consolidating Values of Title,Brand,Net_content_liter,Contact_info into one column
    #print("Consolidating Those columns")
    chunk['title'] = chunk['title'].combine_first(chunk['title_2'])
    chunk['net_content'] = chunk['net_content'].combine_first(chunk['net_content_2'])
    chunk['brand'] = chunk['brand'].combine_first(chunk['brand_2'])
    chunk['net_content_liter'] = chunk['net_content_liter'].combine_first(chunk['net_content_liter_2'])
    chunk['contact_info'] = chunk['contact_info'].combine_first(chunk['contact_info_2'])

    chunk.drop(['title_2','net_content_2','brand_2','net_content_liter_2','contact_info_2'],axis=1,inplace=True)
    print("finished Consolidating Those columns")


    ###############################################
    ######################### adding Pim categories
    ##print("extracting SKU Fact Consolidating Those columns")

    pg_connect_string = f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}/{pg_database}"
    pg_engine = create_engine(f"{pg_connect_string}", echo=False)
    # chunk_size = 1000  # environ.get('CHUNK_SIZE')


    sku_category_fact = pd.read_sql_table(
    'sku_category_fact', con=pg_engine, schema=Variable.get("PG_INFO_SCHEMA"))
    chunk = chunk.merge(sku_category_fact, how='inner',
                        left_on='identifier', right_on='sku',suffixes=('', '_y'))
    chunk.drop(chunk.filter(regex='_y$').columns.tolist(),axis=1, inplace=True)
    print("finished extracting SKU Fact Consolidating Those columns")



    #####################################################################################
    ######################### Translating attributes through attribute  values  in Akeneo
    ##print("Translating values from Attribute options")

    attribute_options = pd.read_sql(
    """select code,value from pim_catalog_attribute_option 
        left join pim_catalog_attribute_option_value   
            on pim_catalog_attribute_option.id = pim_catalog_attribute_option_value.option_id""", con=mysql_engine)


    chunk = chunk.merge(
    attribute_options, left_on='net_content_liter', right_on='code', how='left')
    chunk['net_content_liter'] = chunk['value'].combine_first(chunk['net_content_liter'])
    chunk.drop(attribute_options.columns, inplace=True, axis=1)


    chunk = chunk.merge(
    attribute_options, left_on='net_content_uom', right_on='code', how='left')
    chunk['net_content_uom'] =  chunk['value'].combine_first(chunk['net_content_uom'])
    chunk.drop(attribute_options.columns, inplace=True, axis=1)


    chunk = chunk.merge(
    attribute_options, left_on='amount_single_unit', right_on='code', how='left',)
    chunk['amount_single_unit'] = chunk['value'].combine_first(chunk['amount_single_unit'])
    chunk.drop(attribute_options.columns, inplace=True, axis=1)


    chunk = chunk.merge(
    attribute_options, left_on='structure_packaging_unit', right_on='code', how='left')
    chunk['structure_packaging_unit'] = chunk['value'].combine_first(chunk['structure_packaging_unit'])
    chunk.drop(attribute_options.columns, inplace=True, axis=1)


    chunk = chunk.merge(
    attribute_options, left_on='type_packaging_unit', right_on='code', how='left')
    chunk['type_packaging_unit'] = chunk['value'].combine_first(chunk['type_packaging_unit'])
    chunk.drop(attribute_options.columns, inplace=True, axis=1)


    chunk = chunk.merge(
    attribute_options, left_on='golden_record_level1', right_on='code', how='left')
    chunk['golden_record_level1'] = chunk['value'].combine_first(chunk['golden_record_level1'])
    chunk.drop(attribute_options.columns, inplace=True, axis=1)


    chunk = chunk.merge(
    attribute_options, left_on='status_base', right_on='code', how='left')
    chunk['status_base'] = chunk['value'].combine_first(chunk['status_base'])
    chunk.drop(attribute_options.columns, inplace=True, axis=1)


    chunk = chunk.merge(
    attribute_options, left_on='brand', right_on='code', how='left')
    chunk['brand'] = chunk['value'].combine_first(chunk['brand'])
    chunk.drop(attribute_options.columns, inplace=True, axis=1)


    chunk = chunk.merge(
    attribute_options, left_on='amount_single_unit', right_on='code', how='left')
    chunk['amount_single_unit'] = chunk['value'].combine_first(chunk['amount_single_unit'])
    chunk.drop(attribute_options.columns, inplace=True, axis=1)


    chunk = chunk.merge(
    attribute_options, left_on='net_content_uom', right_on='code', how='left')
    chunk['net_content_uom'] = chunk['value'].combine_first(chunk['net_content_uom'])
    chunk.drop(attribute_options.columns, inplace=True, axis=1)


    print("Finished Translating values from Attribute options")




    ###################################################
    ###################### Extracting Merchant Info 

    #print("creating merchant Columns")
    for merchant in merchants_active.sort_values('merchant_key'):
    # chunk[str(merchant)] = chunk['raw_values_product'].apply(lambda x :json.loads(x)['gfgh_'+str(merchant)+'_enabled']['<all_channels>']['<all_locales>'] if 'gfgh_'+str(merchant)+'_id' in json.dumps(x) else False)
        chunk[str(merchant)+'_id'] = chunk['raw_values_product'].apply(lambda x :json.loads(x)['gfgh_'+str(merchant)+'_id']['<all_channels>']['<all_locales>'] if 'gfgh_'+str(merchant)+'_id' in json.dumps(x) else None)
        chunk[str(merchant)+'_enabled'] = chunk['raw_values_product'].apply(lambda x :json.loads(x)['freigabe_'+str(merchant)+'_id']['<all_channels>']['<all_locales>'] if 'freigabe_'+str(merchant)+'_id' in json.dumps(x) else None)
    ##print(merchant)
    print("finished creating merchant Columns")
    #################################    
    ##### Counting Enabled SKUs
    number_of_merchants = merchants_active['merchant_key'].size
    enabelment_columns  = [col for col in chunk.columns if '_enabled' in col]
    enabled_df = chunk[enabelment_columns]
    enabled_df['enablement']=enabled_df[enabled_df == 'True'].count(axis=1)-2
    chunk['enablement'] = enabled_df['enablement']
    #print("finished creating merchant Columns")
    #################################
    ######## Droping the JSON columns
    #print("Droping JSON Columns")
    chunk.drop(['raw_values_model','raw_values','raw_values_product'],axis=1,inplace=True)
    chunk.drop([ 'family_id', 'product_model_id', 'family_variant_id'],axis=1,inplace=True)



    # chunk.to_csv('dump_chunk.csv')
    ####################################################
    ######################### Writing the results in DWH 
    print("Writing to the DWH")

    chunk.drop('is_enabled',axis=1,inplace=True)



    pg_tables_to_use =Variable.get("PG_ALL_SKUS")
    pg_connect_string = f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}/{pg_database}"
    pg_engine = create_engine(f"{pg_connect_string}", echo=False)


    chunk.to_sql(pg_tables_to_use,
                
                con=pg_engine,
                chunksize=chunk_size,
                if_exists="append",
                method="multi",
                schema=pg_schema
                ,index=False
                )
    print("Finished Writing to the DWH")

    pg_engine.dispose()
    mysql_engine.dispose()