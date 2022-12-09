
def run_gedat():
    import argparse
    import csv
    import fnmatch
    import logging
    import os
    import pandas as pd
    import paramiko
    import shutil
    import datetime
    from sqlalchemy import create_engine
    from datetime import datetime, timedelta
    from dotenv import load_dotenv
    import pandas as pd
    from airflow.models import Variable


    pg_host =   Variable.get("PG_HOST")
    pg_user = Variable.get("PG_USERNAME_WRITE")
    pg_password =  Variable.get("PG_PASSWORD_WRITE")
    pg_database =  Variable.get("PG_DATABASE")
    pg_connect_string = f"postgresql://{pg_user}:{pg_password}@{pg_host}/{pg_database}"
    pg_engine = create_engine(f"{pg_connect_string}", echo=False, pool_pre_ping=True, pool_recycle=800)

    ############ Create list of customers based on agreed format
    ############ with gedat
    connection = pg_engine.connect()
    
    query = f"""
            with merchants as (
                            select '01'                                                as "Satzart"
                                , '00000001'                                          as "Empfänger"
                                , '43999023'                                          as "Absender"
                                , '01'                                                as "Versionsnummer"
                                , concat( 'M_', id_merchant )                         as "Hersteller-Kunden-Nr."
                                , NULL                                                as "GEDAT-Adressnummer"
                                , NULL                                                as "GLN"
                                , NULL                                                as "Leerfeld1"
                                , NULL                                                as "Leerfeld2"
                                , NULL                                                as "Gemeindekennziffer"
                                , 'Getränkefachgroßhandel'                            as "Geschäftstyp"
                                , merchant.name                                       as "Name-1 (Bezeichnung)"
                                , NULL                                                as "Name-2 (Inhaber)"
                                , merchant.name                                       as "Kurzbezeichnung"
                                , concat( address.street, ' ', address.house_number ) as "Straße u. Hausnummer"
                                , zip                                                 as "Postleitzahl"
                                , city                                                as "Ort"
                                , 'Deutschland'                                       as "Land"
                                , contact.mobile_phone                                as "Telefon-1"
                                , NULL                                                as "Telefon-2"
                                , NULL                                                as "Leerfeld2"
                                , NULL                                                as "Telefax"
                                , NULL                                                as "Leerfeld3"
                                , to_char( now( )::date, 'YYYYMMDD' )                 as "Übertragungsdatum"
                                , 'N'                                                 as "Status"
                                , '10000004'                                          as "Übertragungsnummer"
                            from fdw_customer_service.merchant
                                    left join fdw_customer_service.merchant_has_contacts
                                                on merchant.id_merchant = merchant_has_contacts.fk_merchant
                                    left join fdw_customer_service.contact on merchant_has_contacts.fk_contact
                                = contact.id_contact
                                    left join fdw_customer_service.address on address.id_address = merchant.fk_address
                            where merchant.name not like '%%test%%' and merchant.name not like '%%Test%%'
                        )


        , customers as (
                            select '01'                                                as "Satzart"
                                , '00000001'                                          as "Empfänger"
                                , '43999023'                                          as "Absender"
                                , '01'                                                as "Versionsnummer"
                                , concat( 'O_', id_customer )                         as "Hersteller-Kunden-Nr."
                                , NULL                                                as "GEDAT-Adressnummer"
                                , NULL                                                as "GLN"
                                , NULL                                                as "Leerfeld"
                                , NULL                                                as "Leerfeld1"
                                , NULL                                                as "Gemeindekennziffer"
                                , NULL                                                as "Geschäftstyp"
                                , customer.name                                       as "Name-1 (Bezeichnung)"
                                , customer.name2                                      as "Name-2 (Inhaber)"
                                , customer.short_name                                 as "Kurzbezeichnung"
                                , concat( address.street, ' ', address.house_number ) as "Straße u. Hausnummer"
                                , address.zip                                         as "Postleitzahl"
                                , address.city                                        as "Ort"
                                , address.country                                     as "Land"
                                , contact.mobile_phone                                as "Telefon-1"
                                , NULL                                                as "Telefon-2"
                                , NULL                                                as "Leerfeld2"
                                , NULL                                                as "Telefax"
                                , NULL                                                as "Leerfeld3"
                                , to_char( now( )::date, 'YYYYMMDD' )                 as "Übertragungsdatum"
                                , 'N'                                                 as "Status"
                                , '10000004'                                          as "Übertragungsnummer"
                            from fdw_customer_service.customer
                                    left join fdw_customer_service.address
                                                on customer.fk_address = address.id_address
                                    left join fdw_customer_service.customer_has_contacts
                                                on customer.id_customer = customer_has_contacts.fk_customer
                                    left join fdw_customer_service.contact
                                                on customer_has_contacts.fk_contact = contact.id_contact
                            where customer.name not like '%%test%%' and customer.name not like '%%Test%%'

                        )


        , final as (
                        select *
                        from customers

                        union
                        select *
                        from merchants
                    )

        select *
        from final
        where "Straße u. Hausnummer" not like '' 
            and "Name-1 (Bezeichnung)" not like '%Aaa%' 
            and "Name-1 (Bezeichnung)" not like '%itte löschen%' 
            and "Name-1 (Bezeichnung)" not like '%AAA%'
                                    """
    from sqlalchemy import text
    df= pd.read_sql(query,con=pg_engine)
    logging.info(f'Data loaded, closing connection and tunnel')



    dt_string = (datetime.now() - timedelta(days=0)).strftime("%Y-%m-%d-%H-%M-00")



    filename = f"Kunden_{dt_string}.txt"
    df.to_csv(filename, sep="@", index=False
        #     , encoding="windows-1252"
    #           , encoding="iso8859-15"
    #                     , encoding="iso8859-15"
            
                        , encoding="latin9",errors='replace'
            , header=False,quoting=csv.QUOTE_ALL,
            escapechar='"',
            line_terminator='\r\n')

    ########## Upload List of customers to SFTP

    path = '/kollex-transfer/gfgh/gedat/kollex'
    SFTP_HOST = Variable.get("SFTP_HOST")
    SFTP_USER = Variable.get("SFTP_USER")
    SFTP_PASS = Variable.get("SFTP_PASS")


    # ########## Upload the table to the SFTP
    # client = paramiko.SSHClient()
    # client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # client.connect(SFTP_HOST, username=SFTP_USER, password=SFTP_PASS)

    # sftp = client.open_sftp()
    # sftp.put(f"{filename}", f"{path}/{filename}")
    # logging.info(f'Uploaded: {filename} to {path}')
    # ########## Remove all Kunden files that were created to be uploaded to gedat

    # import glob
    # files_to_remove =glob.glob('Kunden_*.txt')
    # import os

    # [os.remove(file) for file in files_to_remove]

    # sftp.close()


    ########## Download all files from SFTP

    path = '/kollex-transfer/gfgh/gedat/GEDAT/'
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(SFTP_HOST, username=SFTP_USER, password=SFTP_PASS)



    sftp = client.open_sftp()



    files= list(sftp.listdir(path))
    files_to_move = list(filter(lambda x :x if '.txt' in x else None,files))


    ############### Move everything to History
    for file in files_to_move:

        sftp.rename(f"{path}/{file}", f"{path}/history/{file}")
        # logging.info(f'Moved {file} to /history folder on remote')
    ############### Download everthing from History
    import os 
    if not os.path.exists("gedat"):
        os.makedirs("gedat")
    os.chdir("gedat")
    path = '/kollex-transfer/gfgh/gedat/GEDAT/history' 
    files= list(sftp.listdir(path))
    for file in files:
        # print(f"{path}/{file}")
        sftp.get(f"{path}/{file}", f"{file}")
        # logging.info(f'Downloaded locally: {file}')
        
        # sftp.rename(f"{path}/{file}", f"{path}/history/history/{file}")
        # logging.info(f'Moved {file} to /history folder on remote')
    import glob 

    Downloaded_files =glob.glob('Kunden_result*.txt')

    df_to_upload = pd.DataFrame()



    ########### Appending these TXT files into one Dataframe
    col_names = [
            'KOLLEX_ID',
            'KOLLEX_ADR_ID',
            'GEDAT_ID',
            'GEDAT_NAME_1',
            'GEDAT_NAME_2',
            'GEDAT_STRASSE_1',
            'GEDAT_PLZ',
            'GEDAT_ORT',
            'GEDAT_GT',
            'GEDAT_TEL',
            'GEDAT_EMAIL',
            'EXPDATE'
        ]
    for file in Downloaded_files:
     df_to_upload= df_to_upload.append(pd.read_csv(file, sep = "	",names=col_names))


    #### Lowering the case of the columns and removing headers from DF
    df_to_upload=df_to_upload.rename(columns=str.lower)
    df_to_upload = df_to_upload[df_to_upload['kollex_id']!='KOLLEX_ID']

    ########## Upload results to DWH
    df_to_upload.to_sql( con = pg_engine
                        ,name='gedat_results'
                        ,schema='sheet_loader'
                        ,if_exists='replace'
                        ,index=False)
    pg_engine.dispose()

    ### remove all trash files

    files_to_remove =glob.glob('Kunden_*.txt')
    import os
    ########## Remove all Kunden files that were downloaded
    [os.remove(file) for file in files_to_remove]
    os.chdir('..')
    os.rmdir("gedat")