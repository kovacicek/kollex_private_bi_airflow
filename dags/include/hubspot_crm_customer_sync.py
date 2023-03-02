import gspread as gs
import pandas as pd

import gspread_dataframe as gd
from sqlalchemy import create_engine
from airflow.models import Variable


def hubspot_sync():
    pg_host = Variable.get("PG_HOST")
    pg_user = Variable.get("PG_USERNAME_WRITE")
    pg_password = Variable.get("PG_PASSWORD_WRITE")
    pg_database = Variable.get("PG_DATABASE")
    pg_connect_string = (
        f"postgresql://{pg_user}:{pg_password}@{pg_host}/{pg_database}"
    )
    pg_engine = create_engine(
        f"{pg_connect_string}", echo=False, pool_pre_ping=True, pool_recycle=800
    )

    sheet_name = Variable.get("HUBSPOT_SHEET_NAME")
    df = pd.read_sql(
        """
        with hubspot as (
        with parent_ids as (select DISTINCT(fk_customer) from fdw_customer_service.customer_has_user_and_role),
         addresses as (select * from fdw_customer_service.address)
         ,users as (select * from fdw_customer_service.user)
         ,customers_emails as (
    select 
    min(customer_has_user_and_role."fk_customer") as "fk_customer", 
    customer_has_user_and_role."fk_user", 
    customer_has_user_and_role."fk_role", 
    u."email",
    u."mobile_phone",
    u.first_name,
    u.last_name,
    u.additional_data->>'sources' as "source",
    coalesce(bbqr."Submitted At", ruuQR."Submitted At__ti",cokeExpressQR."Submitted At",cokeShopQR."Submitted At") as "Wann JV-Typeform ausgefüllt"
    from fdw_customer_service.user u
    left join fdw_customer_service.customer_has_user_and_role
    on "fk_user" = "id_user"
    left join 
        sheet_loader.bitburger_qr bbqr on u."email" = bbqr."Wie lautet deine E-Mail-Adresse?"
    left join
        sheet_loader.ruu_shop_typeform ruuQR on ruuQR."Email" = u.email
    left join
        sheet_loader.coke_express_type_form cokeExpressQR on cokeExpressQR."Wie lautet deine E-Mail-Adresse?" = u.email
     left join
        sheet_loader.coke_shop_type_form cokeShopQR on cokeShopQR."Wie lautet deine E-Mail-Adresse?" = u.email
     left join
        fdw_customer_service.customer_has_user_and_role chur on chur."fk_user" = u."id_user"
    where customer_has_user_and_role."fk_role" = 1 
    group by customer_has_user_and_role."fk_customer", customer_has_user_and_role."fk_user", customer_has_user_and_role."fk_role", u."email", u."mobile_phone", "source", u.first_name, u.last_name, "Wann JV-Typeform ausgefüllt"
) 
, main as (SELECT *
    FROM fdw_customer_service.customer customer
    LEFT JOIN parent_ids
    ON fk_customer = id_customer
    WHERE fk_customer IS NOT NULL)
, childs as (select chp."fk_customer" as "child_id", main."id_customer" as "id_parent" from fdw_customer_service.customer_has_parent chp join main on "fk_customer_parent" = "id_customer")
, merchants as (
        select chm.created_at as "Wann eingeladen", mr.name, fk_customer from fdw_customer_service.customer_has_merchant chm join fdw_customer_service.merchant mr on "fk_merchant" = "id_merchant"
)
,children_statusses as (select 
                        "child_id", 
                        "id_parent",
                        "Customer Name", 
                        "Customer Status", 
                        "Last Order Created", 
                        min("First Order Date") as "First Order Date", 
                        "average days between orders last 3 months", 
                        "average days between orders", 
                        "Status Last Order",
                        "Status Last Supplier Request",
                        "Street",
                        "Zip Code",
                        "City",
                        "Number Of Orders",
                        mr."Wann eingeladen",
                        mr.name as "merchant who invited customer"
                        from childs left join prod_info_layer.customer_table_horeca_children_customers as cth on cth."id_customer" = "child_id" 
                                    left join merchants mr on "child_id" = mr."fk_customer"
                        group by 
                        "child_id", 
                        "id_parent",
                        "Customer Name", 
                        "Customer Status", 
                        "Last Order Created",  
                        "average days between orders last 3 months",
                        "average days between orders",
                        "Status Last Order", 
                        "Status Last Supplier Request",
                        "Number Of Orders",
                         "Street",
                        "Zip Code",
                        "City",
                        "Wann eingeladen",
                        "merchant who invited customer"
                       )
, base_classified as (
    select
	c.uuid, c.name, u.email, u.additional_data->>'sources', c.created_at,
	(select bbqr."Wie lautet deine E-Mail-Adresse?" from sheet_loader.bitburger_qr bbqr where bbqr."Wie lautet deine E-Mail-Adresse?" = u.email LIMIT 1) as BitburgerQRCode,
	(select ruuQR."Email" from sheet_loader.ruu_shop_typeform ruuQR where ruuQR."Email" = u.email LIMIT 1) as RuuQRCode,
	(select cokeExpressQR."Wie lautet deine E-Mail-Adresse?" from sheet_loader.coke_express_type_form cokeExpressQR where cokeExpressQR."Wie lautet deine E-Mail-Adresse?" = u.email LIMIT 1) as cokeExpressQRCode,
	(select cokeShopQR."Wie lautet deine E-Mail-Adresse?" from sheet_loader.coke_shop_type_form cokeShopQR where cokeShopQR."Wie lautet deine E-Mail-Adresse?" = u.email LIMIT 1) as cokeShopQRCode
from fdw_customer_service.customer_has_user_and_role chur
left join fdw_customer_service.customer c on c.id_customer = chur.fk_customer
left join fdw_customer_service.user u on u.id_user = chur.fk_user
where chur.fk_role = 1
order by c.created_at DESC
)
select 
main.uuid as "customer_uuid",
main.name as "parent_customer_name",
main.created_at as "parent_customer_creation_date",
customers_emails."first_name" as "parent_customer_owner_first_name",
customers_emails."last_name" as "parent_customer_owner_last_name",
customers_emails.email as "parent_customer_owner_email",
customers_emails."mobile_phone" as "parent_customer_owner_phone",
CASE
  WHEN "bitburgerqrcode" is not null then 'Bitburger'
  WHEN "ruuqrcode" is not null then 'Ruu'
  WHEN "cokeexpressqrcode" is not null then 'Coca-Cola'
  WHEN "cokeshopqrcode" is not null then 'Coca-Cola'  
  WHEN REPLACE(REPLACE(REPLACE(customers_emails."source", '["', ''), '"]', ''),'"','') = 'Bitburger Braugruppe' then 'Bitburger'
  WHEN coalesce("bitburgerqrcode", "ruuqrcode", "cokeexpressqrcode", "cokeshopqrcode") is null and REPLACE(REPLACE(REPLACE(customers_emails."source", '["', ''), '"]', ''),'"','') = 'coca-cola' THEN 'Coca-Cola'
  WHEN coalesce("bitburgerqrcode", "ruuqrcode", "cokeexpressqrcode", "cokeshopqrcode") is null and REPLACE(REPLACE(REPLACE(customers_emails."source", '["', ''), '"]', ''),'"','') = 'coca_cola' THEN 'Coca-Cola'
  ELSE REPLACE(REPLACE(REPLACE(customers_emails."source", '["', ''), '"]', ''),'"','')
END AS "parent_customer_owner_lead_source",
customers_emails."Wann JV-Typeform ausgefüllt",
main.type,
children_statusses."First Order Date",
children_statusses."Customer Name" as "Child Customer Name",
children_statusses."Customer Status",
children_statusses."Number Of Orders",
children_statusses."Last Order Created" as "Wann zuletzt bestellt",
children_statusses."Status Last Order",
children_statusses."Status Last Supplier Request",
children_statusses."Street" as "street",
children_statusses."Zip Code" as "PLZ",
children_statusses."City" as "Ort",
case
    when children_statusses."Customer Status" = 'Aktiver Kunde letzte 28 Tage' then True else False end as "Gilt als Aktiv",
case
    when children_statusses."Customer Status" = 'Inaktiver Kunde letzte 28 Tage' then True else False end as "Gilt als inaktiv (hat bereits bestellt, ist seit 28 inaktiv)",
case
    when children_statusses."Customer Status" = 'Merchant Aktivierung offen' then True else False end as "Merchant Aktivierung offen",    
case
    when children_statusses."Customer Status" = 'Registrierter Kunde ohne Bestellung' then True else False end as "Registrierter Kunde ohne Bestellung",
children_statusses."average days between orders last 3 months" as "Wie oft durchschnittlichen in den letzten 3 Monaten bestellt",
children_statusses."average days between orders" as "Durchschnittlicher Bestellrythmus (allgemein)",
children_statusses."Wann eingeladen",
children_statusses."merchant who invited customer"
from main 
left join addresses on "fk_address" = "id_address"
left join customers_emails on customers_emails."fk_customer" = main."id_customer"
left join children_statusses on main."id_customer" = "id_parent"
left join base_classified on main."uuid" = base_classified."uuid"
)

,final1 as(
select 
 "customer_uuid",
 parent_customer_name,
 "parent_customer_creation_date",
 "parent_customer_owner_first_name",
  "parent_customer_owner_last_name",
  "parent_customer_owner_email",
  "parent_customer_owner_phone",
  "parent_customer_owner_lead_source",
  "Wann JV-Typeform ausgefüllt",
  "type",
  "First Order Date",
  "Child Customer Name",
  "Customer Status",
  "Number Of Orders",
  "Wann zuletzt bestellt",
  "Status Last Order",
  "Status Last Supplier Request",
  "street",
  "PLZ",
  "Ort",
   "Gilt als Aktiv",
    "Gilt als inaktiv (hat bereits bestellt, ist seit 28 inaktiv)",
    "Merchant Aktivierung offen",  
    "Registrierter Kunde ohne Bestellung",
    "Wie oft durchschnittlichen in den letzten 3 Monaten bestellt",
    "Durchschnittlicher Bestellrythmus (allgemein)",
    "Wann eingeladen",
    "merchant who invited customer"
from hubspot group by
  "customer_uuid",
   parent_customer_name,
  "parent_customer_creation_date",
  "parent_customer_owner_first_name",
  "parent_customer_owner_last_name",
  "parent_customer_owner_email",
  "parent_customer_owner_phone",
  "parent_customer_owner_lead_source",
  "Wann JV-Typeform ausgefüllt",
  "type",
  "First Order Date",
  "Child Customer Name",
  "Customer Status",
  "Number Of Orders",
  "Wann zuletzt bestellt",
  "Status Last Order",
  "Status Last Supplier Request",
  "street",
  "PLZ",
  "Ort",
  "Gilt als Aktiv",
  "Gilt als inaktiv (hat bereits bestellt, ist seit 28 inaktiv)",
  "Merchant Aktivierung offen",  
  "Registrierter Kunde ohne Bestellung",
  "Wie oft durchschnittlichen in den letzten 3 Monaten bestellt",
  "Durchschnittlicher Bestellrythmus (allgemein)",
  "Wann eingeladen",
  "merchant who invited customer"
)

, final2 as (
select * from (
      SELECT final1.*, ROW_NUMBER() OVER (PARTITION BY customer_uuid ORDER BY "First Order Date") AS rn
  FROM final1
) sub

where rn =1
)

select
  final2."customer_uuid",
  final2.parent_customer_name,
  final2."parent_customer_creation_date",
  final2."parent_customer_owner_first_name",
  final2."parent_customer_owner_last_name",
  final2."parent_customer_owner_email",
  final2."parent_customer_owner_phone",
  final2."parent_customer_owner_lead_source",
  final2."Wann JV-Typeform ausgefüllt",
  final2."type",
  final2."First Order Date",
  final2"Child Customer Name",
  final2."Customer Status",
  final2"Number Of Orders",
  final2."Wann zuletzt bestellt",
  final2."Status Last Order",
  final2."Status Last Supplier Request",
  final2."street",
  final2."PLZ",
  final2."Ort",
  final2."Gilt als Aktiv",
  final2."Gilt als inaktiv (hat bereits bestellt, ist seit 28 inaktiv)",
  final2."Merchant Aktivierung offen",  
  final2."Registrierter Kunde ohne Bestellung",
  final2."Wie oft durchschnittlichen in den letzten 3 Monaten bestellt",
  final2."Durchschnittlicher Bestellrythmus (allgemein)",
  final2."Wann eingeladen",
  final2."merchant who invited customer" 
  from final2
        """,
        con=pg_engine,
    )
    df["Number Of Orders"] = df["Number Of Orders"].astype(str)
    df["Number Of Orders"] = df["Number Of Orders"].replace("nan", "0")
    gsheet_credentials = {
        "type": Variable.get("gsheet_creds_type"),
        "project_id": Variable.get("gsheet_creds_project_id"),
        "private_key_id": Variable.get("hubspot_private_key_id"),
        "private_key": Variable.get("hubspot_private_key"),
        "client_email": Variable.get("hubspot_client_email"),
        "client_id": Variable.get("hubspot_client_id"),
        "auth_uri": Variable.get("gsheet_creds_auth_uri"),
        "token_uri": Variable.get("hubspot_token_uri"),
        "auth_provider_x509_cert_url": Variable.get(
            "gsheet_creds_auth_provider_x509_cert_url"
        ),
        "client_x509_cert_url": Variable.get("hubspot_x509_cert_url"),
    }
    gc = gs.service_account_from_dict(gsheet_credentials)
    print("loaded credentials")

    sh = gc.open_by_url(Variable.get("HUBSPOT_SPREADSHEET"))

    ws = sh.worksheet(sheet_name)
    # Get the existing data as a dataframe
    # sheet_as_df = gd.get_as_dataframe(ws)
    # # Append the new dataframe to the existing dataframe
    # df.columns = [c.replace(" ", "_") for c in df.columns]
    # sheet_as_df.columns = [c.replace(" ", "_") for c in sheet_as_df.columns]
    #
    # df["Customer_UUID"] = df["Customer_UUID"].astype(str)
    # sheet_as_df["Customer_UUID"] = sheet_as_df["Customer_UUID"].astype(str)
    #
    # current_ids = sheet_as_df["Customer_UUID"].tolist()
    # new_ids = df["Customer_UUID"].tolist()
    # missing_ids = list(set(new_ids) - set(current_ids))
    # filter_df = df[df["Customer_UUID"].isin(missing_ids)]
    #
    # df.columns = [c.replace("_", " ") for c in df.columns]
    # sheet_as_df.columns = [c.replace("_", " ") for c in sheet_as_df.columns]
    # filter_df.columns = [c.replace("_", " ") for c in filter_df.columns]
    #
    # appended_df = sheet_as_df.append(filter_df)
    # Clear the existing sheet
    ws.clear()
    gd.set_with_dataframe(ws, df)
    print("Dataframe has been appended to the sheet")
