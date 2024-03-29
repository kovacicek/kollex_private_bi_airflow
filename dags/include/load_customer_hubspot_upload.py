from include.db import prepare_pg_connection


def prepare_data_for_hubspot():
    pg_engine = prepare_pg_connection()
    print("Connection established.")
    connection = pg_engine.connect()

    connection.execute(f"drop table if exists prod_info_layer.customer_hubspot_upload;")
    print("Dropped table.")
    connection.execute(
        """
            CREATE TABLE prod_info_layer.customer_hubspot_upload AS
            SELECT 
                customer_order_state,
                parent_customer_owner_first_name,
                parent_customer_owner_last_name,
                parent_customer_owner_email,
                parent_customer_owner_mobile_phone,
                parent_customer_owner_lead_source,
                parent_customer_owner_creation_date,
                child_customer_uuid,
                child_customer_name,
                child_customer_status,
                child_customer_invitation_date,
                child_customer_first_order_date,
                child_customer_last_order_date,
                child_customer_number_of_orders,
                child_customer_number_of_orders_last_3_months,
                child_customer_address_street,
                child_customer_address_zip,
                child_customer_address_city,
                child_customer_average_days_between_orders,
                created_at,
                updated_at,
                current_date as last_sync,
                deleted_at
            FROM prod_info_layer.customer_information
             WHERE customer_state = 'Registered' 
                            OR  customer_state = 'Connected';
        """
    )
    print("Table created and data loaded")
