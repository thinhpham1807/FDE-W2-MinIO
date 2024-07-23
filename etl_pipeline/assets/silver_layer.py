import pandas as pd
from dagster import asset, Output, Definitions
from dagster import multi_asset, MetadataValue
from dagster import AssetExecutionContext, Output
from dagster import AssetIn, AssetOut

COMPUTE_KIND = "MinIO"
LAYER = "silver_layer"

# Define @multi_asset dim_products
@multi_asset(
    ins={
        "bronze_olist_products_dataset": AssetIn(
            key_prefix=["bronze", "ecom"]
            ),
        "bronze_product_category_name_translation": AssetIn(
            key_prefix=["bronze", "ecom"]
            ),
    },
    io_manager_key="minio_io_manager",
    key_prefix=["silver", "ecom", "dim_products"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER
)
def dim_products(context, bronze_olist_products_dataset, bronze_product_category_name_translation) -> Output[pd.DataFrame]:
    """
    This function creates a dimension table for products
    """
    df_products = bronze_olist_products_dataset
    df_categories = bronze_product_category_name_translation

    dim_products_df = pd.merge(df_products, df_categories, on="product_category_name")

    return Output(
        dim_products_df,
        metadata={
            "table": "dim_products",
            "records count": len(dim_products_df),
        },
    )

# Define @multi_asset fact_sales
@multi_asset(
    ins={
        "bronze_olist_order_items_dataset": AssetIn(
            key_prefix=["bronze", "ecom"]
            ),
        "bronze_olist_order_payments_dataset": AssetIn(
            key_prefix=["bronze", "ecom"]
            ),
        "bronze_olist_orders_dataset": AssetIn(
            key_prefix=["bronze", "ecom"]
            ),
    },
    io_manager_key="minio_io_manager",
    key_prefix=["silver", "ecom", "fact_sales"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER
)
def fact_sales(
        bronze_olist_order_items_dataset, 
        bronze_olist_order_payments_dataset, 
        bronze_olist_orders_dataset
) -> Output[pd.DataFrame]:
    """
    This function creates a fact table for sales
    """
    df_items = bronze_olist_order_items_dataset
    df_payments = bronze_olist_order_payments_dataset
    df_orders = bronze_olist_orders_dataset

    fact_sales_df = pd.merge(
        pd.merge(df_items, df_payments, on="order_id"),
        df_orders, on="order_id"
    )

    return Output(
        fact_sales_df,
        metadata={
            "table": "fact_sales",
            "records count": len(fact_sales_df),
        },
    )