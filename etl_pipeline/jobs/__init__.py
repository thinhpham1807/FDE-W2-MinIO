from dagster import AssetSelection, define_asset_job

bronze_layer_job = define_asset_job(
    name="bronze_layer_job",
    selection=AssetSelection.groups("bronze"),
)

silver_layer_job = define_asset_job(
    name="silver_layer_job",
    selection=AssetSelection.groups("silver"),
)

gold_layer_job = define_asset_job(
    name="gold_layer_job",
    selection=AssetSelection.groups("gold"),
)