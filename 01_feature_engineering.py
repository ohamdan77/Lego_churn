# Databricks notebook source
# MAGIC %md
# MAGIC # Churn Prediction Feature Engineering
# MAGIC Our first step is to analyze the data and build the features we'll use to train our model. Let's see how this can be done.
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlops-end2end-flow-1.png" width="1200">
# MAGIC 
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fmlops%2F02_feature_prep&dt=MLOPS">
# MAGIC <!-- [metadata={"description":"MLOps end2end workflow: Feature engineering",
# MAGIC  "authors":["quentin.ambard@databricks.com"],
# MAGIC  "db_resources":{},
# MAGIC   "search_tags":{"vertical": "retail", "step": "Data Engineering", "components": ["feature store"]},
# MAGIC                  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

# COMMAND ----------

# MAGIC %run ./_resources/00-setup $reset_all_data=false

# COMMAND ----------

# DBTITLE 1,Read in Bronze Delta table using Spark
# Read into Spark


# COMMAND ----------

# DBTITLE 1,Define featurization
# OHE the follwing columns ['gender', 'partner', 'dependents','phone_service', 'multiple_lines', 'internet_service','online_security', 'online_backup', 'device_protection','tech_support', 'streaming_tv', 'streaming_movies','contract', 'paperless_billing', 'payment_method']. The resulting schema should be as following : [customer_id: string, senior_citizen: bigint, tenure: bigint, monthly_charges: double, total_charges: string, churn: int, gender_female: bigint, gender_male: bigint, partner_no: bigint, partner_yes: bigint, dependents_no: bigint, dependents_yes: bigint, phone_service_no: bigint, phone_service_yes: bigint, multiple_lines_no: bigint, multiple_lines_no_phone_service: bigint, multiple_lines_yes: bigint, internet_service_dsl: bigint, internet_service_fiber_optic: bigint, internet_service_no: bigint, online_security_no: bigint, online_security_no_internet_service: bigint, online_security_yes: bigint, online_backup_no: bigint, online_backup_no_internet_service: bigint, online_backup_yes: bigint, device_protection_no: bigint, device_protection_no_internet_service: bigint, device_protection_yes: bigint, tech_support_no: bigint, tech_support_no_internet_service: bigint, tech_support_yes: bigint, streaming_tv_no: bigint, streaming_tv_no_internet_service: bigint, streaming_tv_yes: bigint, streaming_movies_no: bigint, streaming_movies_no_internet_service: bigint, streaming_movies_yes: bigint, contract_month_to_month: bigint, contract_one_year: bigint, contract_two_year: bigint, paperless_billing_no: bigint, paperless_billing_yes: bigint, payment_method_bank_transfer__automatic_: bigint, payment_method_credit_card__automatic_: bigint, payment_method_electronic_check: bigint, payment_method_mailed_check: bigint]

# Convert label to int

# Clean up column names by removing special characters and make sure they are lower

# Drop missing values

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Write to Feature Store
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlops-end2end-flow-feature-store.png" style="float:right" width="500" />
# MAGIC 
# MAGIC Once our features are ready, we'll save them in Databricks Feature Store. Under the hood, features store are backed by a Delta Lake table.
# MAGIC 
# MAGIC This will allow discoverability and reusability of our feature accross our organization, increasing team efficiency.
# MAGIC 
# MAGIC Feature store will bring traceability and governance in our deployment, knowing which model is dependent of which set of features.
# MAGIC 
# MAGIC Make sure you're using the "Machine Learning" menu to have access to your feature store using the UI.

# COMMAND ----------

from databricks.feature_store import FeatureStoreClient

fs = FeatureStoreClient()

# Use Databricks feature store API to create a feature store table and write the resulting features from feature engineering to it

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Accelerating Churn model creation using Databricks Auto-ML
# MAGIC ### A glass-box solution that empowers data teams without taking away control
# MAGIC 
# MAGIC Databricks simplify model creation and MLOps. However, bootstraping new ML projects can still be long and inefficient. 
# MAGIC 
# MAGIC Instead of creating the same boilerplate for each new project, Databricks Auto-ML can automatically generate state of the art models for Classifications, regression, and forecast.
# MAGIC 
# MAGIC 
# MAGIC <img width="1000" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/auto-ml-full.png"/>
# MAGIC 
# MAGIC <img style="float: right" width="600" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/churn-auto-ml.png"/>
# MAGIC 
# MAGIC Models can be directly deployed, or instead leverage generated notebooks to boostrap projects with best-practices, saving you weeks of efforts.
# MAGIC 
# MAGIC ### Using Databricks Auto ML with our Churn dataset
# MAGIC 
# MAGIC Auto ML is available in the "Machine Learning" space. All we have to do is start a new Auto-ML experimentation and select the feature table we just created (`churn_features`)
# MAGIC 
# MAGIC Our prediction target is the `churn` column.
# MAGIC 
# MAGIC Click on Start, and Databricks will do the rest.
# MAGIC 
# MAGIC While this is done using the UI, you can also leverage the [python API](https://docs.databricks.com/applications/machine-learning/automl.html#automl-python-api-1)
