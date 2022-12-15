# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "mlops-end2end",
  "category": "data-science",
  "title": "MLOps - End 2 end pipeline",
  "description": "Automate your model deployment with MLFlow webhook & repo, end 2 end!",
  "bundle": True,
  "tags": [{"ds": "Data Science"}],
  "notebooks": [
    {
      "path": "_resources/00-setup",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title":  "Setup",
      "description": "Init data for demo."
    },
    {
      "path": "_resources/API_Helpers",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title":  "Setup",
      "description": "Init data for demo."
    },
    {
      "path": "_resources/00-prep-data-db-sql",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title":  "Setup",
      "description": "Init data for demo."
    },
    {
      "path": "00_mlops_end2end_presentation", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "MLOps end2end presentation", 
      "description": "Understand MLOps and the flow we'll implement for Customer Churn detection."
    },
    {
      "path": "01_feature_engineering", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Feature engineering & Feature store for Auto-ML", 
      "description": "Create and save your features to Feature store.",
      "parameters": {"force_refresh_automl": "true"}
    },
    {
      "path": "02_automl_baseline", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Train ML model using AutoML best run", 
      "description": "Leverage Auto-ML generated notebook to build the best model out of the bpox."
    },
    {
      "path": "03_webhooks_setup", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Setup MLFlow webhooks", 
      "description": "Setup webhooks to trigger action on model registration (One-off)"
    },
    {
      "path": "04_from_notebook_to_registry", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Identify column, PK & FK", 
      "description": "Register our best model to MLFlow registry."
    },
    {
      "path": "05_job_staging_validation", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Model validation job", 
      "description": "Job to validate our model before staging (triggered by webhooks)."
    },
    {
      "path": "06_staging_inference", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Deploy model for inference", 
      "description": "Retrive model & run inferences to build our final table & build DBSQL dashboard."
    },
    {
      "path": "07_retrain_churn_automl", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Build job to retrain model", 
      "description": "Retrain periodically our model (or after drift)."
    }
  ],
  "init_job": {
    "settings": {
      "name": "demos_mlops_end2end_init_{{CURRENT_USER_NAME}}",
      "email_notifications": {
          "no_alert_for_skipped_runs": False
      },
      "timeout_seconds": 0,
      "max_concurrent_runs": 1,
      "tasks": [
          {
              "task_key": "init_data",
              "notebook_task": {
                  "notebook_path": "{{DEMO_FOLDER}}/_resources/00-prep-data-db-sql",
                  "source": "WORKSPACE"
              },
              "job_cluster_key": "Shared_job_cluster",
              "timeout_seconds": 0,
              "email_notifications": {}
          }
      ],
      "job_clusters": [
          {
              "job_cluster_key": "Shared_job_cluster",
              "new_cluster": {
                  "spark_version": "11.1.x-scala2.12",
                  "spark_conf": {},
                  "spark_env_vars": {
                      "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                  },
                  "enable_elastic_disk": True,
                  "data_security_mode": "SINGLE_USER",
                  "runtime_engine": "STANDARD",
                  "num_workers": 3
              }
          }
      ],
      "format": "MULTI_TASK"
    }
  }
}
