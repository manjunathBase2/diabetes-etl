# Databricks notebook source
# MAGIC %md
# MAGIC # Diabetes Data Analysis - Delta Live Tables Pipeline
# MAGIC
# MAGIC This DLT pipeline implements a complete diabetes data ETL workflow:
# MAGIC - **Bronze Layer**: Raw data ingestion with Autoloader from Azure container
# MAGIC - **Silver Layer**: Data cleaning, validation, and feature engineering
# MAGIC - **Gold Layer**: Business-ready aggregated tables for analytics and dashboards
# MAGIC
# MAGIC Pipeline will automatically trigger when new CSV files are uploaded to the raw folder

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Configuration
storage_account = "stmanetl"
container_name = "diabetes"
base_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net"
raw_path = f"{base_path}/raw"

# Define diabetes schema with proper nullable settings
diabetes_schema = StructType([
    StructField("Pregnancies", IntegerType(), True),
    StructField("Glucose", IntegerType(), True),
    StructField("BloodPressure", IntegerType(), True),
    StructField("SkinThickness", IntegerType(), True),
    StructField("Insulin", IntegerType(), True),
    StructField("BMI", DoubleType(), True),
    StructField("DiabetesPedigreeFunction", DoubleType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Outcome", IntegerType(), True)
])


# COMMAND ----------

# MAGIC %md
# MAGIC ## EXTRACT - Bronze Layer - Raw Data Ingestion

# COMMAND ----------

@dlt.table(
    name="diabetes_bronze",
    comment="Raw diabetes patient data ingested from CSV files in batch mode",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_or_drop("valid_file", "file_name IS NOT NULL")
def diabetes_bronze():
    """
    Bronze layer: Ingest raw CSV files incrementally (batch ingestion).
    New files will be processed only once.
    Streaming query halts after reading available files using trigger option.
    """

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .schema(diabetes_schema)
        .load(raw_path)
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("ingestion_date", current_date())
        .withColumn("file_name", regexp_extract(col("_metadata.file_path"), r"([^/]+)\.csv$", 1))
    )

# COMMAND ----------

# Create a materialized view of bronze data for batch processing
@dlt.table(
    name="diabetes_bronze_materialized",
    comment="Materialized view of bronze data for median calculations and batch processing",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def diabetes_bronze_materialized():
    """
    Materialized bronze layer: Creates a batch-queryable version of streaming bronze data
    This enables aggregations like median calculations for imputation
    """
    return dlt.read("diabetes_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TRANSFORM - Silver Layer - Data Cleaning and Feature Engineering

# COMMAND ----------

@dlt.table(
    name="diabetes_silver",
    comment="Cleaned diabetes data with dynamic median-based imputation and feature engineering",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_all({
    "valid_age": "Age > 0 AND Age < 120",
    "valid_outcome": "Outcome IN (0, 1)",
    "valid_pregnancies": "Pregnancies >= 0"
})
def diabetes_silver():
    """
    Silver layer: Apply dynamic imputation using medians from real data,
    and add feature engineering.
    """
    bronze_df = dlt.read("diabetes_bronze_materialized")
    
    # Calculate medians for imputation (excluding zeros) - this should work now
    try:
        glucose_median = bronze_df.filter(col("Glucose") > 0).select(expr("percentile_approx(Glucose, 0.5)")).collect()[0][0]
        if glucose_median is None:
            glucose_median = 117.0
    except:
        glucose_median = 117.0
    
    try:
        bp_median = bronze_df.filter(col("BloodPressure") > 0).select(expr("percentile_approx(BloodPressure, 0.5)")).collect()[0][0]
        if bp_median is None:
            bp_median = 72.0
    except:
        bp_median = 72.0
    
    try:
        skin_median = bronze_df.filter(col("SkinThickness") > 0).select(expr("percentile_approx(SkinThickness, 0.5)")).collect()[0][0]
        if skin_median is None:
            skin_median = 23.0
    except:
        skin_median = 23.0
    
    try:
        insulin_median = bronze_df.filter(col("Insulin") > 0).select(expr("percentile_approx(Insulin, 0.5)")).collect()[0][0]
        if insulin_median is None:
            insulin_median = 125.0
    except:
        insulin_median = 125.0
    
    try:
        bmi_median = bronze_df.filter(col("BMI") > 0).select(expr("percentile_approx(BMI, 0.5)")).collect()[0][0]
        if bmi_median is None:
            bmi_median = 32.3
    except:
        bmi_median = 32.3


    # Apply cleaning and create imputation flags
    cleaned_df = bronze_df.withColumn(
        "Glucose", when(col("Glucose") == 0, glucose_median).otherwise(col("Glucose"))
    ).withColumn(
        "glucose_imputed", when(col("Glucose") == 0, True).otherwise(False)
    ).withColumn(
        "BloodPressure", when(col("BloodPressure") == 0, bp_median).otherwise(col("BloodPressure"))
    ).withColumn(
        "bp_imputed", when(col("BloodPressure") == 0, True).otherwise(False)
    ).withColumn(
        "SkinThickness", when(col("SkinThickness") == 0, skin_median).otherwise(col("SkinThickness"))
    ).withColumn(
        "skin_imputed", when(col("SkinThickness") == 0, True).otherwise(False)
    ).withColumn(
        "Insulin", when(col("Insulin") == 0, insulin_median).otherwise(col("Insulin"))
    ).withColumn(
        "insulin_imputed", when(col("Insulin") == 0, True).otherwise(False)
    ).withColumn(
        "BMI", when(col("BMI") == 0, bmi_median).otherwise(col("BMI"))
    ).withColumn(
        "bmi_imputed", when(col("BMI") == 0, True).otherwise(False)
    )
    
    # Add transformation timestamp
    cleaned_df = cleaned_df.withColumn("transformation_timestamp", current_timestamp())
    
    # Feature engineering with explicit type casting
    enhanced_df = (cleaned_df
        # Age groups
        .withColumn("age_group",
            when(col("Age") < 30, lit("Young (< 30)"))
            .when(col("Age") < 40, lit("Adult (30-39)"))
            .when(col("Age") < 50, lit("Middle Age (40-49)"))
            .when(col("Age") < 60, lit("Mature (50-59)"))
            .otherwise(lit("Senior (60+)"))
        )
        
        # BMI categories
        .withColumn("bmi_category",
            when(col("BMI") < 18.5, lit("Underweight"))
            .when(col("BMI") < 25, lit("Normal"))
            .when(col("BMI") < 30, lit("Overweight"))
            .otherwise(lit("Obese"))
        )
        
        # Glucose level categories
        .withColumn("glucose_level",
            when(col("Glucose") < 100, lit("Normal"))
            .when(col("Glucose") < 126, lit("Prediabetic"))
            .otherwise(lit("Diabetic Range"))
        )
        
        # Blood pressure categories
        .withColumn("bp_category",
            when(col("BloodPressure") < 80, lit("Normal"))
            .when(col("BloodPressure") < 90, lit("High Normal"))
            .when(col("BloodPressure") < 100, lit("Mild Hypertension"))
            .otherwise(lit("Hypertension"))
        )
        
        # Pregnancy risk categories
        .withColumn("pregnancy_risk",
            when(col("Pregnancies") == 0, lit("No Pregnancies"))
            .when(col("Pregnancies") <= 2, lit("Low Risk"))
            .when(col("Pregnancies") <= 5, lit("Moderate Risk"))
            .otherwise(lit("High Risk"))
        )
        
        # Calculate comprehensive risk score with explicit casting
        .withColumn("risk_score",
            (
                (col("Glucose").cast(DoubleType()) / 200.0) * 0.25 +
                (col("BMI").cast(DoubleType()) / 50.0) * 0.20 +
                (col("Age").cast(DoubleType()) / 100.0) * 0.15 +
                (col("Pregnancies").cast(DoubleType()) / 20.0) * 0.10 +
                (col("BloodPressure").cast(DoubleType()) / 200.0) * 0.10 +
                (col("DiabetesPedigreeFunction") / 2.5) * 0.10 +
                (col("Insulin").cast(DoubleType()) / 1000.0) * 0.05 +
                (col("SkinThickness").cast(DoubleType()) / 100.0) * 0.05
            ).cast(DoubleType())
        )
        
        # Risk level based on risk score
        .withColumn("risk_level",
            when(col("risk_score") < 0.4, lit("Low"))
            .when(col("risk_score") < 0.6, lit("Medium"))
            .otherwise(lit("High"))
        )
        
        # Data quality score (percentage of non-imputed values)
        .withColumn("data_quality_score",
            (
                when(col("glucose_imputed"), lit(0)).otherwise(lit(20)) +
                when(col("bp_imputed"), lit(0)).otherwise(lit(20)) +
                when(col("skin_imputed"), lit(0)).otherwise(lit(20)) +
                when(col("insulin_imputed"), lit(0)).otherwise(lit(20)) +
                when(col("bmi_imputed"), lit(0)).otherwise(lit(20))
            ).cast(IntegerType())
        )
    )
    
    return enhanced_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer - Aggregated Analytics Tables

# COMMAND ----------

@dlt.table(
    name="diabetes_demographics_summary",
    comment="Patient demographics analysis by age, BMI, and pregnancy risk",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def diabetes_demographics_summary():
    """
    Gold layer: Demographics summary for business analytics
    """
    silver_df = dlt.read("diabetes_silver")
    
    return (silver_df
        .groupBy("age_group", "bmi_category", "pregnancy_risk")
        .agg(
            count("*").alias("patient_count"),
            sum(col("Outcome").cast(IntegerType())).alias("diabetes_cases"),
            round(avg("Age"), 2).alias("avg_age"),
            round(avg("BMI"), 2).alias("avg_bmi"),
            round(avg("Glucose"), 2).alias("avg_glucose"),
            round(avg("BloodPressure"), 2).alias("avg_blood_pressure"),
            round(avg("risk_score"), 3).alias("avg_risk_score"),
            round(avg("data_quality_score"), 2).alias("avg_data_quality"),
            min("Age").alias("min_age"),
            max("Age").alias("max_age")
        )
        .withColumn("diabetes_rate", 
            round((col("diabetes_cases").cast(DoubleType()) / col("patient_count").cast(DoubleType())) * 100, 2)
        )
        .withColumn("created_at", current_timestamp())
        .orderBy("age_group", "bmi_category", "pregnancy_risk")
    )

@dlt.table(
    name="diabetes_risk_analysis",
    comment="Comprehensive diabetes risk factor analysis",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def diabetes_risk_analysis():
    """
    Gold layer: Risk analysis summary focusing on diabetes risk factors
    """
    silver_df = dlt.read("diabetes_silver")
    
    return (silver_df
        .groupBy("risk_level", "glucose_level", "bp_category")
        .agg(
            count("*").alias("patient_count"),
            sum(col("Outcome").cast(IntegerType())).alias("diabetes_cases"),
            round(avg("risk_score"), 3).alias("avg_risk_score"),
            round(stddev("risk_score"), 3).alias("stddev_risk_score"),
            round(avg("DiabetesPedigreeFunction"), 3).alias("avg_pedigree_function"),
            round(avg("Insulin"), 2).alias("avg_insulin"),
            round(avg("SkinThickness"), 2).alias("avg_skin_thickness"),
            countDistinct("age_group").alias("age_groups_represented"),
            round(avg("data_quality_score"), 2).alias("avg_data_quality")
        )
        .withColumn("diabetes_rate",
            round((col("diabetes_cases").cast(DoubleType()) / col("patient_count").cast(DoubleType())) * 100, 2)
        )
        .withColumn("risk_score_range",
            concat(
                format_number(col("avg_risk_score") - coalesce(col("stddev_risk_score"), lit(0.0)), 3),
                lit(" - "),
                format_number(col("avg_risk_score") + coalesce(col("stddev_risk_score"), lit(0.0)), 3)
            )
        )
        .withColumn("created_at", current_timestamp())
        .orderBy("risk_level", "glucose_level", "bp_category")
    )

@dlt.table(
    name="diabetes_executive_summary",
    comment="Executive dashboard metrics and KPIs",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def diabetes_executive_summary():
    """
    Gold layer: Executive summary with key business metrics
    """
    silver_df = dlt.read("diabetes_silver")
    
    # Calculate metrics using standard aggregations
    metrics_df = silver_df.agg(
        count("*").alias("total_patients"),
        sum(when(col("Outcome") == 1, 1).otherwise(0)).alias("diabetes_cases"),
        sum(when(col("risk_level") == "High", 1).otherwise(0)).alias("high_risk_patients"),
        round(avg("Age"), 1).alias("avg_age"),
        round(avg("risk_score"), 3).alias("avg_risk_score"),
        round(avg("data_quality_score"), 1).alias("data_quality_score")
    )
    
    # Add percentage calculations with explicit casting
    metrics_with_percentages = metrics_df.withColumns({
        "diabetes_percentage": round((col("diabetes_cases").cast(DoubleType()) / col("total_patients").cast(DoubleType())) * 100, 2),
        "high_risk_percentage": round((col("high_risk_patients").cast(DoubleType()) / col("total_patients").cast(DoubleType())) * 100, 2)
    })
    
    # Convert to long format for better dashboard consumption
    summary_df = metrics_with_percentages.select(
        lit("summary").alias("summary_type"),
        col("total_patients").cast(DoubleType()).alias("total_patients"),
        col("diabetes_cases").cast(DoubleType()).alias("diabetes_cases"),
        col("diabetes_percentage"),
        col("high_risk_patients").cast(DoubleType()).alias("high_risk_patients"),
        col("high_risk_percentage"),
        col("avg_age"),
        col("avg_risk_score"),
        col("data_quality_score"),
        current_date().alias("summary_date"),
        current_timestamp().alias("created_at")
    )
    
    return summary_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer - Dashboard Views

# COMMAND ----------

@dlt.view(
    name="v_demographics_dashboard",
    comment="Dashboard view for demographics analysis"
)
def v_demographics_dashboard():
    """
    Dashboard view for demographics visualization
    """
    return (dlt.read("diabetes_demographics_summary")
        .select(
            "age_group", "bmi_category", "pregnancy_risk", 
            "patient_count", "diabetes_rate", "avg_risk_score",
            "avg_age", "avg_bmi", "avg_glucose", "created_at"
        )
    )

@dlt.view(
    name="v_risk_analysis_dashboard", 
    comment="Dashboard view for risk analysis"
)
def v_risk_analysis_dashboard():
    """
    Dashboard view for risk analysis visualization
    """
    return (dlt.read("diabetes_risk_analysis")
        .select(
            "risk_level", "glucose_level", "bp_category",
            "patient_count", "diabetes_rate", "avg_risk_score",
            "avg_pedigree_function", "avg_insulin", "created_at"
        )
    )

@dlt.view(
    name="v_executive_summary_dashboard",
    comment="Dashboard view for executive summary"
)
def v_executive_summary_dashboard():
    """
    Dashboard view for executive KPIs
    """
    return (dlt.read("diabetes_executive_summary")
        .select(
            "total_patients", "diabetes_cases", "diabetes_percentage",
            "high_risk_patients", "high_risk_percentage", "avg_age",
            "avg_risk_score", "data_quality_score", "summary_date", "created_at"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Monitoring Tables

# COMMAND ----------

@dlt.table(
    name="diabetes_data_quality_metrics",
    comment="Data quality monitoring and metrics tracking",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def diabetes_data_quality_metrics():
    """
    Gold layer: Data quality metrics for monitoring
    """
    silver_df = dlt.read("diabetes_silver")
    
    return (silver_df
        .withColumn("processing_date", current_date())
        .groupBy("processing_date", "source_file")
        .agg(
            count("*").alias("total_records"),
            sum(when(col("glucose_imputed"), 1).otherwise(0)).alias("glucose_imputed_count"),
            sum(when(col("bp_imputed"), 1).otherwise(0)).alias("bp_imputed_count"),
            sum(when(col("skin_imputed"), 1).otherwise(0)).alias("skin_imputed_count"),
            sum(when(col("insulin_imputed"), 1).otherwise(0)).alias("insulin_imputed_count"),
            sum(when(col("bmi_imputed"), 1).otherwise(0)).alias("bmi_imputed_count"),
            round(avg("data_quality_score"), 2).alias("avg_data_quality_score"),
            min("data_quality_score").alias("min_data_quality_score"),
            max("data_quality_score").alias("max_data_quality_score")
        )
        .withColumn("total_imputed_fields",
            (col("glucose_imputed_count") + col("bp_imputed_count") + 
             col("skin_imputed_count") + col("insulin_imputed_count") + col("bmi_imputed_count")).cast(IntegerType())
        )
        .withColumn("imputation_rate",
            round((col("total_imputed_fields").cast(DoubleType()) / (col("total_records").cast(DoubleType()) * 5)) * 100, 2)
        )
        .withColumn("created_at", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dashboard Refresh Trigger Table

# COMMAND ----------

@dlt.table(
    name="dashboard_refresh_log",
    comment="Log table to track dashboard refresh events and trigger automation",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def dashboard_refresh_log():
    """
    Gold layer: Dashboard refresh tracking for automation triggers
    This table can be monitored to trigger dashboard refreshes
    """
    # Get the latest pipeline run information
    silver_df = dlt.read("diabetes_silver")
    
    pipeline_stats = silver_df.agg(
        count("*").alias("total_records_processed"),
        countDistinct("source_file").alias("files_processed"),
        max("ingestion_timestamp").alias("latest_ingestion"),
        max("transformation_timestamp").alias("latest_transformation"),
        round(avg("data_quality_score"), 2).alias("overall_data_quality")
    )
    
    return (pipeline_stats
        .withColumn("pipeline_run_id", expr("uuid()"))
        .withColumn("pipeline_completion_time", current_timestamp())
        .withColumn("status", lit("COMPLETED"))
        .withColumn("next_dashboard_refresh_due", 
            current_timestamp()  # Immediate refresh
        )
        .withColumn("refresh_priority", 
            when(col("overall_data_quality") < 80, lit("HIGH"))
            .when(col("total_records_processed") > 1000, lit("HIGH")) 
            .otherwise(lit("NORMAL"))
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Monitoring and Alerting

# COMMAND ----------

@dlt.table(
    name="pipeline_health_metrics",
    comment="Pipeline health and performance monitoring",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def pipeline_health_metrics():
    """
    Monitor pipeline health and performance
    """
    bronze_df = dlt.read("diabetes_bronze")  
    
    return (bronze_df
        .withColumn("processing_hour", date_format(col("ingestion_timestamp"), "yyyy-MM-dd HH"))
        .groupBy("processing_hour", "file_name")
        .agg(
            count("*").alias("records_processed"),
            countDistinct("source_file").alias("unique_files"),
            min("ingestion_timestamp").alias("first_record_time"),
            max("ingestion_timestamp").alias("last_record_time")
        )
        .withColumn("processing_duration_minutes",
            (unix_timestamp("last_record_time") - unix_timestamp("first_record_time")) / 60.0
        )
        .withColumn("records_per_minute",
            when(col("processing_duration_minutes") > 0, 
                round(col("records_processed").cast(DoubleType()) / col("processing_duration_minutes"), 2)
            ).otherwise(col("records_processed").cast(DoubleType()))
        )
        .withColumn("health_status",
            when(col("records_processed") == 0, lit("ERROR"))
            .when(col("records_per_minute") < 10, lit("SLOW"))
            .otherwise(lit("HEALTHY"))
        )
        .withColumn("created_at", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Helper Functions for Advanced Analytics

# COMMAND ----------

@dlt.table(
    name="diabetes_feature_correlation",
    comment="Feature correlation analysis for model insights",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def diabetes_feature_correlation():
    """
    Gold layer: Feature correlation analysis
    """
    silver_df = dlt.read("diabetes_silver")
    
    # Create correlation pairs analysis
    correlation_analysis = (silver_df
        .groupBy("age_group", "bmi_category")
        .agg(
            count("*").alias("sample_size"),
            corr("Glucose", "BMI").alias("glucose_bmi_corr"),
            corr("Age", "Pregnancies").alias("age_pregnancies_corr"),
            corr("BloodPressure", "BMI").alias("bp_bmi_corr"),
            corr("Insulin", "Glucose").alias("insulin_glucose_corr"),
            round(avg("Outcome"), 3).alias("diabetes_prevalence")
        )
        .withColumn("correlation_strength",
            when(abs(col("glucose_bmi_corr")) > 0.7, lit("Strong"))
            .when(abs(col("glucose_bmi_corr")) > 0.4, lit("Moderate"))
            .otherwise(lit("Weak"))
        )
        .withColumn("created_at", current_timestamp())
    )
    
    return correlation_analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Validation Summary

# COMMAND ----------

@dlt.table(
    name="data_validation_summary",
    comment="Summary of data validation results and data quality checks",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def data_validation_summary():
    """
    Gold layer: Data validation summary
    """
    silver_df = dlt.read("diabetes_silver")
    
    validation_summary = (silver_df
        .agg(
            count("*").alias("total_records"),
            sum(when((col("Age") > 0) & (col("Age") < 120), 1).otherwise(0)).alias("valid_age_count"),
            sum(when(col("Outcome").isin([0, 1]), 1).otherwise(0)).alias("valid_outcome_count"),
            sum(when(col("Pregnancies") >= 0, 1).otherwise(0)).alias("valid_pregnancies_count"),
            sum(when(col("Glucose") > 0, 1).otherwise(0)).alias("valid_glucose_count"),
            sum(when(col("BMI") > 0, 1).otherwise(0)).alias("valid_bmi_count")
        )
        .withColumn("age_validity_rate", 
            round((col("valid_age_count").cast(DoubleType()) / col("total_records").cast(DoubleType())) * 100, 2))
        .withColumn("outcome_validity_rate", 
            round((col("valid_outcome_count").cast(DoubleType()) / col("total_records").cast(DoubleType())) * 100, 2))
        .withColumn("pregnancies_validity_rate", 
            round((col("valid_pregnancies_count").cast(DoubleType()) / col("total_records").cast(DoubleType())) * 100, 2))
        .withColumn("glucose_validity_rate", 
            round((col("valid_glucose_count").cast(DoubleType()) / col("total_records").cast(DoubleType())) * 100, 2))
        .withColumn("bmi_validity_rate", 
            round((col("valid_bmi_count").cast(DoubleType()) / col("total_records").cast(DoubleType())) * 100, 2))
        .withColumn("overall_data_quality",
            round((col("age_validity_rate") + col("outcome_validity_rate") + 
                   col("pregnancies_validity_rate") + col("glucose_validity_rate") + 
                   col("bmi_validity_rate")) / 5, 2))
        .withColumn("validation_timestamp", current_timestamp())
        .withColumn("validation_date", current_date())
    )
    
    return validation_summary