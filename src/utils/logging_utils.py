# Databricks notebook source
"""
Centralized logging utilities for the cargo fleet project
"""

import logging
from datetime import datetime
from pyspark.sql import SparkSession

def get_logger(name, level=logging.INFO):
    """
    Create a configured logger instance
    
    Args:
        name: Logger name (usually module name)
        level: Logging level (default INFO)
    
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Clear existing handlers
    logger.handlers = []
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    
    # Format
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(formatter)
    
    logger.addHandler(console_handler)
    
    return logger

def log_to_table(spark, catalog_name, pipeline_name, log_level, message, details=None):
    """
    Log message to centralized logging table
    
    Args:
        spark: SparkSession
        catalog_name: Unity Catalog name
        pipeline_name: Name of the pipeline/notebook
        log_level: Log level (INFO, WARNING, ERROR)
        message: Log message
        details: Additional details (optional)
    """
    try:
        log_data = [(
            f"{pipeline_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}",
            pipeline_name,
            log_level,
            message,
            str(details) if details else None,
            datetime.now(),
            catalog_name.split('_')[-1]  # Extract environment
        )]
        
        log_df = spark.createDataFrame(
            log_data,
            ["log_id", "pipeline_name", "log_level", "message", "details", "timestamp", "environment"]
        )
        
        log_df.write.mode("append").saveAsTable(f"{catalog_name}.bronze.pipeline_logs")
        
    except Exception as e:
        # Fallback to console logging if table write fails
        logger = get_logger("log_to_table_fallback")
        logger.error(f"Failed to write to log table: {str(e)}")