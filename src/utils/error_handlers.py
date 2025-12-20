# Databricks notebook source
"""
Error handling utilities with retry logic and alerting
"""

import time
from functools import wraps
import logging
from functools import reduce

def handle_streaming_error(exception, query_name, max_retries=3):
    """
    Handle streaming query errors with retry logic
    
    Args:
        exception: The caught exception
        query_name: Name of the streaming query
        max_retries: Maximum number of retry attempts
    """
    logger = logging.getLogger(query_name)
    
    logger.error(f"Streaming query failed: {query_name}")
    logger.error(f"Error: {str(exception)}")
    logger.error(f"Error type: {type(exception).__name__}")
    
    # Log to table if possible
    try:
        from logging_utils import log_to_table
        log_to_table(
            spark, 
            catalog_name="cargo_fleet_dev",  # Should be parameterized
            pipeline_name=query_name,
            log_level="ERROR",
            message=str(exception),
            details={"error_type": type(exception).__name__}
        )
    except:
        pass

def retry_on_failure(max_retries=3, delay_seconds=5):
    """
    Decorator for retrying functions on failure
    
    Args:
        max_retries: Maximum number of retry attempts
        delay_seconds: Delay between retries
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger = logging.getLogger(func.__name__)
            
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
                        logger.info(f"Retrying in {delay_seconds} seconds...")
                        time.sleep(delay_seconds)
                    else:
                        logger.error(f"All {max_retries} attempts failed")
                        raise
        return wrapper
    return decorator

def validate_and_handle_errors(df, validation_rules, error_threshold=0.05):
    """
    Validate DataFrame and handle errors based on threshold
    
    Args:
        df: Input DataFrame
        validation_rules: Dictionary of column_name: validation_function
        error_threshold: Maximum acceptable error rate (default 5%)
    
    Returns:
        Tuple of (valid_df, invalid_df, error_rate)
    """
    logger = logging.getLogger("validate_and_handle_errors")
    
    # Apply all validation rules
    for column, rule in validation_rules.items():
        df = df.withColumn(f"{column}_valid", rule(df[column]))
    
    # Calculate error rate
    total_count = df.count()
    
    # Count invalid records (any validation failed)
    validation_cols = [f"{col}_valid" for col in validation_rules.keys()]
    invalid_count = df.filter(
        ~reduce(lambda a, b: a & b, [df[col] for col in validation_cols])
    ).count()
    
    error_rate = invalid_count / total_count if total_count > 0 else 0
    
    logger.info(f"Validation complete: {invalid_count}/{total_count} invalid ({error_rate:.2%})")
    
    if error_rate > error_threshold:
        logger.error(f"Error rate {error_rate:.2%} exceeds threshold {error_threshold:.2%}")
        raise ValueError(f"Data quality check failed: error rate too high")
    
    # Split into valid and invalid DataFrames
    valid_df = df.filter(
        reduce(lambda a, b: a & b, [df[col] for col in validation_cols])
    )
    invalid_df = df.filter(
        ~reduce(lambda a, b: a & b, [df[col] for col in validation_cols])
    )
    
    return valid_df, invalid_df, error_rate