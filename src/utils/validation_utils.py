# Databricks notebook source
"""
Data validation utilities
"""

from pyspark.sql.functions import col

def validate_coordinates(latitude, longitude):
    """Validate geographic coordinates"""
    return (latitude.between(-90, 90)) & (longitude.between(-180, 180))

def validate_fuel_level(fuel_level):
    """Validate fuel level percentage"""
    return fuel_level.between(0, 100)

def validate_speed(speed):
    """Validate ship speed (knots)"""
    return speed.between(0, 50)  # Max container ship speed ~25 knots

def validate_temperature(temp):
    """Validate container temperature"""
    return temp.between(-30, 50)  # Celsius

def validate_manifest_data(df):
    """
    Comprehensive manifest data validation
    
    Returns: DataFrame with validation flags
    """
    return (
        df
        .withColumn("is_valid_weight", col("cargo_weight_kg") > 0)
        .withColumn("is_valid_value", col("cargo_value_usd") > 0)
        .withColumn("is_valid_dates", 
            col("estimated_arrival_date") > col("departure_date"))
    )