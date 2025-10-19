# Databricks notebook source
# MAGIC %sql
# MAGIC -- Create catalog & schema (idempotent)
# MAGIC CREATE CATALOG IF NOT EXISTS sports;
# MAGIC CREATE SCHEMA  IF NOT EXISTS sports.cricket_demo;
# MAGIC
# MAGIC -- Create Volumes (raw landing + system/checkpoints)
# MAGIC CREATE VOLUME IF NOT EXISTS sports.cricket_demo.cricket_landing COMMENT 'Raw streaming JSON landing zone';
# MAGIC CREATE VOLUME IF NOT EXISTS sports.cricket_demo.cricket_system  COMMENT 'SchemaLocation & checkpoints for Auto Loader/DLT';
# MAGIC
