#!/usr/bin/env python

import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv
load_dotenv()


def db_connection():
  conn = psycopg2.connect(dbname=os.getenv('DB_DATABASE'), user=os.getenv('DB_USER'), password=os.getenv('DB_PASSWORD'),host=os.getenv('DB_HOST'), port=os.getenv('DB_PORT'))
  return conn

def insert_alert(alert):
  sql = "INSERT INTO alerts (symbol, amount, quote_volume, price_change_percent, created_at) VALUES(%s, %s, %s, %s, %s::timestamp)"
  conn = None
  try:
      conn = db_connection()
      cur = conn.cursor()
      cur.execute(sql, alert)
      conn.commit()
      cur.close()
  except (Exception, psycopg2.DatabaseError) as error:
      print(error)
  finally:
      if conn is not None:
          conn.close()

def create_tables():
  """ create tables in the PostgreSQL database"""
  commands = (
    """
    CREATE TABLE teste (
      teste_id SERIAL PRIMARY KEY,
      teste_name VARCHAR(255) NOT NULL
    )
    """,
    """ CREATE TABLE IF NOT EXISTS alerts (
          alert_id SERIAL PRIMARY KEY,
          symbol VARCHAR(255) NOT NULL,
          amount DECIMAL(20,2) NOT NULL,
          quote_volume DECIMAL(20,2) NOT NULL,
          price_change_percent DECIMAL(5,2) NOT NULL,
          created_at TIMESTAMP NOT NULL
        )
    """
  )
  conn = None
  try:
    # Create database
    conn = psycopg2.connect(dbname='postgres', user=os.getenv('DB_USER'), password=os.getenv('DB_PASSWORD'), host=os.getenv('DB_HOST'), port=os.getenv('DB_PORT'))
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()

    cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{os.getenv('DB_DATABASE')}'")

    exists = cursor.fetchone()
    if not exists:
      cursor.execute(f"CREATE DATABASE {os.getenv('DB_DATABASE')}")
  
    # Create tables if not exists
    conn = db_connection()
    cur = conn.cursor()

    for command in commands:
      cur.execute(command)

    conn.commit()
    cur.close()
  except (Exception, psycopg2.DatabaseError) as error:
      print(error)
  finally:
    if conn is not None:
      conn.close()
