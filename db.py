import psycopg2

def database_connection():
    return psycopg2.connect("dbname=lfm")
