import psycopg2

import csv

from itertools import zip_longest

# DB_NAME = "pfmegrnargs"

# DB_USER = "reader"

# DB_PASS = "NWDMCE5xdipIjRrp"

# DB_HOST = "hh-pgsql-public.ebi.ac.uk"

# DB_PORT = "5432"



DB_NAME = "ecom1676218646118swnxhypumcaebyuk"

DB_USER = "qeticwandknjknkmmitjgsvi@psql-mock-database-cloud"

DB_PASS = "myxhhzkpojqscwchurbuedew"

DB_HOST = "psql-mock-database-cloud.postgres.database.azure.com"

DB_PORT = "5432"

connection = psycopg2.connect(database=DB_NAME,

                            user=DB_USER,

                            password=DB_PASS,

                            host=DB_HOST,

                            port=DB_PORT)

print("Database connected successfully")

cursor=connection.cursor()

cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")

tables=[row[0] for row in cursor.fetchall()]

cursor.execute("SELECT table_name FROM information_schema.views WHERE table_schema NOT IN ('information_schema', 'pg_catalog') ")

views=[row[0] for row in cursor.fetchall()]

cursor.execute("SELECT proname FROM pg_proc WHERE pronamespace=(SELECT oid FROM pg_namespace WHERE nspname='public')")

procedures=[row[0] for row in cursor.fetchall()]

cursor.execute("SELECT indexname FROM pg_indexes WHERE schemaname = 'public' ORDER BY indexname")

indexes=[row[0] for row in cursor.fetchall()]

cursor.execute("SELECT conname AS primary_key FROM   pg_constraint WHERE  contype = 'p' AND    connamespace = 'public'::regnamespace  ORDER  BY conrelid::regclass::text, contype DESC;")

primarykeys=[row[0] for row in cursor.fetchall()]

cursor.execute("SELECT conname AS foreign_key FROM   pg_constraint  WHERE  contype = 'f' AND    connamespace = 'public'::regnamespace   ORDER  BY conrelid::regclass::text, contype DESC")

foreignkeys=[row[0] for row in cursor.fetchall()]

print("Tables",tables)

print("Views",views)

print("Procedure",procedures)

print("Primary Keys",primarykeys)

print("Foreign Keys",foreignkeys)

fields = ('Tables', 'Views', 'Procedures', 'Indexes','PrimaryKeys','ForeignKeys')

   

# data rows of csv file



d = [ tables,

         views,

         procedures,

         indexes,

         primarykeys,

         foreignkeys]

export_data = zip_longest(*d, fillvalue = '')



with open('values.csv', 'w', encoding="ISO-8859-1", newline='') as myfile:



    wr = csv.writer(myfile)



    wr.writerow(fields)



    wr.writerows(export_data)



    myfile.close()

cursor.close()

connection.close()