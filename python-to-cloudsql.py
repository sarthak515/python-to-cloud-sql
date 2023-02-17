from google.cloud.sql.connector import Connector, IPTypes
import sqlalchemy
from itertools import zip_longest
import csv

# Python Connector database creator function
def getconn():
    with Connector() as connector:
        conn = connector.connect(
            "mlconsole-poc:us-central1-b:gems-postgres", # Cloud SQL Instance Connection Name
            "pg8000",
            user="postgres",
            password="India@123",
            db="sarthak",
            #ip_type=IPTypes.PUBLIC # IPTypes.PRIVATE for private IP
        )
    return conn

# create SQLAlchemy connection pool
pool = sqlalchemy.create_engine(
    "postgresql+pg8000://",
    creator=getconn,
)

# interact with Cloud SQL database using connection pool
with pool.connect() as db_conn:
    db_conn.execute(
    sqlalchemy.text( "CREATE TABLE  persistent "

      "( id SERIAL NOT NULL, name VARCHAR(255) NOT NULL, "

       "surname VARCHAR(255) NOT NULL, rating FLOAT NOT NULL, "

      "PRIMARY KEY (id));" ))

      
      


      

    # query database
    tables=db_conn.execute(sqlalchemy.text("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")).fetchall()
    indexes=db_conn.execute(sqlalchemy.text("SELECT indexname FROM pg_indexes WHERE schemaname = 'public' ORDER BY indexname")).fetchall()
    views=db_conn.execute(sqlalchemy.text("SELECT table_name FROM information_schema.views WHERE table_schema NOT IN ('information_schema', 'pg_catalog') ")).fetchall()
    primary_keys=db_conn.execute(sqlalchemy.text("SELECT conname AS primary_key FROM   pg_constraint WHERE  contype = 'p' AND    connamespace = 'public'::regnamespace  ORDER  BY conrelid::regclass::text, contype DESC;")).fetchall()
    functions=db_conn.execute(sqlalchemy.text("SELECT routine_name FROM information_schema.routines WHERE routine_type = 'FUNCTION' AND routine_schema = 'public'")).fetchall()
    proc=db_conn.execute(sqlalchemy.text("SELECT proname FROM pg_proc WHERE pronamespace=(SELECT oid FROM pg_namespace WHERE nspname='public')")).fetchall()
    const=db_conn.execute(sqlalchemy.text("SELECT constraint_name FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE table_schema='public';")).fetchall()
    triggers=db_conn.execute(sqlalchemy.text("SELECT  trigger_name FROM information_schema.triggers")).fetchall()
    tables1=[row[0] for row in tables]
    views1=[row[0] for row in views]
    indexes1=[row[0] for row in indexes]
    pri1=[row[0] for row in primary_keys]
    fun=[row[0] for row in functions]
    proc=[row[0] for row in proc]
    const1=[row[0] for row in const]
    tri=[row[0] for row in triggers]

    print(tables1)
    print(views1)
    print(indexes1)
    print(pri1)
    print(fun)
    print(proc)
    print(const1)
    print(tri)

    fields = ('Tables', 'Views', 'Indexes','PrimaryKeys','functions','Procedures','Constraints','Triggers')
    d = [ tables1,

         views1,

         indexes1,

         pri1,fun,proc,const1,tri]

    export_data = zip_longest(*d, fillvalue = '')



    with open('values.csv', 'w', encoding="ISO-8859-1", newline='') as myfile:
        wr = csv.writer(myfile)
        wr.writerow(fields)
        wr.writerows(export_data)
        myfile.close()

