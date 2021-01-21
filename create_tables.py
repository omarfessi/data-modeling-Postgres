#import the needed libraries:
from sql_queries import *
import psycopg2 # python driver to set connexion to Postgres
def create_database():
	#connect to the default database :
	conn=psycopg2.connect("host=127.0.0.1 dbname=postgres user=postgres password=admin" )
	conn.set_session(autocommit=True)
	cur=conn.cursor()

	#drop sparkifydb if it exists 
	cur.execute("DROP DATABASE IF EXISTS sparkifydb")
	#create sparkifydb where to host all tables 
	cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

	#close the connection to the default database
	conn.close()

	#connect to the sparkifydb database
	conn=psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=admin" )
	cur=conn.cursor()

	return cur, conn


def drop_tables(cur, conn):
	for query in drop_table_queries:
		cur.execute(query)
		conn.commit()
def create_tables(cur, conn):
	for query in create_table_queries:
		cur.execute(query)
		conn.commit()



def main():
	cur, conn = create_database()
	drop_tables(cur, conn)
	create_tables(cur, conn)

	conn.close()

if __name__=="__main__":
	main()



