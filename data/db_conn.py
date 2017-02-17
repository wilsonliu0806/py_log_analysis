import sqlalchemy
from sqlalchemy import create_engine,MetaData
from sqlalchemy.orm import sessionmaker


engine = create_engine('mysql+mysqldb://root:root@localhost/squidlog')
metadata = MetaData(engine)
Session = sessionmaker(bind = engine)


