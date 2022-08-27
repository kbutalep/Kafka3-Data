from kafka import KafkaConsumer, TopicPartition
from json import loads
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
#import config



user = 'kendra'
pwd = 'kendra123'
host = 'Zipcoders-MacBook-Pro-45.local'
dbname = 'zipbank'

engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=host, db=dbname, user=user, pw=pwd))
Base = declarative_base()

class Transaction(Base):
    __tablename__ = 'transactions'
    id = Column(Integer, primary_key=True, autoincrement=True)
    custid = Column(Integer)
    type = Column(String(255), nullable=False)
    date = Column(Integer)
    amt = Column(Integer)

    def __init__(self, custid, type, date, amt):
        self.custid = custid
        self.type = type
        self.date = date
        self.amt = amt

class XactionConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer('bank-customer-events',
            bootstrap_servers=['localhost:9092'],
            # auto_offset_reset='earliest',
            value_deserializer=lambda m: loads(m.decode('ascii')))
        ## These are two python dictionarys
        # Ledger is the one where all the transaction get posted
        self.ledger = {}
        # custBalances is the one where the current balance of each customer
        # account is kept.
        self.custBalances = {}
        # THE PROBLEM is every time we re-run the Consumer, ALL our customer
        # data gets lost!
        # add a way to connect to your database here.
        self.engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=host, db=dbname, user=user, pw=pwd))

        self.Session = sessionmaker
        self.session = self.Session(self.engine)
        #Go back to the readme.

    def handleMessages(self):
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            self.ledger[message['custid']] = message
            # add message to the transaction table in your SQL using SQLalchemy
            if message['custid'] not in self.custBalances:
                self.custBalances[message['custid']] = 0
            if message['type'] == 'dep':
                self.custBalances[message['custid']] += message['amt']
            else:
                self.custBalances[message['custid']] -= message['amt']
            print(self.custBalances)
            xaction = Transaction(message['custid'], message['type'], message['date'], message['amt'])
            Session = sessionmaker(bind=engine)
            session = Session()
            session.add(xaction)
            session.commit()

if __name__ == "__main__":
    c = XactionConsumer()
    c.handleMessages()