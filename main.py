import configparser
import json
import sqlalchemy as sq
from sqlalchemy.orm import declarative_base, relationship, sessionmaker


Base = declarative_base()


class Publisher(Base):
	__tablename__ = "publisher"

	id = sq.Column(sq.Integer, primary_key=True)
	name = sq.Column(sq.String(length=50), unique=True)
	

class Book(Base):
	__tablename__ = "book"

	id = sq.Column(sq.Integer, primary_key=True)
	title = sq.Column(sq.String(length=100), unique=True)
	id_publisher = sq.Column(sq.Integer, sq.ForeignKey("publisher.id"), nullable=False)

	publisher = relationship(Publisher, backref="books")


class Shop(Base):
	__tablename__ = "shop"

	id = sq.Column(sq.Integer, primary_key=True)
	name = sq.Column(sq.String(length=50), unique=True)


class Stock(Base):
	__tablename__ = "stock"

	id = sq.Column(sq.Integer, primary_key=True)
	id_book = sq.Column(sq.Integer, sq.ForeignKey("book.id"), nullable=False)
	id_shop = sq.Column(sq.Integer, sq.ForeignKey("shop.id"), nullable=False)
	count = sq.Column(sq.Integer, nullable=False)

	book = relationship(Book, backref="stocks")
	shop = relationship(Shop, backref="stocks")


class Sale(Base):
	__tablename__ = "sale"

	id = sq.Column(sq.Integer, primary_key=True)
	price = sq.Column(sq.Float(precision=2), nullable=False)
	date_sale = sq.Column(sq.DateTime, nullable=False)
	id_stock = sq.Column(sq.Integer, sq.ForeignKey("stock.id"), nullable=False)
	count = sq.Column(sq.Integer, nullable=False)

	stock = relationship(Stock, backref="sales")


def create_tables(engine):
	Base.metadata.drop_all(engine)
	Base.metadata.create_all(engine)


def insert_from_json(json_file, session):
	with open(json_file) as file:
		data = json.load(file)

	for line in data:
		model = {
			'book': Book,
			'shop': Shop,
			'publisher': Publisher,
			'stock': Stock,
			'sale': Sale
		}[line['model']]

		session.add(model(id=line['pk'], **line['fields']))
	session.commit()


def get_shops(query, session): 
	subq = session.query( 
		Book.title, Shop.name, Sale.price, Sale.date_sale 
	).select_from(Shop).\
	join(Stock).\
	join(Book).\
	join(Publisher).\
	join(Sale)

	if query.isdigit(): 
		q = subq.filter(Publisher.id == query).all()
	else:
		q = subq.filter(Publisher.name == query).all() 
	
	for title, name, price, date_sale in q: 
		print(f"{title: <40} | {name: <10} | {price: <8} | {date_sale.strftime('%d-%m-%Y')}") 


if __name__ == '__main__':
	config = configparser.ConfigParser()
	config.read('settings.ini')

	db = config['DB']['db']
	login = config['DB']['login']
	password = config['DB']['password']
	name = config['DB']['name']
	input_file = config['JSON']['filename']

	DSN = f"{db}://{login}:{password}@localhost:5432/{name}"
	engine = sq.create_engine(DSN)
	create_tables(engine)

	Session = sessionmaker(bind=engine)
	session = Session()

	insert_from_json(input_file, session)
	query = input("Введите имя или id издателя: ")
	get_shops(query, session)


