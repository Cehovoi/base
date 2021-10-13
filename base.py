from sqlalchemy import create_engine, Integer, Column, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database


url = "mysql+mysqlconnector://root:@localhost/test_1"
engine = create_engine(url, echo=True)

if not database_exists(engine.url):
    create_database(engine.url)
else:
    print('_' * 30, '\n', 'db already created', '\n', '_' * 30)
    engine.connect()


Base = declarative_base()


class Accrual(Base):
    __tablename__ = 'accrual'
    id = Column(Integer, primary_key=True)
    data = Column(String(50))  # other sql base need length String(50)
    month = Column(Integer)

    def __init__(self, data, month):
        self.data = data
        self.month = month

    def __repr__(self):
        return "<Accrual('%s','%s', )>" % (self.data, self.month)


class Payment(Base):
    __tablename__ = 'payment'
    id = Column(Integer, primary_key=True)
    data = Column(String(50))  # other sql base need length String(50)
    month = Column(Integer)

    def __init__(self, data, month):
        self.data = data
        self.month = month

    def __repr__(self):
        return "<Payment('%s','%s', )>" % (self.data, self.month)


Session = sessionmaker(bind=engine)

Session.configure(bind=engine)

session = Session()

Base.metadata.create_all(bind=engine)


def single(month, year='2021'):
    session.add(Payment(year, month))
    session.commit()
    return


def filler(order=(1, 13, 1), year='2021', flag=1):
    if flag == 1:
        p, a = Payment, Accrual
    else:
        a, p = Payment, Accrual
    for month in range(order[0], order[1], order[2]):
        session.add(p(year, month))
        session.commit()
        if month % 2 == 0:
            session.add(a(year, month))
            session.commit()


def eraser():
    a = session.query(Accrual).all()
    p = session.query(Payment).all()
    cyc(a)
    cyc(p)
    return ('db erase')


def cyc(data):
    for i in data:
        session.delete(i)
        session.commit()


def shower():
    print('ACCRUAL - ', session.query(Accrual).all(), '\n',
          'PAYMENTS - ', session.query(Payment).all())


def printer(answer, lost):
    for conformity in answer:
        print('%s, this debt has been paid off %s' % (conformity[0],
                                                      conformity[1]))
    print('Payments not found debt %s' % lost)


comparator = lambda x, y, z: x.data == y.data and x.month == y.month if not z else x.month > y.month or x.data > y.data

generator = lambda array: ((x, y) for x, y in enumerate(array))


def collector():
    answer = []
    lost = []
    debts = sorted(session.query(Accrual).all(), key=lambda x: (x.data, x.month)) 
    pays = sorted(session.query(Payment).all(), key=lambda x: (x.data, x.month))
    i_pays = generator(pays)  # next(i_pays) have 2 fields - item and element
    i_debts = generator(debts)
    flag = 0  # need when all matches are exhausted
    while True:
        try:
            pay = next(i_pays)
            debt = next(i_debts)
            while pay[1].data < debt[1].data:
                lost += [pay[1]]
                pays[pay[0]] = None
                pay = next(i_pays)
            while not comparator(pay[1], debt[1], flag):
                if pay[1].data > debt[1].data and not flag:
                    debt = next(i_debts)
                if pay[1].month < debt[1].month:
                    pay = next(i_pays)
                else:
                    debt = next(i_debts)
            answer += [(debt[1], pay[1])]
            debts[debt[0]], pays[pay[0]] = None, None
        except(StopIteration):
            if pays and debts:
                pays = list(filter(None.__ne__, pays))
                debts = list(filter(None.__ne__, debts))
                flag += 1
                i_pays = generator(pays)
                i_debts = generator(debts)
            else:
                break
    return printer(answer, lost)


if __name__ == '__main__':
    collector()