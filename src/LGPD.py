from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Date, DateTime, insert, text
from datetime import datetime
import time
from functools import wraps
from secret import dbuser, dbpassword, dbhost

def medir_tempo(func):
    """Decorator que mede o tempo de execução de uma função."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        inicio = time.perf_counter()  # tempo inicial (mais preciso que time.time)
        resultado = func(*args, **kwargs)
        fim = time.perf_counter()     # tempo final
        duracao = fim - inicio
        print(f"⏱ Função '{func.__name__}' executada em {duracao:.6f} segundos.")
        return resultado
    return wrapper

engine = create_engine(f'postgresql+psycopg2://{dbuser}:{dbpassword}@{dbhost}/smith', echo=False)
metadata = MetaData()

usuarios = Table(
    'usuarios', metadata,
    Column('id', Integer, primary_key=True),
    Column('nome', String(50), nullable=False, index=True),
    Column('cpf', String(14), nullable=False),
    Column('email', String(100), nullable=False, unique=True),
    Column('telefone', String(20), nullable=False),
    Column('data_nascimento', Date, nullable=False),
    Column('created_on', DateTime(), default=datetime.now),
    Column('updated_on', DateTime(), default=datetime.now, onupdate=datetime.now)
)

metadata.create_all(engine)

@medir_tempo
def LGPD(row):
    print(type(row))
    newrow = []
    for key in row._mapping:
        match key:
            case 'nome':
                nome: list[str] = row._mapping[key].split(" ")
                nome[0] = nome[0][:1]+("*" * len(nome[0][1:]))
                newrow.append(" ".join(nome)) # PQ ASSIM? QUAL O SENTIDO DE CONCATENAR UMA LISTA ASSIM?!
            case 'cpf':
                cpf: str = row._mapping[key]
                cpf_chars = list(cpf[:3]+("*" * len(cpf[3:])))
                cpf_chars[3], cpf_chars[7] = "."*2
                cpf_chars[11] = "-"
                newrow.append("".join(cpf_chars))
            case 'email':
                email: list[str] = row._mapping[key].split("@")
                email[0] = email[0][:1]+("*" * len(email[0][1:]))
                newrow.append("@".join(email))
            case 'telefone':
                telefone: str = row._mapping[key][-4:]
                newrow.append(telefone)
            case _:
                newrow.append(row._mapping[key])
    return tuple(newrow)

users = []
with engine.connect() as conn:
    result = conn.execute(text("SELECT * FROM atividade2.usuarios LIMIT 5;"))
    for row in result:
        row = LGPD(row)
        users.append(row)

for user in users:
    print(user)
