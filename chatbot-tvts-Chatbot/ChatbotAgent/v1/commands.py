from flask import Flask
from flask.cli import AppGroup
from models import init_database

database_cli = AppGroup('database')

@database_cli.command('init')
def init_data():
    print("init database")
    init_database()
