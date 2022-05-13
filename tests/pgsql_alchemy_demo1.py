from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from typing import List

from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy.orm import registry
from sqlalchemy.orm import relationship

mapper_registry = registry()

@dataclass
class User:
    id: int = field(init=False)
    name: str = None
    fullname: str = None
    nickname: str = None
    addresses: List[Address] = field(default_factory=list)

@dataclass
class Address:
    id: int = field(init=False)
    user_id: int = field(init=False)
    email_address: str = None

metadata_obj = MetaData()

user = Table(
    'user',
    metadata_obj,
    Column('id', Integer, primary_key=True),
    Column('name', String(50)),
    Column('fullname', String(50)),
    Column('nickname', String(12)),
)

address = Table(
    'address',
    metadata_obj,
    Column('id', Integer, primary_key=True),
    Column('user_id', Integer, ForeignKey('user.id')),
    Column('email_address', String(50)),
)

mapper_registry.map_imperatively(User, user, properties={
    'addresses': relationship(Address, backref='user', order_by=address.c.id),
})

mapper_registry.map_imperatively(Address, address)