from pynamodb.models import Model
from pynamodb.attributes import (
    UnicodeAttribute, UnicodeSetAttribute, Attribute, LegacyBooleanAttribute, BooleanAttribute, NumberAttribute, BinaryAttribute,MapAttribute, ListAttribute)
from pynamodb.indexes import GlobalSecondaryIndex, AllProjection
import json
from six import *
from pynamodb.models import Model
from pynamodb.indexes import GlobalSecondaryIndex, AllProjection
from pynamodb.attributes import NumberAttribute, UnicodeAttribute, MapAttribute, MapAttributeMeta, AttributeContainer
from pynamodb.constants import (
    STRING, STRING_SHORT, NUMBER, ATTR_TYPE_MAP
)
from pynamodb.expressions.operand import Path
import copy


class MyModel(Model):
    class Meta:
        table_name = "anand-test-consumed"
        host="dynamodb-server.devbox.lyft.net:8000"
    id = UnicodeAttribute(hash_key=True)
    name = UnicodeAttribute(null=True)

if MyModel.exists():
    MyModel.delete_table()
MyModel.create_table(read_capacity_units=2, write_capacity_units=2)

for i in range(1,100):
    m = MyModel(i)
    m.name = str(i)
    m.save()


for x in MyModel.scan(limit=10):
    print (x)