from pynamodb.models import Model
from pynamodb.attributes import (
    UnicodeAttribute, UnicodeSetAttribute, Attribute, LegacyBooleanAttribute, BooleanAttribute, NumberAttribute, BinaryAttribute,MapAttribute, ListAttribute)

from pynamodb.indexes import GlobalSecondaryIndex, AllProjection
import json

from six import *
from pynamodb.models import Model
from pynamodb.indexes import GlobalSecondaryIndex, AllProjection
from pynamodb.attributes import NumberAttribute, UnicodeAttribute, MapAttribute, MapAttributeMeta
from pynamodb.constants import (
    STRING, STRING_SHORT, NUMBER, ATTR_TYPE_MAP
)





class MyModel(Model):
    class Meta:
        table_name = "TestMsodel"
        host="http://localhost:8000"
    id = UnicodeAttribute(hash_key=True)
    mixed = UnicodeAttribute(null=True)

if MyModel.exists():
    MyModel.delete_table()
MyModel.create_table(read_capacity_units=2, write_capacity_units=2)


#MyModel.from_raw_data({"id":{"S":"2"}, "attributes": {"mixed": {"S": None}}}).save()

m1 = MyModel('1')
m1.mixed="sdads"
m1.save()
print (MyModel.dumps())
exit()


class MyModel2(Model):
    class Meta:
        table_name = "TestModel"
        host="http://localhost:8000"
    id = UnicodeAttribute(hash_key=True)
    mixed = UnicodeSetAttribute(attr_name="mixed")

print([x for x  in MyModel2.query('1')][0]._get_json())

exit()
class EnumMapAttributeMeta(MapAttributeMeta):
    def __init__(cls, name, bases, attrs):
        super(EnumMapAttributeMeta, cls).__init__(name, bases, attrs)
        print ((cls, name, bases, attrs))

    def __new__(cls, name, bases, dct):
        print (cls, name, bases, dct)
        return type.__new__(cls, name, bases, dct)


#@add_metaclass(EnumMapAttributeMeta)
class EnumMapAttribute(Attribute):
    def __init__(self, type_set = {}, *args, **kwargs):
        self.inner = type("_inner", (MapAttribute,),  {ATTR_TYPE_MAP[instance.__class__.attr_type]:instance for instance in type_set})
    def __get__(self, instance, owner):
        if instance:
            return self.inner
        else:
            return self

    def __set__(self, instance, value):
        return self.inner.__set__(instance, value)

    def __getitem__(self, item):
        return self.inner.__getitem__(self, item).__get__()

    def __setitem__(self, item, value):
        return self.inner.__setitem__(self, item, value)

#e1=EnumMapAttribute("a","b")
class MixedAttribute(MapAttribute):
    b = BinaryAttribute(null=True)
    n = NumberAttribute(null=True)
    s = UnicodeAttribute(null=True)




class MyModel(Model):
    class Meta:
        table_name = "TestModel"
        host="http://localhost:8000"
    id = UnicodeAttribute(hash_key=True)
    mixed = EnumMapAttribute(null=True, type_set={BinaryAttribute(), NumberAttribute(), UnicodeAttribute()})

if MyModel.exists():
    MyModel.delete_table()
MyModel.create_table(read_capacity_units=2, write_capacity_units=2)



m1 = MyModel('1')
m1.mixed= {"B":b"sss", "N":3, "S":"ssss"}
m1.save()
print (m1.dumps())
exit()


class TestAttribute(Attribute):
    attr_type = "Enum"

    def __init__(self, type_set = {}, *args, **kwargs):
        self.type_instance_map = {ATTR_TYPE_MAP[instance.__class__.attr_type]:instance for instance in type_set}
        #self.type_value = {ATTR_TYPE_MAP[instance.__class__.attr_type]:instance.default for instance in type_set}
        super(TestAttribute, self).__init__(*args, **kwargs)

    def __set__(self, instance, value):
        if instance:
            attr_name = instance._dynamo_to_python_attrs.get(self.attr_name, self.attr_name)
            instance.attribute_values[attr_name] = value

    def __get__(self, instance, owner):
        return self
        if instance:
            attr_name = instance._dynamo_to_python_attrs.get(self.attr_name, self.attr_name)
            return instance.attribute_values.get(attr_name, None)
        else:
            return self

    def __getitem__(self, item):
        print('get', item)
        return self.type_instance_map[ATTR_TYPE_MAP[item.attr_type]]

    def __setitem__(self, item, value):
        print('set', item, value)
        self.type_instance_map[ATTR_TYPE_MAP[item.attr_type]] = value

    def __repr__(self):
        return str(self.type_instance_map)

    def get_value(self, value):
        if not value:
            return None
        return value[value.keys()[0]]

    def serialize(self, value):
        """
        This method should return a dynamodb compatible value
        """
        return self.type_instance_map[value.keys()[0]].serialize(value[value.keys()[0]])

    def deserialize(self, value):
        """
        Performs any needed deserialization on the value
        """
        return self.type_instance_map[value.keys()[0]].deserialize(value[value.keys()[0]])

#t = TestAttribute(type_set={BinaryAttribute(), NumberAttribute(), UnicodeAttribute()})


class MyModel(Model):
    class Meta:
        table_name = "TestModel"
        host="http://localhost:8000"
    id = UnicodeAttribute(hash_key=True)
    normal_attr = UnicodeAttribute(null=True)
    enum_attr = TestAttribute(null=True, type_set={BinaryAttribute(), NumberAttribute(), UnicodeAttribute()})
    map_attr = MapAttribute(of=BinaryAttribute)

if MyModel.exists():
    MyModel.delete_table()
MyModel.create_table(read_capacity_units=2, write_capacity_units=2)

m1 = MyModel('2')
m1.enum_attr[UnicodeAttribute] = '2'

print (m1.enum_attr[UnicodeAttribute])
m1.save()

exit()
class MetaMap(MapAttributeMeta):
    def __init__(cls, name, bases, attrs):
        super(MapAttributeMeta, cls).__init__(name, bases, attrs)

@add_metaclass(MetaMap)
class EnumAttribute(Attribute):
    attr_type = "Enum"

    def __init__(self, attr_list):
        self.attr_types = attr_list
        self.attr_store = MapAttribute(of=Attribute)

    def serialize(self, value):
        values = {x: value.get(x, None) for x in self.attr_list}

        if len(values) > 0:
            return json.dump(values[values.keys()[0]])

        return json.dump(None)

    def get_value(self, value):
        return

    def deserialize(self, value):

        return
class EnumAttr2(EnumAttribute):
    pass

class T1(Model):
    class Meta:
        table_name = "TestModel"
        host="http://localhost:8000"
    enum_attr1 = EnumAttribute(UnicodeAttribute, BooleanAttribute)
    enum_attr2 = None


class MultiValue(MapAttribute):
    flag = BooleanAttribute(null=True)
    name = UnicodeAttribute(null=True)

class TestModel(Model):
    class Meta:
        table_name = "TestModel"
        host="http://localhost:8000"
    id = UnicodeAttribute(hash_key=True)
    mixed_values = MultiValue(null=True)


if TestModel.exists():
    TestModel.delete_table()
TestModel.create_table(read_capacity_units=2, write_capacity_units=2)

t1 = TestModel('1')
t1.mixed_values= {'flag': False, 'name':'d'}
t1.save()

t2=TestModel.get('1')
print (t2.mixed_values.name)

print (TestModel.dumps())

exit()
class ViewIndex(GlobalSecondaryIndex):
    class Meta:
        read_capacity_units = 2
        write_capacity_units = 1
        projection = AllProjection()
    view = NumberAttribute(default=0, hash_key=True)

class TestModel(Model):
    class Meta:
        table_name = "TestModel"
        host="http://localhost:8000"
    forum = UnicodeAttribute(hash_key=True)
    thread = UnicodeAttribute(range_key=True)
    view = NumberAttribute(default=0)
    view_index = ViewIndex()
TestModel.create_table(read_capacity_units=2, write_capacity_units=2)

class EmailIndex(GlobalSecondaryIndex):
    """
    A global secondary index for email addresses
    """

    class Meta:
        #index_name = 'custom_didx_ddname'
        read_capacity_units = 2
        write_capacity_units = 1
        projection = AllProjection()

    attr2 = NumberAttribute(hash_key=True, default="")
    alt_numbers = NumberAttribute(range_key=True, attr_name='numbers')

class BaseModel(Model):
    class Meta:
        table_name = "BaseModddel"
        host = "http://localhost:8000"
    attr1 = UnicodeAttribute(hash_key=True)
    attr2 = NumberAttribute(null=True, default=0)
    attr3 = UnicodeAttribute(range_key=True)
    attr4 = UnicodeAttribute(null=True)
    email_index = EmailIndex()
BaseModel.create_table(read_capacity_units=2, write_capacity_units=2)