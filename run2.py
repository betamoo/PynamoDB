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



class MultiAttribute(Attribute):
    attr_type = "String"

    def __init__(self, type_set={}, *args, **kwargs):
        for t in type_set:
            if t.attr_name is None:

        self.attr_names = {self.attr_name + "." + ATTR_TYPE_MAP[x.attr_type] : x for x in type_set}
        pass

    def __get__(self, instance, owner):
        if instance:
             attr_name = instance._dynamo_to_python_attrs.get(self.attr_name, self.attr_name)
             return instance.attribute_values.get(attr_name, None)
        else:
             return self

    def __set__(self, instance, value):
        if instance:
            attr_name = instance._dynamo_to_python_attrs.get(self.attr_name, self.attr_name)
            instance.attribute_values[attr_name] = value


class MultiKeyMap1(object):
    def __init__(self):
        self.values = {}
        self.keymap = {}

    def add_keys(self, actual_key, keys):
        self.keymap[actual_key]=actual_key
        for key in keys:
            self.keymap[key] = actual_key

    def __getitem__(self, key):
        actual_key = self.keymap.get(key, None)
        if actual_key is None:
            return None
        return self.values.get(actual_key, None)

    def __setitem__(self, key, value):
        for x in self.values:
            self.values[x] = None
        actual_key = self.keymap.get(key, None)
        if actual_key is None:
            actual_key = key
            self.keymap[actual_key] = actual_key
        self.values[actual_key]=value

    def __get__(self, instance, owner):
        if instance:
            return self.values[self.values.keys()[0]]

    def serialize(self):
        keys = [x for x in self.values if self.values[x] is not None]
        return {self.keymap[keys[0]]: self.values[keys[0]]}


class MultiKeyMap(object):
    def __init__(self):
        self.key = None
        self.value = None
        self.keymap = {}

    def translate_key(self, key):
        return self.keymap.get(key, key)

    def add_keys(self, actual_key, keys):
        self.keymap[actual_key]=actual_key
        for key in keys:
            self.keymap[key] = actual_key

    def __getitem__(self, key):
        if self.key != self.translate_key(key):
            return None
        return self.value

    def __setitem__(self, key, value):
        self.key = self.translate_key(key)
        self.value = value

    # def __getattr__(self, item):
    #     if hasattr(self, "keymap") and self.keymap and item in self.keymap:
    #         return self.__getitem__(item)
    #     else:
    #         return super.__getattr__(self, item)
    #
    # def __setattr__(self, name, value):
    #     if hasattr(self, "keymap") and self.keymap and name in self.keymap:
    #         self.__setitem__(name, value)
    #     else:
    #         super.__setattr__(self, name, value)

    def __get__(self, instance, owner):
        print("here")
        if instance:
            return self.value
        else:
            return self

    def get_key_value(self):
        return (self.key, self.value)

    def get_value(self):
        return self.value

class EnumAttribute(Attribute):
    attr_type = "String"

    def __init__(self, type_set={}, *args, **kwargs):
        self.type_set = {ATTR_TYPE_MAP[x.attr_type]: x for x in type_set}
        self.multikey_map = MultiKeyMap()
        for t in type_set:
            self.multikey_map.add_keys(ATTR_TYPE_MAP[t.attr_type], [t.attr_type, t.__class__, t.__class__.__name__ ])
        Attribute.__init__(self, default=copy.deepcopy(self.multikey_map), *args, **kwargs)

    def default(self):
        return copy.deepcopy(self.multikey_map)

    def serialize(self, value):
        key, value = value.get_key_value()
        if value is None:
            return json.dumps(None)
        return json.dumps({key: self.type_set[key].serialize(value)})

    def deserialize(self, value):
        value=json.loads(value)
        ret = copy.deepcopy(self.multikey_map)
        if value is None:
            return ret
        key = value.keys()[0]
        value = value[key]

        ret[key] = self.type_set[key].deserialize(value)
        return ret

    def __get__(self, instance, owner):
         if instance:
             attr_name = instance._dynamo_to_python_attrs.get(self.attr_name, self.attr_name)
             return instance.attribute_values.get(attr_name, None)
         else:
             return self

    def __eq__(self, other):
        if other is None or isinstance(other, Attribute):  # handle object identity comparison
            return self is other
        key = other[0]
        value = other[1]
        ret = copy.deepcopy(self.multikey_map)
        ret[key]=value
        result = self.serialize(ret)
        print (result)
        return Path(self).__eq__(ret)


class MultiValue(MapAttribute):
    flag = BooleanAttribute(null=True)
    name = UnicodeAttribute(null=True)
    number = NumberAttribute(null=True)
    nameset = UnicodeSetAttribute(null=True)

class MyModel(Model):
    class Meta:
        table_name = "TestModel"
        host="http://localhost:8000"
    id = UnicodeAttribute(hash_key=True)
    #multi = MultiValue(null=True)
    multi1 = EnumAttribute(type_set={UnicodeAttribute(null=True), NumberAttribute(null=True)}, null=True)
    multi2 = EnumAttribute(type_set={BooleanAttribute(null=True), NumberAttribute(null=True)}, null=True)
    multi3 = EnumAttribute(type_set={BooleanAttribute(), UnicodeSetAttribute()}, null=True)

if MyModel.exists():
    MyModel.delete_table()
MyModel.create_table(read_capacity_units=2, write_capacity_units=2)

m1 = MyModel("id1")

#m1.multi={}
m1.multi1['S'] = "ahmed"
m1.save()

print (MyModel.dumps())


m2 = MyModel("id2")
m2.multi2['BOOL'] = True
m2.save()


m3 = MyModel("id3")
m3.multi3["UnicodeSetAttribute"] = {"hey", "there"}
m3.save()

m1 = MyModel.get("id1")
print(m1.multi1)
print(m1.multi1.get_value())

m1.multi1['N'] = 3
m1.save()

#m1.update({m1.multi1['S']:"yay"})

print(MyModel.multi1)
print(m1.multi1['N'])
print(m1.multi1['NumberAttribute'])
#print (m1.multi.default())
print (MyModel.dumps())


print([x.multi1.get_value() for x in MyModel.query("id1")])
print([x for x in MyModel.query("id1", filter_condition=MyModel.multi1==(NumberAttribute,3))])

exit()


m = MultiKeyMap()
m.add_keys('key1', ['key 1', '1', 'one'])
m['key1'] = 1
print (m['key1'], m['key 1'], m['one'])
m['key2'] = 2
print (m['key1'], m['key 1'], m['one'])
print (m['key2'])
m.add_keys('key2', ['2', 'two', 'key 2'])
print (m['key2'], m['key 2'], m['two'])



class EnumAttribute(MapAttribute):
    field_s = UnicodeAttribute(null=True)
    field_b = BinaryAttribute(null=True)
    field_n = NumberAttribute(null=True)


def MyModel(Model):
    class Meta:
        s=0

    id = UnicodeAttribute(hash_key=True)
    # EnumAttributes can't be hash key or range key
    enum_attr = EnumAttribute()


class TestAttribute(Attribute):
    attr_type = "Enum"

    @staticmethod
    def get_not_none(d):
        return d
        if not d:
            return None
        key = d.keys()[0]
        return d[key]

    def __init__(self, type_set = {}, *args, **kwargs):
        self.type_instance_map = {ATTR_TYPE_MAP[instance.__class__.attr_type]:instance for instance in type_set}
        #self.type_value = {ATTR_TYPE_MAP[instance.__class__.attr_type]:instance.default for instance in type_set}
        super(TestAttribute, self).__init__(*args, **kwargs)

    def __repr__(self):
        return str(self.type_instance_map)

    def __set__(self, instance, value):
        if instance:
            attr_name = instance._dynamo_to_python_attrs.get(self.attr_name, self.attr_name)
            instance.attribute_values[attr_name] = {ATTR_TYPE_MAP[value[0].attr_type]: value[1]}

    def __get__(self, instance, owner):
        if instance:
            attr_name = instance._dynamo_to_python_attrs.get(self.attr_name, self.attr_name)
            return instance.attribute_values.get(attr_name, None)
        else:
            return self

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
    abnormal=NumberAttribute(null=True)
    #enum_attr = TestAttribute(null=True, type_set={BinaryAttribute(), NumberAttribute(), UnicodeAttribute()})
    diff = MultiValue(null=True)


m1 = MyModel('2')
#m1.normal_attr="hey"
m1.abnormal=4
m1.diff=MultiValue(flag='flag')
m1.save()

m2 = MyModel('3')
m2.diff={}
m2.diff.name="name3"
m2.diff['number']=3
m2.diff.nameset={'na', 'me'}
m2.save()


m3 = MyModel('4')
m3.diff={}
m3.diff['name'] = 'name4'
m3.diff['number'] = 4
m3.save()

m0 = MyModel.get('2')
print(m0.diff)
print(m0.diff.name)
print (m0._get_json())
print (MyModel.dumps())
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