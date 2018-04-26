import pandas as pd
import MySQLdb, time 
from sqlalchemy import create_engine
import  sqlalchemy.exc
import memcache
from sqlalchemy import MetaData
from sqlalchemy import Table

mc = memcache.Client(['127.0.0.1:11211'],debug=0)


#c = cfg.config()

class dbInstance:
       
  def __init__(self,host):
    """
    - Bundling data of database instance.

    :param host: Instance hostname.
    :type host: str

    """      
    sc = mc.get('server_config')
    self.name = sc['db']['name'] #: str: Database name
    self.host = sc['hosts'][host] #: str: Host name
    self.user = sc['db']['user'] #: str: Database username
    self.password = sc['db']['password'] #: str: Database password
      

def connect(host='local'):   
    """
    - Creating the ``MySQLdb.connect`` connetion for the database.

    Args:
        host (str): Hostname.

    Returns:
        Returns the ``MySQLdb.connect()`` as db and ``db.cursor()`` as cur 
        connection to the host.

    """ 
    dbc = dbInstance(host)

    while True:
        try:
            db = MySQLdb.connect(host = dbc.host, user = dbc.user, 
                passwd = dbc.password, db = dbc.name)
            cur = db.cursor()
            return db, cur
        except MySQLdb.OperationalError:
        # except IndexError:
            print '6.',
            time.sleep(2)


def write(query='', identifier='', last_insert=False, instance='local'):
    """
    - The process of writing to the database by a query statement.

    Args:
        query (str): Query statement.
        identifier (str): Identifier statement for the query.
        Last_insert (str): Select the last insert. Defaults to False.
        instance (str): Hostname. Defaults to local.

    Raises:
        KeyError: Error on writing to database.
        MySQLdb.IntegrityError: If duplicate entry detected.

    """ 
    db, cur = connect(instance)

    b=''
    try:
        retry = 0
        while True:
            try:
                a = cur.execute(query)

                b = ''
                if last_insert:
                    b = cur.execute('select last_insert_id()')
                    b = cur.fetchall()

                if a:
                    db.commit()
                    break
                else:

                    db.commit()
                    time.sleep(0.1)
                    break

            except IndexError:
                print '5.',

                if retry > 10:
                    break
                else:
                    retry += 1
                    time.sleep(2)
    except KeyError:
        print '>> Error: Writing to database', identifier
    except MySQLdb.IntegrityError:
        print '>> Warning: Duplicate entry detected', identifier
    db.close()
    return b

def read(query='', identifier='', instance='local'):
    """
    - The process of reading the output from the query statement.

    Args:
        query (str): Query statement.
        identifier (str): Identifier statement for the query.
        instance (str): Hostname. Defaults to local.

    Returns:
        Returns the query output and fetch by a ``cur.fetchall()``.

    Raises:
        ValueError: Returns None.
    """ 

    db, cur = connect(instance)
    a = ''
    
    try:
        a = cur.execute(query)
        a = None
        try:
            a = cur.fetchall()
            return a
        except ValueError:
            return None
    except MySQLdb.OperationalError:
        a =  None
    except KeyError:
        a = None

def df_engine(host='local'):
    """
    - Creating the engine connection for the database.

    Args:
        host (str): Hostname. Defaults to local.

    Returns:
        Returns the ``create_engine()`` connection to the host.


    """ 
    dbc = dbInstance(host)
    engine = create_engine('mysql+pymysql://'+dbc.user+':'
        +dbc.password+'@'+dbc.host+':3306/'+dbc.name)
    return engine

def df_write(dataframe,host='local'):
    """
    - The process of writing data frame data to a database.

    Args:
        dataframe (DataFrame): DataFrame data.
        host (str): Hostname. Defaults to local.

    Raises:
        IndexError: ``print`` Possible data type error.
        ValueError: ``print`` Value error detected.
        AttributeError: ``print`` Value error in data pass.

    """
    engine = df_engine(host)
    df = dataframe.data
    df = df.drop_duplicates(subset=None, keep='first',
     inplace=False)
    df = df.reset_index()
    df_list = str(df.values.tolist())[:-1][1:]
    df_list =df_list.replace("]",")").replace("[","(")
    df_header = str(list(df))[:-1][1:].replace("\'","")
    df_keys =[];
    for value in list(df):
        df_keys.append(value +" = VALUES("+value+")")
    df_keys = str(df_keys)[:-1][1:]
    df_keys =df_keys.replace("]",")")
    df_keys =df_keys.replace("[", "(").replace("\'", "")
    query = "insert into %s (%s) values %s" % (dataframe.name,
        df_header,df_list)
    query += " on DUPLICATE key update  %s " % (df_keys)
    try:
        write(query=query, 
            identifier='Insert dataFrame values')
    except IndexError:
        print "\n\n>> Error: Possible data type error"
    except ValueError:
        print ">> Value error detected"   
    except AttributeError:
        print ">> Value error in data pass"       














# # -*- coding: utf-8 -*-
# """Example Google style docstrings.

# This module demonstrates documentation as specified by the `Google Python
# Style Guide`_. Docstrings may extend over multiple lines. Sections are created
# with a section header and a colon followed by a block of indented text.

# Example:
#     Examples can be given using either the ``Example`` or ``Examples``
#     sections. Sections support any reStructuredText formatting, including
#     literal blocks::

#         $ python example_google.py

# Section breaks are created by resuming unindented text. Section breaks
# are also implicitly created anytime a new section starts.

# Attributes:
#     module_level_variable1 (int): Module level variables may be documented in
#         either the ``Attributes`` section of the module docstring, or in an
#         inline docstring immediately following the variable.

#         Either form is acceptable, but the two should not be mixed. Choose
#         one convention to document module level variables and be consistent
#         with it.

# Todo:
#     * For module TODOs
#     * You have to also use ``sphinx.ext.todo`` extension

# .. _Google Python Style Guide:
#    http://google.github.io/styleguide/pyguide.html

# """

# module_level_variable1 = 12345

# module_level_variable2 = 98765
# """int: Module level variable documented inline.

# The docstring may span multiple lines. The type may optionally be specified
# on the first line, separated by a colon.
# """


# def function_with_types_in_docstring(param1, param2):
#     """Example function with types documented in the docstring.

#     `PEP 484`_ type annotations are supported. If attribute, parameter, and
#     return types are annotated according to `PEP 484`_, they do not need to be
#     included in the docstring:

#     Args:
#         param1 (int): The first parameter.
#         param2 (str): The second parameter.

#     Returns:
#         bool: The return value. True for success, False otherwise.

#     .. _PEP 484:
#         https://www.python.org/dev/peps/pep-0484/

#     """


# # def function_with_pep484_type_annotations(param1: int, param2: str) -> bool:
# #     """Example function with PEP 484 type annotations.

# #     Args:
# #         param1: The first parameter.
# #         param2: The second parameter.

# #     Returns:
# #         The return value. True for success, False otherwise.

# #     """


# def module_level_function(param1, param2=None, *args, **kwargs):
#     """This is an example of a module level function.

#     Function parameters should be documented in the ``Args`` section. The name
#     of each parameter is required. The type and description of each parameter
#     is optional, but should be included if not obvious.

#     If \*args or \*\*kwargs are accepted,
#     they should be listed as ``*args`` and ``**kwargs``.

#     The format for a parameter is::

#         name (type): description
#             The description may span multiple lines. Following
#             lines should be indented. The "(type)" is optional.

#             Multiple paragraphs are supported in parameter
#             descriptions.

#     Args:
#         param1 (int): The first parameter.
#         param2 (:obj:`str`, optional): The second parameter. Defaults to None.
#             Second line of description should be indented.
#         *args: Variable length argument list.
#         **kwargs: Arbitrary keyword arguments.

#     Returns:
#         bool: True if successful, False otherwise.

#         The return type is optional and may be specified at the beginning of
#         the ``Returns`` section followed by a colon.

#         The ``Returns`` section may span multiple lines and paragraphs.
#         Following lines should be indented to match the first line.

#         The ``Returns`` section supports any reStructuredText formatting,
#         including literal blocks::

#             {
#                 'param1': param1,
#                 'param2': param2
#             }

#     Raises:
#         AttributeError: The ``Raises`` section is a list of all exceptions
#             that are relevant to the interface.
#         ValueError: If `param2` is equal to `param1`.

#     """
#     if param1 == param2:
#         raise ValueError('param1 may not be equal to param2')
#     return True


# def example_generator(n):
#     """Generators have a ``Yields`` section instead of a ``Returns`` section.

#     Args:
#         n (int): The upper limit of the range to generate, from 0 to `n` - 1.

#     Yields:
#         int: The next number in the range of 0 to `n` - 1.

#     Examples:
#         Examples should be written in doctest format, and should illustrate how
#         to use the function.

#         >>> print([i for i in example_generator(4)])
#         [0, 1, 2, 3]

#     """
#     for i in range(n):
#         yield i


# class ExampleError(Exception):
#     """Exceptions are documented in the same way as classes.

#     The __init__ method may be documented in either the class level
#     docstring, or as a docstring on the __init__ method itself.

#     Either form is acceptable, but the two should not be mixed. Choose one
#     convention to document the __init__ method and be consistent with it.

#     Note:
#         Do not include the `self` parameter in the ``Args`` section.

#     Args:
#         msg (str): Human readable string describing the exception.
#         code (:obj:`int`, optional): Error code.

#     Attributes:
#         msg (str): Human readable string describing the exception.
#         code (int): Exception error code.

#     """

#     def __init__(self, msg, code):
#         self.msg = msg
#         self.code = code


# class ExampleClass(object):
#     """The summary line for a class docstring should fit on one line.

#     If the class has public attributes, they may be documented here
#     in an ``Attributes`` section and follow the same formatting as a
#     function's ``Args`` section. Alternatively, attributes may be documented
#     inline with the attribute's declaration (see __init__ method below).

#     Properties created with the ``@property`` decorator should be documented
#     in the property's getter method.

#     Attributes:
#         attr1 (str): Description of `attr1`.
#         attr2 (:obj:`int`, optional): Description of `attr2`.

#     """

#     def __init__(self, param1, param2, param3):
#         """Example of docstring on the __init__ method.

#         The __init__ method may be documented in either the class level
#         docstring, or as a docstring on the __init__ method itself.

#         Either form is acceptable, but the two should not be mixed. Choose one
#         convention to document the __init__ method and be consistent with it.

#         Note:
#             Do not include the `self` parameter in the ``Args`` section.

#         Args:
#             param1 (str): Description of `param1`.
#             param2 (:obj:`int`, optional): Description of `param2`. Multiple
#                 lines are supported.
#             param3 (:obj:`list` of :obj:`str`): Description of `param3`.

#         """
#         self.attr1 = param1
#         self.attr2 = param2
#         self.attr3 = param3  #: Doc comment *inline* with attribute

#         #: list of str: Doc comment *before* attribute, with type specified
#         self.attr4 = ['attr4']

#         self.attr5 = None
#         """str: Docstring *after* attribute, with type specified."""

#     @property
#     def readonly_property(self):
#         """str: Properties should be documented in their getter method."""
#         return 'readonly_property'

#     @property
#     def readwrite_property(self):
#         """:obj:`list` of :obj:`str`: Properties with both a getter and setter
#         should only be documented in their getter method.

#         If the setter method contains notable behavior, it should be
#         mentioned here.
#         """
#         return ['readwrite_property']

#     @readwrite_property.setter
#     def readwrite_property(self, value):
#         value

#     def example_method(self, param1, param2):
#         """Class methods are similar to regular functions.

#         Note:
#             Do not include the `self` parameter in the ``Args`` section.

#         Args:
#             param1: The first parameter.
#             param2: The second parameter.

#         Returns:
#             True if successful, False otherwise.

#         """
#         return True

#     def __special__(self):
#         """By default special members with docstrings are not included.

#         Special members are any methods or attributes that start with and
#         end with a double underscore. Any special member with a docstring
#         will be included in the output, if
#         ``napoleon_include_special_with_doc`` is set to True.

#         This behavior can be enabled by changing the following setting in
#         Sphinx's conf.py::

#             napoleon_include_special_with_doc = True

#         """
#         pass

#     def __special_without_docstring__(self):
#         pass

#     def _private(self):
#         """By default private members are not included.

#         Private members are any methods or attributes that start with an
#         underscore and are *not* special. By default they are not included
#         in the output.

#         This behavior can be changed such that private members *are* included
#         by changing the following setting in Sphinx's conf.py::

#             napoleon_include_private_with_doc = True

#         """
#         pass

#     def _private_without_docstring(self):
#         pass