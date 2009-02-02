#!/usr/bin/env python
# coding=utf-8

from fuse import Fuse, Stat
import fuse
import stat
from time import time
from subprocess import *
import os
import errno
import MySQLdb

import sys
import pdb


"""
So here's the general concept:

The root directory contains subdirs named for each table in the db.

Within each table, we look up the PRIMARY indexes (those have to be unique,
right?).  Ummm... for the first cut, we'll take them in order given by
Sequence (ideally we should be able to take them in all orders, see below).
This will be better with an example.

CREATE TABLE `testtab` (
	`key1` varchar(10) default NULL,
	`key2` varchar(7) default NULL,
        `data1` text default NULL,
        `data2` varchar(18) default NULL,
        PRIMARY KEY (`key1`, `key2`)
)

(N.B. Handling NULL keys and different types of keys (int, varchar) is
going to get tricky) The above should lead to this:


                              testtab
                             /       \
                            /         \
                        key1:foo  ...  key1:bar     (all the extant key1 vals)
                       /        \      |         \
                      /          |     |          \
              key2:baz ... key2:quux   key2:yuzz .. key2:zatz   (all extent key2)
             /       |      |      |
            /        |      |      |
           /         |      |      |
        data1      data2   data1   data2 ...

At the bottom level, the filenames are "data1" and "data2", i.e. all the
*names* of the non-key fields, and each such file contains the *value* for
that field in that row (note that each key2:... directory contains a single
row, assuming the keys have to be unique, i.e. primary).  Higher-level
entries are all directories, not files, and are named with the field-*name*
concatenated with a colon and then the *value*, and all extant values are
represented.

The other way might be to do /testtab/key1/foo/key2/baz/data1 (alternate
key-field names with values in the path), but that seems more complicated.
Doable once the other way is handled though.

Also, ideally all possible permutations of key-orderings should be
available, i.e. the top-level dir should have both key1: and key2: keys,
and /testtab/key1:X/ should have key2:Y entries while /testtab/key2:Y/
should have key1:X entries, and so on for more types of keys.  The
alternating path starts to look more tempting with this feature.

"""


def getDepth(path):
    """
    Return the depth of a given path, zero-based from root ('/')
    """
    if path == '/':
        return 0
    else:
        return path.count('/')

def getParts(path):
    """
    Return the slash-separated parts of a given path as a list
    """
    if path == '/':
        return [['/']]
    else:
        return path.split('/')

def escape_for_fs(string):
    x=string
    x=x.replace("%","%%")
    x=x.replace("/","%/")
    return x

def unescape_from_fs(string):
    x=string
    x=x.replace("%/","/")
    x=x.replace("%%","%")
    return x

def escape_for_sql(string):
    x=string
    x=x.replace("'","''")
    return x

def unescape_from_sql(string):
    return string.replace("''","'")


def make_criteria(elts):
    criteria=""
    for dirname in elts:
        (key, name)=dirname.split(':',1)
        key=escape_for_sql(unescape_from_fs(key))
        name=escape_for_sql(unescape_from_fs(name))
        criteria+="%s='%s' AND "%(key,name)
    criteria=criteria[:-5]  # Chop off the final AND.
    return criteria



class MyStat(Stat):
    def __init__(self):
        self.st_mode = stat.S_IFDIR | 0755
        self.st_ino = 0
        self.st_dev = 0
        self.st_nlink = 2
        self.st_uid = 0
        self.st_gid = 0
        self.st_size = 4096
        self.st_atime = 0
        self.st_mtime = 0
        self.st_ctime = 0


# decorator for debugging!
def debugfunc(f):
    def newf(*args, **kwargs):
        MySQLFUSE.dbg.write(">>entering function %s\n"%f.__name__)
        MySQLFUSE.dbg.flush()
        x=f(*args, **kwargs)
        MySQLFUSE.dbg.write("<<leaving function %s, returning %s\n"%
                            (f.__name__, str(x)))
        MySQLFUSE.dbg.flush()
        return x
    return newf

class MySQLFUSE(Fuse):

    dbg=open("DBG","w")

    def DBG(self,s):
        MySQLFUSE.dbg.write(s+"\n")
        MySQLFUSE.dbg.flush()

    @debugfunc
    def __init__(self, *args, **kw):
        Fuse.__init__(self, *args, **kw)
        # I assume that here I need to log into the db.
        print str(args)
        print str(kw)
        # Gonna have to read this from a config file oslt.
        self.connection=MySQLdb.Connection(host="XX",user="XX",
                                           db="XX", passwd="XX")
        self.cursor=self.connection.cursor()
        self.dcursor=self.connection.cursor(MySQLdb.cursors.DictCursor)
        # Build a data structure encoding things.  The indexes aren't
        # allowed to change during a session, only their contents.
        self.cursor.execute("SHOW TABLES")
        self.tables=map((lambda x:x[0]), self.cursor.fetchall())
        self.keys={}
        self.fields={}
        for table in self.tables:
            self.cursor.execute("SHOW INDEXES FROM %s WHERE Key_name='PRIMARY'"%table)
            ix=self.cursor.fetchall()
            self.keys[table]=map((lambda x:x[4]), ix)
            self.cursor.execute("SHOW COLUMNS FROM %s"%table)
            fl=self.cursor.fetchall()
            fl=map((lambda x:x[0]), fl)
            # remove the key fields?
            for k in self.keys[table]:
                fl.remove(k)
            self.fields[table]=fl
        self.DBG(str(self.keys))

    def is_root(self, path=None, pathelts=None):
        if pathelts is None:
            pathelts=getParts(path)[1:]
        if path=='/' or len(pathelts)==0:
            return True
        else:
            return False

    def is_directory(self, path=None, pathelts=None):
        # Something is a directory iff it isn't deep enough to be a file.
        # In order to be a file, the depth of the path must equal
        # 1(for the table) plus the number of keys for that table, plus 1
        # more to get down to the file level.
        if not pathelts:
            pathelts=getParts(path)[1:]
        if self.is_root(pathelts=pathelts):
            # Root dir.  Special case.
            return True
        table=pathelts[0]
        try:
            keys=self.keys[table]
        except KeyError:
            # We don't have that table!  Fake it.
            self.DBG("Trying to look up keys for non-existent table %s!"%table)
            return False
        if len(pathelts) < len(keys)+2:
            return True
        else:
            return False

    @debugfunc
    def getattr(self, path):
        self.DBG("inside getattr: %s"%path)
        st = MyStat()
        pe = getParts(path)[1:]

        if self.is_root(pathelts=pe):
            return st
        table=escape_for_sql(unescape_from_fs(pe[0]))
        if not table in self.tables:
            return -fuse.ENOENT
        if len(pe)==1:      # It's a table
            return st
        query="SELECT COUNT(*) from %s "%table
        if self.is_directory(path):
            criteria=make_criteria(pe[1:])
        else:
            criteria=make_criteria(pe[1:-1])
        if criteria:
            query+="WHERE %s"%criteria
        self.DBG(query)
        self.cursor.execute(query)
        if self.cursor.fetchone()[0]<1:
            return -fuse.ENOENT
        else:
            if self.is_directory(path):
                return st
            # Otherwise, it's a "file", i.e. an actual field.
            st.st_mode = stat.S_IFREG | 0666
            st.st_nlink = 1
            # Oh what the hell.  Yes, we query EACH TIME.
            query="SELECT length(%s) FROM %s WHERE "\
                "%s"%(escape_for_sql(unescape_from_fs(pe[-1])),
                      table, criteria)
            self.DBG(query)
            n=self.cursor.execute(query)
            if n<=0:
                return -fuse.ENOENT
            st.st_size = int(self.cursor.fetchone()[0])
        return st


    @debugfunc
    def readdir(self, path, offset):
        dirents=['.', '..']
        pe=path.split('/')[1:]
        if path=='/':
            # Populate root dir
            dirents.extend(self.tables)
        else:
            # find the next key.
            table=escape_for_sql(unescape_from_fs(pe[0]))
            d=getDepth(path)
            try:
                nextkey=self.keys[table][d-1]
            except KeyError:
                # ?? Not a table?
                self.DBG("This shouldn't happen.")
                yield ""
            except IndexError:
                # I think this means we're at the "file" level.
                # Just append the fields.
                dirents.extend(self.fields[table])
            else:
                criteria=make_criteria(pe[1:])
                query="SELECT DISTINCT %s FROM %s "%(nextkey, table)
                if criteria:
                    query+="WHERE %s"%criteria
                self.DBG(query)
                self.cursor.execute(query)
                dirents.extend([nextkey+':'+str(row[0]) 
                                for row in self.cursor.fetchall()])
        for r in dirents:
            yield fuse.Direntry(r)

    @debugfunc
    def mknod(self, path, mode, dev):
        pe=getParts(path)[1:]
        table=pe[0]
        keys=''
        seen=[]
        values=''
        if self.is_directory(path):
            elts=pe[1:]
        else:
            elts=pe[1:-1]
        for key in elts:
            (key, value)=key.split(':',1)
            value=escape_for_sql(unescape_from_fs(value))
            seen.append(key)
            keys+=key+","
            values+="'%s',"%value
        # Make sure we have all the required keys.
        for key in self.keys[table]:
            if key in seen:
                continue
            keys+=key+","
            values+="NULL,"
        # Chop off the comma
        keys=keys[:-1]
        values=values[:-1]
        query="REPLACE INTO %s (%s) VALUES (%s)"
        self.DBG(query)
        self.cursor.execute(query)
        return 0

    @debugfunc
    def unlink(self,path):
        # You can only remove a "directory" (a bottom-level one).
        # So this doesn't really do anything.
        return 0

    @debugfunc
    def write(self, path, buf, offset):
        buf=escape_for_sql(buf)
        pe=getParts(path)[1:]
        # We ought to be at the bottom, at the "file" level.
        if self.is_directory(path):
            # We shouldn't be writing here.  Just nod and smile.
            return len(buf)
        table=pe[0]
        criteria=make_criteria(pe[1:-1])
        field=escape_for_sql(unescape_from_fs(pe[-1]))
        data=escape_for_sql(buf)
        # Ignoring offset crap for now.
        query="UPDATE %s SET %s='%s' "%(table, field, data)
        if criteria:
            query+="WHERE %s"%criteria
        self.DBG(query)
        self.cursor.execute(query)
        return len(buf)

    @debugfunc
    def read(self, path, size, offset):
        pe=getParts(path)[1:]
        if self.is_directory(path):
            # We shouldn't be here.
            return ""
        table=pe[0]
        self.DBG("About to make criteria; pe="+str(pe))
        criteria=make_criteria(pe[1:-1])
        field=pe[-1]
        query="SELECT %s FROM %s "%(field,table)
        if criteria:
            query +="WHERE %s"%criteria
        self.DBG(query)
        self.cursor.execute(query)
        x=self.cursor.fetchone()
        self.DBG(str(x))
        data=x[0]
        # doesn't matter if we're on the wrong level of the tree.
        return data[offset:offset+size]


    @debugfunc
    def mkdir(self, path, mode, dev):
        # I think I did all this in mknod.
        self.mknod(path, mode, dev)
        return 0


    @debugfunc
    def release(self, path, flags):
        # nothing to do
        return 0

    @debugfunc
    def open(self, path, flags):
        return 0

    @debugfunc
    def truncate(self, path, size):
        return 0

    @debugfunc
    def utime(self, path, times):
        return 0

    @debugfunc
    def rmdir(self, path):
        # You can drop a directory, if you really want to I guess.
        if self.is_root(path):
            return 0            # Shyeah right.  Not gonna DROP DATABASE.
        pe=getParts(path)[1:]
        table=pe[0]
        criteria=make_criteria(pe[1:])
        # I think trying to delete a table will only delete its contents,
        # not the actual table.
        # That violates Least Astonishment; may have to change that.
        query="DELETE FROM %s "%table
        if criteria:
            query+="WHERE %s"%criteria
        self.DBG(query)
        # Not actually gonna do it yet!!
        # self.cursor.execute(query)
        return 0

    @debugfunc
    def rename(self, pathfrom, pathto):
        return 0

    @debugfunc
    def fsync(self, path, isfsyncfile):
        return 0


fuse.fuse_python_api=(0,2)

def main():
    usage="""
          surveyfs:
      """ + fuse.Fuse.fusage

    server = MySQLFUSE(version="%prog " + fuse.__version__,
                       usage=usage, dash_s_do='setsingle')

    server.parse(errex=1)
    server.main()

if __name__ == '__main__':
    main()
