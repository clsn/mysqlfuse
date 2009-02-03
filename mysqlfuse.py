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
from copy import copy

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

This branch of mysqlfuse is going to have the directories alternating
between keyname and keyvalue.  This should allow browsing by any ordering
of keys.

(N.B. Handling NULL keys and different types of keys (int, varchar) is
going to get tricky--actually it hasn't.) The above should lead to this:


                           testtab
                          /       \
                         /         \
                     key1           key2     (all the key fields)
               _____/ |  \         / |  \
              /       |   \       /  |   \
             foo     ...  bar   baz ...  quux  (all the values for each field)
              |            |     |         |
              |            |     |         |
             key2         key2  key1      key1  (the key fields not above each)
           /  |  \       / | \ / |  \    / |  \
          /   |   \     |  | | | |   |  |  |   \
        baz  ...  quux baz ..  foo..bar foo...  bar  (values for those)
       /   \
     data1  data2  ...


At the bottom level, the filenames are "data1" and "data2", i.e. all the
*names* of the non-key fields, and each such file contains the *value* for
that field in that row (note that each directory just above the bottom
level contains a single row, assuming the keys have to be unique,
i.e. primary).  Higher-level entries are all directories, not files.  They
alternate between name-of-a-key-field and values-of-a-key-field.  For
example, /books/book/My_Friend_Flicka/page/71/pagecontents.

This way, the data can be accessed by any possible permutation of keys,
since both /books/book/My_Friend_Flicka/page/71/ and also
/books/page/71/book/My_Friend_Flicka/ are there.  This gets more fun when
there are more elements in the key.

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
    # Elements are going to be alternating key/value pairs.
    criteria=""
    for i in range(0,len(elts),2): # Count by twos!
        (key, name)=elts[i:i+2]
        key=escape_for_sql(unescape_from_fs(key))
        name=escape_for_sql(unescape_from_fs(name))
        criteria+="`%s`='%s' AND "%(key,name)
    if len(criteria)>5:
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

    @debugfunc
    def fsinit(self):
        # I assume that here I need to log into the db.
        # Declare host/user/passwd/db in the -o options on the cmd line.
        self.connection=MySQLdb.Connection(host=self.host,
                                           user=self.user,
                                           db=self.db,
                                           passwd=self.passwd)
        self.cursor=self.connection.cursor()
        self.dcursor=self.connection.cursor(MySQLdb.cursors.DictCursor)
        # Build a data structure encoding things.  The indexes aren't
        # allowed to change during a session, only their contents.
        self.cursor.execute("SHOW TABLES")
        self.tables=map((lambda x:x[0]), self.cursor.fetchall())
        self.keys={}
        self.fields={}
        for table in self.tables:
            self.cursor.execute("SHOW INDEXES FROM `%s` WHERE Key_name='PRIMARY'"%table)
            ix=self.cursor.fetchall()
            self.keys[table]=map((lambda x:x[4]), ix)
            self.cursor.execute("SHOW COLUMNS FROM `%s`"%table)
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
        # In order to be a file, the depth of the path must equal 1(for the
        # table) plus the number of keys for that table *times two* (key
        # and value), plus 1 more to get down to the file level.
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
        if len(pathelts) < 2*len(keys)+2:
            return True
        else:
            return False

    @debugfunc
    def getattr(self, path):
        st = MyStat()
        pe = getParts(path)[1:]

        if self.is_root(pathelts=pe):
            return st
        table=escape_for_sql(unescape_from_fs(pe[0]))
        if not table in self.tables:
            return -fuse.ENOENT
        if len(pe)==1:      # It's a table
            return st
        query="SELECT COUNT(*) from `%s` "%table
        if self.is_directory(path):
            # Which KIND of directory is it??
            d=getDepth(path)
            if d%2==1:
                # This is a keyvalue dir.
                criteria=make_criteria(pe[1:])
            else:
                criteria=make_criteria(pe[1:-1])
            if criteria:
                query+="WHERE %s"%criteria
            self.DBG(query)
            self.cursor.execute(query)
            if self.cursor.fetchone()[0]<1:
                return -fuse.ENOENT
            return st
        else:
            # Otherwise, it's a "file", i.e. an actual field.
            st.st_mode = stat.S_IFREG | 0666
            st.st_nlink = 1
            self.DBG("getattr for a file: %s"%path)
            # Oh what the hell.  Yes, we query EACH TIME.
            self.DBG("about to make criteria: %s"%str(pe[1:-1]))
            criteria=make_criteria(pe[1:-1])
            query="SELECT length(`%s`) FROM `%s` WHERE "\
                "%s"%(escape_for_sql(unescape_from_fs(pe[-1])),
                      table, criteria)
            self.DBG(query)
            n=self.cursor.execute(query)
            if n<=0:
                return -fuse.ENOENT
            sz=self.cursor.fetchone()[0]
            if sz is None:
                sz=0
            else:
                sz=int(sz)
            st.st_size = sz
        return st


    @debugfunc
    def readdir(self, path, offset):
        dirents=['.', '..']
        pe=path.split('/')[1:]
        if path=='/':
            # Populate root dir
            dirents.extend(self.tables)
        else:
            # OK.  Apart from root (above) we are either at a "keyname"
            # directory or a "keyvalue" directory.  That is, this dir is
            # either named "book" or "My Friend Flicka", etc.  I also know
            # all the keys (if any) specified above this level.  If I am
            # reading a directory named with a keyname, its contents will
            # be dirs whose names are values that are available for this
            # field, searching on whatever is specified between here and
            # root (really tabledir).  If this directory is named with a
            # keyvalue, then the contents of this dir will be all the
            # keynames not already listed on the path from tabledir to
            # here.
            #
            # Unless, of course, we are at a bottom-level directory, in
            # which case the contents are files named after the non-key
            # fields in the DB.
            table=escape_for_sql(unescape_from_fs(pe[0]))
            d=getDepth(path)
            # If depth==0, we're at root, we already did that.  If
            # depth==1, we're at tabledir, reading keynames.  tabledir
            # counts as a keyvalue dir.  If depth==3, we are at a keyvalue
            # dir after the first key, and so forth.
            self.DBG("Depth: %d"%d)
            if d%2==1:
                # Odd.  Read keynames.
                if not self.is_directory(path+"/foo"):
                    # One level deeper is files!  This is a bottom-level dir.
                    # This has to be an odd-numbered one, a keyvalued dir.
                    self.DBG("files now.")
                    dirents.extend(self.fields[table])
                else:
                    all_keys=copy(self.keys[table])
                    self.DBG("all keynames: %s"%str(all_keys))
                    for i in range(1,d,2): # Every other dir: the keynames.
                        self.DBG("Removing %s"%pe[i])
                        all_keys.remove(pe[i])
                    # Whatever is left is what goes here.
                    dirents.extend(all_keys)
            else:
                # Even.  Read in keyvalues.  I need to use criteria here.
                self.DBG("Keyvalues.  pe = %s"%str(pe))
                self.DBG("About to make criteria from %s"%str(pe[1:-1]))
                criteria=make_criteria(pe[1:-1])
                self.DBG("criteria: %s"%criteria)
                query="SELECT DISTINCT `%s` FROM `%s` "%(pe[-1],table)
                if criteria:
                    query+="WHERE "+criteria
                self.DBG(query)
                self.cursor.execute(query)
                l=self.cursor.fetchall()
                self.DBG("returned: %s"%str(l))
                # Mustn't forget to convert to string
                l=map((lambda x:str(x[0])), l) 
                dirents.extend(l)
        for r in dirents:
            yield fuse.Direntry(r)

    @debugfunc
    def mknod(self, path, mode, dev):
        # I don't know that we need this.
        return 0

    @debugfunc
    def unlink(self,path):
        # You can only remove a "directory"
        # So this doesn't really do anything.
        return 0

    @debugfunc
    def write(self, path, buf, offset):
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
        query="UPDATE `%s` SET `%s`='%s' "%(table, field, data)
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
        criteria=make_criteria(pe[1:-1])
        field=pe[-1]
        query="SELECT `%s` FROM `%s` "%(field,table)
        if criteria:
            query +="WHERE %s"%criteria
        self.DBG(query)
        self.cursor.execute(query)
        x=self.cursor.fetchone()
        self.DBG(str(x))
        data=str(x[0])
        # doesn't matter if we're on the wrong level of the tree.
        return data[offset:offset+size]


    @debugfunc
    def mkdir(self, path, mode):
        pe=getParts(path)[1:]
        table=pe[0]
        keys=''
        seen=[]
        values=''
        # We can't make a keyname directory; those are fixed by the DB
        # structure.  If someone asks, fail.
        d=getDepth(path)
        if d==1:
            # Trying to create a table.  n00b.
            return -fuse.ENOTDIR
        if d%2==0:
            # Trying to create a keyname dir.
            return -fuse.ENOTDIR
        for i in range(1,d,2):
            (key, value)=pe[i:i+2]
            value=escape_for_sql(unescape_from_fs(value))
            seen.append(key)
            keys+="`"+key+"`,"
            values+="'%s',"%value
        # Make sure we have all the required keys.
        for key in self.keys[table]:
            if key in seen:
                continue
            keys+="`"+key+"`,"
            values+="'',"
        # Chop off the comma
        keys=keys[:-1]
        values=values[:-1]
        query="REPLACE INTO `%s` (%s) VALUES (%s)"%(table, keys, values)
        self.DBG(query)
        self.cursor.execute(query)
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
        query="DELETE FROM `%s` "%table
        if criteria:
            query+="WHERE %s"%criteria
        self.DBG(query)
        self.cursor.execute(query)
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
    server.parser.add_option(mountopt="host", default="localhost")
    server.parser.add_option(mountopt="user", default=os.environ['USER'])
    server.parser.add_option(mountopt="passwd", default=None)
    server.parser.add_option(mountopt="db", default="test")

    server.parse(errex=1, values=server)
    # Parse, *then* initialize.
    server.fsinit()
    server.main()

if __name__ == '__main__':
    main()
