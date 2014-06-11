#!/usr/bin/env python
import os
import sys
import json
import tornado.ioloop
import tornado.web
import hashlib
import shutil

EXECUTOR_BASE_PATH = sys.argv[2]

def usage():
    print 'Usage:'
    print 'executor_manager_srv.py [port] [executor_path]'
    exit(-1)

def file_md5(fpath):
    fp = open(fpath, 'rb')
    content = fp.read()
    fp.close()
    return hashlib.md5(content).hexdigest()

def get_new_revision(path):
    files = os.listdir(path)
    max_version = -1
    for fn in files:
        name, _ = os.path.splitext(fn)
        if name.startswith('r_'):
            version = int(name.split('r_')[1])
            if version > max_version:
                max_version = version
    max_version += 1
    return max_version

class ListHandler(tornado.web.RequestHandler):
    def get(self):
        ret = []
        files = os.listdir(EXECUTOR_BASE_PATH)
        for f in files:
            fn = os.path.join(EXECUTOR_BASE_PATH, f)
            if os.path.isdir(fn):
                executors = os.listdir(fn)
                for executor in executors:
                    url = '/static/' + f + '/' + executor
                    ret.append(url)
        ret.sort()
        self.write(json.dumps(ret))

class ExecutorUploadHandler(tornado.web.RequestHandler):
    def post(self):
        executor_name = self.get_argument('executor_name', '')
        if len(executor_name) > 0:
            basepath = os.path.join(EXECUTOR_BASE_PATH, executor_name)

            if not os.path.exists(basepath):
                os.makedirs(basepath)

            for f in self.request.files['file']:
                rawname = 'latest'
                dstname = os.path.join(basepath, rawname)
                fp = open(dstname, 'wb')
                fp.write(f['body'])
                fp.close()
                version = get_new_revision(basepath)
                shutil.copyfile(dstname, os.path.join(basepath, 'r_' + str(version)))
                self.write(file_md5(dstname))
                return
        self.write("invalid post")

settings = {'debug': True,
            'static_path': os.path.abspath(EXECUTOR_BASE_PATH)}

application = tornado.web.Application([
    (r"/list", ListHandler),
    (r"/upload", ExecutorUploadHandler)
], **settings)

if __name__ == '__main__':
    if len(sys.argv) < 3:
        usage()
    application.listen(int(sys.argv[1]), "0.0.0.0")
    tornado.ioloop.IOLoop.instance().start()
