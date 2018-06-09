# -*- coding:utf-8 -*-
class MsgCall(object):

    def __init__(self,app):
        self.app = app

    def update_bean(self, cls_name, json, timestamp):
        raise NotImplementedError

    def delete_bean(self, cls_name, ids, timestamp):
        raise NotImplementedError

    def __call__(self, message, timestamp):
        try:
            flag = message[:1]
            index = message.index(':')
            cls_name = message[1:index]
            content = message[index+1:]
            if ord(flag) == 48:
                self.delete_bean(cls_name, content, timestamp)
            else:
                self.update_bean(cls_name, content, timestamp)
        except Exception as e:
            pass