# -*- coding:utf-8 -*-
import six


class MsgCall(object):

    def update_bean(self, cls_name, json, timestamp):
        raise NotImplementedError

    def delete_beans(self, cls_name, ids, timestamp):
        raise NotImplementedError

    def __call__(self, message, timestamp):
        try:
            flag = message[:1]
            index = message.index(':')
            cls_name = message[1:index]
            content = message[index+1:]
            if ord(flag) == 48:
                self.delete_beans(cls_name, content, timestamp)
            else:
                self.update_bean(cls_name, content, timestamp)
        except Exception as e:
            pass


def to_unicode(text, encoding=None, errors='strict'):
    if isinstance(text, six.text_type):
        return text
    if not isinstance(text, (bytes, six.string_types)):
        raise TypeError('to_unicode must receive a bytes,str or unicode')
    if encoding is None:
        encoding = 'utf-8'
    return text.decode(encoding, errors)


def to_bytes(text, encoding=None, errors='strict'):
    if isinstance(text, bytes):
        return text
    if not isinstance(text, (bytes, six.string_types)):
        raise TypeError('to_bytes must receive a bytes,str or unicode')
    if encoding is None:
        encoding = 'utf-8'
    return text.encode(encoding, errors)

