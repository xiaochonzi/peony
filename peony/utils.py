# -*- coding:utf-8 -*-
import six


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

