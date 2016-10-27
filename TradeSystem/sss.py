#!/usr/bin/python
# -*- coding:utf-8 -*-
#
# http://blog.ithomer.net

import sys
import math


# 判断是否为数字
def isNum(value):
    try:
        value + 1
    except TypeError:
        return False
    else:
        return True


# 判断是否为数字
def isNum2(value):
    try:
        x = int(value)
    except TypeError:
        return False
    except ValueError:
        return False
    except Exception, e:
        return False
    else:
        return True


def test1():
    a = "123abcDE"
    print a.isalnum()  # True, 所有字符都是数字或者字母

    a = "abcDE"
    print a.isalpha()  # True, 所有字符都是字母

    a = "123.3"
    print a.isdigit()  # False, 所有字符都是数字

    a = "abcde"
    print a.islower()  # True, 所有字符都是小写

    a = "ABCDE"
    print a.isupper()  # True, 所有字符都是大写

    a = "Abcde"
    print a.istitle()  # True, 所有单词都是首字母大写，像标题

    a = "\t"
    print a.isspace()  # True, 所有字符都是空白字符、\t、\n、\r

    arr = (1, '2.1', -3, -4.5, '123a', 'abc', 'aBC', 'Abc', 'ABC', '\t')
    for a in arr:
        print a, isNum(a)

    '''
    1 True
    2.1 True
    -3 True
    -4.5 True
    123a False
    abc False
    aBC False
    Abc False
    ABC False
        False
    '''

    for a in arr:
        print a, isNum2(a)
    '''
    1 True
    2.1 True
    -3 True
    -4.5 True
    123a False
    abc False
    aBC False
    Abc False
    ABC False
        False
    '''
test1()