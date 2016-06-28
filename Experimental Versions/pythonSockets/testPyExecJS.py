# -*- coding: utf-8 -*-
"""
Created on Wed Jun 22 13:31:08 2016

@author: PradoArturo
"""

import execjs

testArr = execjs.eval("'red yellow blue'.split(' ')")
print testArr

ctx = execjs.compile("""
    function add(x, y) {
        return x + y;
    }""")
    
test = ctx.call("add", 1, 2)
print test

wsConn = execjs.compile("""
        var conn = new WebSocket('ws://www.codesword.com:5050')
        
        conn.onopen = function(e) {
            return "Connection established!";
        };        
        
        conn.onmessage = function(e) {
            return e.data;
        };
        """)
