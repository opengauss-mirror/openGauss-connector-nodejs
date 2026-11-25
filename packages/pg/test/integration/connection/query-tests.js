/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

'use strict'
var helper = require('./test-helper')
var assert = require('assert')

var rows = []
// testing the low level 1-1 mapping api of client to postgres messages
// it's cumbersome to use the api this way
test('simple query', function () {
  helper.connect(function (con) {
    con.query('select * from ids')
    assert.emits(con, 'dataRow')
    con.on('dataRow', function (msg) {
      rows.push(msg.fields)
    })
    assert.emits(con, 'readyForQuery', function () {
      con.end()
    })
  })
})

process.on('exit', function () {
  assert.equal(rows.length, 2)
  assert.equal(rows[0].length, 1)
  assert.strictEqual(String(rows[0][0]), '1')
  assert.strictEqual(String(rows[1][0]), '2')
})
