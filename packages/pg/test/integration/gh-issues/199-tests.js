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
var helper = require('../test-helper')
var client = helper.client()

client.query('CREATE TEMP TABLE arrtest (n integer, s varchar)')
client.query("INSERT INTO arrtest VALUES (4, 'foo'), (5, 'bar'), (6, 'baz');")

var qText =
  "SELECT \
ARRAY[1, 2, 3] AS b,\
ARRAY['xx', 'yy', 'zz'] AS c,\
ARRAY(SELECT n FROM arrtest) AS d,\
ARRAY(SELECT s FROM arrtest) AS e;"

client.query(qText, function (err, result) {
  if (err) throw err
  var row = result.rows[0]
  for (var key in row) {
    assert.equal(typeof row[key], 'object')
    assert.equal(row[key].length, 3)
  }
  client.end()
})
