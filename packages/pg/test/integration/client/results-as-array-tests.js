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
var util = require('util')
var helper = require('./test-helper')

var Client = helper.Client

var conInfo = helper.config

test('returns results as array', function () {
  var client = new Client(conInfo)
  var checkRow = function (row) {
    assert(util.isArray(row), 'row should be an array')
    assert.equal(row.length, 4)
    assert.equal(row[0].getFullYear(), new Date().getFullYear())
    assert.strictEqual(row[1], 1)
    assert.strictEqual(row[2], 'hai')
    assert.strictEqual(row[3], null)
  }
  client.connect(
    assert.success(function () {
      var config = {
        text: 'SELECT NOW(), 1::int, $1::text, null',
        values: ['hai'],
        rowMode: 'array',
      }
      var query = client.query(
        config,
        assert.success(function (result) {
          assert.equal(result.rows.length, 1)
          checkRow(result.rows[0])
          client.end()
        })
      )
    })
  )
})
