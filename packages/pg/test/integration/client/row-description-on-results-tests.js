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

var Client = helper.Client

var conInfo = helper.config

var checkResult = function (result) {
  assert(result.fields)
  assert.equal(result.fields.length, 3)
  var fields = result.fields
  assert.equal(fields[0].name, 'now')
  assert.equal(fields[1].name, 'num')
  assert.equal(fields[2].name, 'texty')
  assert.equal(fields[0].dataTypeID, 1184)
  assert.equal(fields[1].dataTypeID, 23)
  assert.equal(fields[2].dataTypeID, 25)
}

test('row descriptions on result object', function () {
  var client = new Client(conInfo)
  client.connect(
    assert.success(function () {
      client.query(
        'SELECT NOW() as now, 1::int as num, $1::text as texty',
        ['hello'],
        assert.success(function (result) {
          checkResult(result)
          client.end()
        })
      )
    })
  )
})

test('row description on no rows', function () {
  var client = new Client(conInfo)
  client.connect(
    assert.success(function () {
      client.query(
        'SELECT NOW() as now, 1::int as num, $1::text as texty LIMIT 0',
        ['hello'],
        assert.success(function (result) {
          checkResult(result)
          client.end()
        })
      )
    })
  )
})
