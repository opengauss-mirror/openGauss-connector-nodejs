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
var Client = require('../../lib/native')
var Query = Client.Query

test('many rows', function () {
  var client = new Client(helper.config)
  client.connect()
  var q = client.query(new Query('SELECT * FROM person'))
  var rows = []
  q.on('row', function (row) {
    rows.push(row)
  })
  assert.emits(q, 'end', function () {
    client.end()
    assert.lengthIs(rows, 26)
  })
})

test('many queries', function () {
  var client = new Client(helper.config)
  client.connect()
  var count = 0
  var expected = 100
  for (var i = 0; i < expected; i++) {
    var q = client.query(new Query('SELECT * FROM person'))
    assert.emits(q, 'end', function () {
      count++
    })
  }
  assert.emits(client, 'drain', function () {
    client.end()
    assert.equal(count, expected)
  })
})

test('many clients', function () {
  var clients = []
  for (var i = 0; i < 10; i++) {
    clients.push(new Client(helper.config))
  }
  clients.forEach(function (client) {
    client.connect()
    for (var i = 0; i < 20; i++) {
      client.query('SELECT * FROM person')
    }
    assert.emits(client, 'drain', function () {
      client.end()
    })
  })
})
