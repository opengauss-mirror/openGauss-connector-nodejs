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
var async = require('async')
var helper = require('../test-helper')
const suite = new helper.Suite()

var db = helper.client()

function createTableFoo(callback) {
  db.query('create temp table foo(column1 int, column2 int)', callback)
}

function createTableBar(callback) {
  db.query('create temp table bar(column1 text, column2 text)', callback)
}

function insertDataFoo(callback) {
  db.query(
    {
      name: 'insertFoo',
      text: 'insert into foo values($1,$2)',
      values: ['one', 'two'],
    },
    callback
  )
}

function insertDataBar(callback) {
  db.query(
    {
      name: 'insertBar',
      text: 'insert into bar values($1,$2)',
      values: ['one', 'two'],
    },
    callback
  )
}

function startTransaction(callback) {
  db.query('BEGIN', callback)
}
function endTransaction(callback) {
  db.query('COMMIT', callback)
}

function doTransaction(callback) {
  // The transaction runs startTransaction, then all queries, then endTransaction,
  // no matter if there has been an error in a query in the middle.
  startTransaction(function () {
    insertDataFoo(function () {
      insertDataBar(function () {
        endTransaction(callback)
      })
    })
  })
}

var steps = [createTableFoo, createTableBar, doTransaction, insertDataBar]

suite.test('test if query fails', function (done) {
  async.series(
    steps,
    assert.success(function () {
      db.end()
      done()
    })
  )
})

suite.test('test if prepare works but bind fails', function (done) {
  var client = helper.client()
  var q = {
    text: 'SELECT $1::int as name',
    values: ['brian'],
    name: 'test',
  }
  client.query(
    q,
    assert.calls(function (err, res) {
      q.values = [1]
      client.query(
        q,
        assert.calls(function (err, res) {
          assert.ifError(err)
          client.end()
          done()
        })
      )
    })
  )
})
