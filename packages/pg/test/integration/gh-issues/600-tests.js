/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * All rights reserved.
 *
 * Licensed under the MIT License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://opensource.org/license/mit
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
