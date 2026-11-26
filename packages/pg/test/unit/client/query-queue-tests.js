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
var helper = require('./test-helper')
var Connection = require('../../../lib/connection')

test('drain', function () {
  var con = new Connection({ stream: 'NO' })
  var client = new Client({ connection: con })
  con.connect = function () {
    con.emit('connect')
  }
  con.query = function () {}
  client.connect()

  var raisedDrain = false
  client.on('drain', function () {
    raisedDrain = true
  })

  client.query('hello')
  client.query('sup')
  client.query('boom')

  test('with pending queries', function () {
    test('does not emit drain', function () {
      assert.equal(raisedDrain, false)
    })
  })

  test('after some queries executed', function () {
    con.emit('readyForQuery')
    test('does not emit drain', function () {
      assert.equal(raisedDrain, false)
    })
  })

  test('when all queries are sent', function () {
    con.emit('readyForQuery')
    con.emit('readyForQuery')
    test('does not emit drain', function () {
      assert.equal(raisedDrain, false)
    })
  })

  test('after last query finishes', function () {
    con.emit('readyForQuery')
    test('emits drain', function () {
      process.nextTick(function () {
        assert.ok(raisedDrain)
      })
    })
  })
})
