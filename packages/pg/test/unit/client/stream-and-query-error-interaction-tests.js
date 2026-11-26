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
var Client = require('../../../lib/client')

test('emits end when not in query', function () {
  var stream = new (require('events').EventEmitter)()
  stream.setNoDelay = () => {}
  stream.connect = function () {
    // NOOP
  }
  stream.write = function () {
    // NOOP
  }

  var client = new Client({ connection: new Connection({ stream: stream }) })
  client.connect(
    assert.calls(function () {
      client.query(
        'SELECT NOW()',
        assert.calls(function (err, result) {
          assert(err)
        })
      )
    })
  )
  assert.emits(client, 'error')
  assert.emits(client, 'end')
  client.connection.emit('connect')
  process.nextTick(function () {
    client.connection.emit('readyForQuery')
    assert.equal(client.queryQueue.length, 0)
    assert(client.activeQuery, 'client should have issued query')
    process.nextTick(function () {
      stream.emit('close')
    })
  })
})
