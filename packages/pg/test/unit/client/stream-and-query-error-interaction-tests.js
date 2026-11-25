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
