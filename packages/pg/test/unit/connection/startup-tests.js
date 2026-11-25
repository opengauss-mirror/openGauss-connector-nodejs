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
require('./test-helper')
var Connection = require('../../../lib/connection')
test('connection can take existing stream', function () {
  var stream = new MemoryStream()
  var con = new Connection({ stream: stream })
  assert.equal(con.stream, stream)
})

test('using any stream', function () {
  var makeStream = function () {
    var stream = new MemoryStream()
    stream.connect = function (port, host) {
      this.connectCalled = true
      this.port = port
      this.host = host
    }
    return stream
  }

  var stream = makeStream()

  var con = new Connection({ stream: stream })

  con.connect(1234, 'bang')

  test('makes stream connect', function () {
    assert.equal(stream.connectCalled, true)
  })

  test('uses configured port', function () {
    assert.equal(stream.port, 1234)
  })

  test('uses configured host', function () {
    assert.equal(stream.host, 'bang')
  })

  test('after stream connects client emits connected event', function () {
    var hit = false

    con.once('connect', function () {
      hit = true
    })

    assert.ok(stream.emit('connect'))
    assert.ok(hit)
  })

  test('after stream emits connected event init TCP-keepalive', function () {
    var stream = makeStream()
    var con = new Connection({ stream: stream, keepAlive: true })
    con.connect(123, 'test')

    var res = false

    stream.setKeepAlive = function (bit) {
      res = bit
    }

    assert.ok(stream.emit('connect'))
    setTimeout(function () {
      assert.equal(res, true)
    })
  })
})
