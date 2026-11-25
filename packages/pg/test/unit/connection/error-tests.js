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
var net = require('net')

const suite = new helper.Suite()

suite.test('connection emits stream errors', function (done) {
  var con = new Connection({ stream: new MemoryStream() })
  assert.emits(con, 'error', function (err) {
    assert.equal(err.message, 'OMG!')
    done()
  })
  con.connect()
  con.stream.emit('error', new Error('OMG!'))
})

suite.test('connection emits ECONNRESET errors during normal operation', function (done) {
  var con = new Connection({ stream: new MemoryStream() })
  con.connect()
  assert.emits(con, 'error', function (err) {
    assert.equal(err.code, 'ECONNRESET')
    done()
  })
  var e = new Error('Connection Reset')
  e.code = 'ECONNRESET'
  con.stream.emit('error', e)
})

suite.test('connection does not emit ECONNRESET errors during disconnect', function (done) {
  var con = new Connection({ stream: new MemoryStream() })
  con.connect()
  var e = new Error('Connection Reset')
  e.code = 'ECONNRESET'
  con.end()
  con.stream.emit('error', e)
  done()
})

var SSLNegotiationPacketTests = [
  {
    testName: 'connection does not emit ECONNRESET errors during disconnect also when using SSL',
    errorMessage: null,
    response: 'S',
    responseType: 'sslconnect',
  },
  {
    testName: 'connection emits an error when SSL is not supported',
    errorMessage: 'The server does not support SSL connections',
    response: 'N',
    responseType: 'error',
  },
  {
    testName: 'connection emits an error when postmaster responds to SSL negotiation packet',
    errorMessage: 'There was an error establishing an SSL connection',
    response: 'E',
    responseType: 'error',
  },
]

for (const tc of SSLNegotiationPacketTests) {
  suite.test(tc.testName, function (done) {
    // our fake postgres server
    var socket
    var server = net.createServer(function (c) {
      socket = c
      c.once('data', function (data) {
        c.write(Buffer.from(tc.response))
      })
    })

    server.listen(7778, function () {
      var con = new Connection({ ssl: true })
      con.connect(7778, 'localhost')
      assert.emits(con, tc.responseType, function (err) {
        if (tc.errorMessage !== null || err) {
          assert.equal(err.message, tc.errorMessage)
        }
        con.end()
        socket.destroy()
        server.close()
        done()
      })
      con.requestSsl()
    })
  })
}
