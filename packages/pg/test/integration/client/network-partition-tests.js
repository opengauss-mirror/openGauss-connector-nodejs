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
var buffers = require('../../test-buffers')
var helper = require('./test-helper')
var suite = new helper.Suite()

var net = require('net')

var Server = function (response) {
  this.server = undefined
  this.socket = undefined
  this.response = response
}

Server.prototype.start = function (cb) {
  // this is our fake postgres server
  // it responds with our specified response immediatley after receiving every buffer
  // this is sufficient into convincing the client its connectet to a valid backend
  // if we respond with a readyForQuery message
  this.server = net.createServer(
    function (socket) {
      this.socket = socket
      if (this.response) {
        this.socket.on(
          'data',
          function (data) {
            // deny request for SSL
            if (data.length == 8) {
              this.socket.write(Buffer.from('N', 'utf8'))
              // consider all authentication requests as good
            } else if (!data[0]) {
              this.socket.write(buffers.authenticationOk())
              // respond with our canned response
            } else {
              this.socket.write(this.response)
            }
          }.bind(this)
        )
      }
    }.bind(this)
  )

  var port = 54321

  var options = {
    host: 'localhost',
    port: port,
  }
  this.server.listen(options.port, options.host, function () {
    cb(options)
  })
}

Server.prototype.drop = function () {
  this.socket.destroy()
}

Server.prototype.close = function (cb) {
  this.server.close(cb)
}

var testServer = function (server, cb) {
  // wait for our server to start
  server.start(function (options) {
    // connect a client to it
    var client = new helper.Client(options)
    client.connect().catch((err) => {
      assert(err instanceof Error)
      clearTimeout(timeoutId)
      server.close(cb)
    })

    server.server.on('connection', () => {
      // after 50 milliseconds, drop the client
      setTimeout(function () {
        server.drop()
      }, 50)
    })

    // blow up if we don't receive an error
    var timeoutId = setTimeout(function () {
      throw new Error('Client should have emitted an error but it did not.')
    }, 5000)
  })
}

suite.test('readyForQuery server', (done) => {
  const respondingServer = new Server(buffers.readyForQuery())
  testServer(respondingServer, done)
})

suite.test('silent server', (done) => {
  const silentServer = new Server()
  testServer(silentServer, done)
})
