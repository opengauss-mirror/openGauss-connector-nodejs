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
const net = require('net')
const buffers = require('../../test-buffers')
const helper = require('./test-helper')

const suite = new helper.Suite()

const options = {
  host: 'localhost',
  port: 54321,
  connectionTimeoutMillis: 2000,
  user: 'not',
  database: 'existing',
}

const serverWithConnectionTimeout = (timeout, callback) => {
  const sockets = new Set()

  const server = net.createServer((socket) => {
    sockets.add(socket)
    socket.once('end', () => sockets.delete(socket))

    socket.on('data', (data) => {
      // deny request for SSL
      if (data.length === 8) {
        socket.write(Buffer.from('N', 'utf8'))
        // consider all authentication requests as good
      } else if (!data[0]) {
        socket.write(buffers.authenticationOk())
        // send ReadyForQuery `timeout` ms after authentication
        setTimeout(() => socket.write(buffers.readyForQuery()), timeout).unref()
        // respond with our canned response
      } else {
        socket.write(buffers.readyForQuery())
      }
    })
  })

  let closing = false
  const closeServer = (done) => {
    if (closing) return
    closing = true

    server.close(done)
    for (const socket of sockets) {
      socket.destroy()
    }
  }

  server.listen(options.port, options.host, () => callback(closeServer))
}

suite.test('successful connection', (done) => {
  serverWithConnectionTimeout(0, (closeServer) => {
    const timeoutId = setTimeout(() => {
      throw new Error('Client should have connected successfully but it did not.')
    }, 3000)

    const client = new helper.Client(options)
    client
      .connect()
      .then(() => client.end())
      .then(() => closeServer(done))
      .catch((err) => closeServer(() => done(err)))
      .then(() => clearTimeout(timeoutId))
  })
})

suite.test('expired connection timeout', (done) => {
  serverWithConnectionTimeout(options.connectionTimeoutMillis * 2, (closeServer) => {
    const timeoutId = setTimeout(() => {
      throw new Error('Client should have emitted an error but it did not.')
    }, 3000)

    const client = new helper.Client(options)
    client
      .connect()
      .then(() => client.end())
      .then(() => closeServer(() => done(new Error('Connection timeout should have expired but it did not.'))))
      .catch((err) => {
        assert(err instanceof Error)
        assert(/timeout expired\s*/.test(err.message))
        closeServer(done)
      })
      .then(() => clearTimeout(timeoutId))
  })
})
