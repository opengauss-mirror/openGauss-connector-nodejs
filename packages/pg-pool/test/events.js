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

const expect = require('expect.js')
const EventEmitter = require('events').EventEmitter
const describe = require('mocha').describe
const it = require('mocha').it
const Pool = require('../')

describe('events', function () {
  it('emits connect before callback', function (done) {
    const pool = new Pool()
    let emittedClient = false
    pool.on('connect', function (client) {
      emittedClient = client
    })

    pool.connect(function (err, client, release) {
      if (err) return done(err)
      release()
      pool.end()
      expect(client).to.be(emittedClient)
      done()
    })
  })

  it('emits "connect" only with a successful connection', function () {
    const pool = new Pool({
      // This client will always fail to connect
      Client: mockClient({
        connect: function (cb) {
          process.nextTick(() => {
            cb(new Error('bad news'))
          })
        },
      }),
    })
    pool.on('connect', function () {
      throw new Error('should never get here')
    })
    return pool.connect().catch((e) => expect(e.message).to.equal('bad news'))
  })

  it('emits acquire every time a client is acquired', function (done) {
    const pool = new Pool()
    let acquireCount = 0
    pool.on('acquire', function (client) {
      expect(client).to.be.ok()
      acquireCount++
    })
    for (let i = 0; i < 10; i++) {
      pool.connect(function (err, client, release) {
        if (err) return done(err)
        release()
      })
      pool.query('SELECT now()')
    }
    setTimeout(function () {
      expect(acquireCount).to.be(20)
      pool.end(done)
    }, 100)
  })

  it('emits error and client if an idle client in the pool hits an error', function (done) {
    const pool = new Pool()
    pool.connect(function (err, client) {
      expect(err).to.equal(undefined)
      client.release()
      setImmediate(function () {
        client.emit('error', new Error('problem'))
      })
      pool.once('error', function (err, errClient) {
        expect(err.message).to.equal('problem')
        expect(errClient).to.equal(client)
        done()
      })
    })
  })
})

function mockClient(methods) {
  return function () {
    const client = new EventEmitter()
    Object.assign(client, methods)
    return client
  }
}
