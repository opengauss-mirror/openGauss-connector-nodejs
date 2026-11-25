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
var bluebird = require('bluebird')
var helper = require('../test-helper')
var pg = helper.pg

process.on('unhandledRejection', function (e) {
  console.error(e, e.stack)
  process.exit(1)
})

const suite = new helper.Suite()

suite.test('promise API', (cb) => {
  const pool = new pg.Pool()
  pool.connect().then((client) => {
    client
      .query('SELECT $1::text as name', ['foo'])
      .then(function (result) {
        assert.equal(result.rows[0].name, 'foo')
        return client
      })
      .then(function (client) {
        client.query('ALKJSDF').catch(function (e) {
          assert(e instanceof Error)
          client.query('SELECT 1 as num').then(function (result) {
            assert.equal(result.rows[0].num, 1)
            client.release()
            pool.end(cb)
          })
        })
      })
  })
})

suite.test('promise API with configurable promise type', (cb) => {
  const client = new pg.Client({ Promise: bluebird })
  const connectPromise = client.connect()
  assert(connectPromise instanceof bluebird, 'Client connect() returns configured promise')

  connectPromise
    .then(() => {
      const queryPromise = client.query('SELECT 1')
      assert(queryPromise instanceof bluebird, 'Client query() returns configured promise')

      return queryPromise.then(() => {
        client.end(cb)
      })
    })
    .catch((error) => {
      process.nextTick(() => {
        throw error
      })
    })
})
