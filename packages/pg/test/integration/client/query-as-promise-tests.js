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
