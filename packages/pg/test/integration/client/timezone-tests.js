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
var helper = require('./../test-helper')
var exec = require('child_process').exec

var oldTz = process.env.TZ
process.env.TZ = 'Europe/Berlin'

var date = new Date()

const pool = new helper.pg.Pool()
const suite = new helper.Suite()

pool.connect(function (err, client, done) {
  assert(!err)

  suite.test('timestamp without time zone', function (cb) {
    client.query('SELECT CAST($1 AS TIMESTAMP WITHOUT TIME ZONE) AS "val"', [date], function (err, result) {
      assert(!err)
      assert.equal(result.rows[0].val.getTime(), date.getTime())
      cb()
    })
  })

  suite.test('timestamp with time zone', function (cb) {
    client.query('SELECT CAST($1 AS TIMESTAMP WITH TIME ZONE) AS "val"', [date], function (err, result) {
      assert(!err)
      assert.equal(result.rows[0].val.getTime(), date.getTime())

      done()
      pool.end(cb)
      process.env.TZ = oldTz
    })
  })
})
