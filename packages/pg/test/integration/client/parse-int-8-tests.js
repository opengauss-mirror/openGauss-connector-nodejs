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

var helper = require('../test-helper')
var pg = helper.pg
const suite = new helper.Suite()

const pool = new pg.Pool(helper.config)
suite.test('ability to turn on and off parser', function () {
  if (helper.args.binary) return false
  pool.connect(
    assert.success(function (client, done) {
      pg.defaults.parseInt8 = true
      client.query('CREATE TEMP TABLE asdf(id SERIAL PRIMARY KEY)')
      client.query(
        'SELECT COUNT(*) as "count", \'{1,2,3}\'::bigint[] as array FROM asdf',
        assert.success(function (res) {
          assert.strictEqual(0, res.rows[0].count)
          assert.strictEqual(1, res.rows[0].array[0])
          assert.strictEqual(2, res.rows[0].array[1])
          assert.strictEqual(3, res.rows[0].array[2])
          pg.defaults.parseInt8 = false
          client.query(
            'SELECT COUNT(*) as "count", \'{1,2,3}\'::bigint[] as array FROM asdf',
            assert.success(function (res) {
              done()
              assert.strictEqual('0', res.rows[0].count)
              assert.strictEqual('1', res.rows[0].array[0])
              assert.strictEqual('2', res.rows[0].array[1])
              assert.strictEqual('3', res.rows[0].array[2])
              pool.end()
            })
          )
        })
      )
    })
  )
})
