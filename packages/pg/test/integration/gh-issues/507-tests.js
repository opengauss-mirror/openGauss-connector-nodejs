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

new helper.Suite().test('parsing array results', function (cb) {
  const pool = new pg.Pool()
  pool.connect(
    assert.success(function (client, done) {
      client.query('CREATE TEMP TABLE test_table(bar integer, "baz\'s" integer)')
      client.query('INSERT INTO test_table(bar, "baz\'s") VALUES(1, 1), (2, 2)')
      client.query('SELECT * FROM test_table', function (err, res) {
        assert.equal(res.rows[0]["baz's"], 1)
        assert.equal(res.rows[1]["baz's"], 2)
        done()
        pool.end(cb)
      })
    })
  )
})
