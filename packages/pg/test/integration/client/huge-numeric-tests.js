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
var helper = require('./test-helper')
const pool = new helper.pg.Pool()

pool.connect(
  assert.success(function (client, done) {
    var types = require('pg-types')
    // 1231 = numericOID
    types.setTypeParser(1700, function () {
      return 'yes'
    })
    types.setTypeParser(1700, 'binary', function () {
      return 'yes'
    })
    var bignum = '294733346389144765940638005275322203805'
    client.query('CREATE TEMP TABLE bignumz(id numeric)')
    client.query('INSERT INTO bignumz(id) VALUES ($1)', [bignum])
    client.query(
      'SELECT * FROM bignumz',
      assert.success(function (result) {
        assert.equal(result.rows[0].id, 'yes')
        done()
        pool.end()
      })
    )
  })
)
