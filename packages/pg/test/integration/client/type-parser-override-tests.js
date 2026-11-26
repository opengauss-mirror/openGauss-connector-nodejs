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

function testTypeParser(client, expectedResult, done) {
  var boolValue = true
  client.query('CREATE TEMP TABLE parserOverrideTest(id bool)')
  client.query('INSERT INTO parserOverrideTest(id) VALUES ($1)', [boolValue])
  client.query(
    'SELECT * FROM parserOverrideTest',
    assert.success(function (result) {
      assert.equal(result.rows[0].id, expectedResult)
      done()
    })
  )
}

const pool = new helper.pg.Pool(helper.config)
pool.connect(
  assert.success(function (client1, done1) {
    pool.connect(
      assert.success(function (client2, done2) {
        var boolTypeOID = 16
        client1.setTypeParser(boolTypeOID, function () {
          return 'first client'
        })
        client2.setTypeParser(boolTypeOID, function () {
          return 'second client'
        })

        client1.setTypeParser(boolTypeOID, 'binary', function () {
          return 'first client binary'
        })
        client2.setTypeParser(boolTypeOID, 'binary', function () {
          return 'second client binary'
        })

        testTypeParser(client1, 'first client', () => {
          done1()
          testTypeParser(client2, 'second client', () => done2(), pool.end())
        })
      })
    )
  })
)
