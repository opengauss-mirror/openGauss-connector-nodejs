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
var util = require('util')
var helper = require('./test-helper')

var Client = helper.Client

var conInfo = helper.config

test('returns results as array', function () {
  var client = new Client(conInfo)
  var checkRow = function (row) {
    assert(util.isArray(row), 'row should be an array')
    assert.equal(row.length, 4)
    assert.equal(row[0].getFullYear(), new Date().getFullYear())
    assert.strictEqual(row[1], 1)
    assert.strictEqual(row[2], 'hai')
    assert.strictEqual(row[3], null)
  }
  client.connect(
    assert.success(function () {
      var config = {
        text: 'SELECT NOW(), 1::int, $1::text, null',
        values: ['hai'],
        rowMode: 'array',
      }
      var query = client.query(
        config,
        assert.success(function (result) {
          assert.equal(result.rows.length, 1)
          checkRow(result.rows[0])
          client.end()
        })
      )
    })
  )
})
