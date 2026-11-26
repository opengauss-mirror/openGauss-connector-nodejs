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
const suite = new helper.Suite()

suite.test('noData message handling', function () {
  var client = helper.client()

  var q = client.query({
    name: 'boom',
    text: 'create temp table boom(id serial, size integer)',
  })

  client.query(
    {
      name: 'insert',
      text: 'insert into boom(size) values($1)',
      values: [100],
    },
    function (err, result) {
      if (err) {
        console.log(err)
        throw err
      }
    }
  )

  client.query({
    name: 'insert',
    values: [101],
  })

  var query = client.query(
    {
      name: 'fetch',
      text: 'select size from boom where size < $1',
      values: [101],
    },
    (err, res) => {
      var row = res.rows[0]
      assert.strictEqual(row.size, 100)
    }
  )

  client.on('drain', client.end.bind(client))
})
