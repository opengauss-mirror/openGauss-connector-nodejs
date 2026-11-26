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
var client = helper.client()

client.query('CREATE TEMP TABLE arrtest (n integer, s varchar)')
client.query("INSERT INTO arrtest VALUES (4, 'foo'), (5, 'bar'), (6, 'baz');")

var qText =
  "SELECT \
ARRAY[1, 2, 3] AS b,\
ARRAY['xx', 'yy', 'zz'] AS c,\
ARRAY(SELECT n FROM arrtest) AS d,\
ARRAY(SELECT s FROM arrtest) AS e;"

client.query(qText, function (err, result) {
  if (err) throw err
  var row = result.rows[0]
  for (var key in row) {
    assert.equal(typeof row[key], 'object')
    assert.equal(row[key].length, 3)
  }
  client.end()
})
