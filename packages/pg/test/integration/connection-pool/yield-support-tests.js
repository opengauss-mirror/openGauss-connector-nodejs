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
var co = require('co')

const pool = new helper.pg.Pool()
new helper.Suite().test(
  'using coroutines works with promises',
  co.wrap(function* () {
    var client = yield pool.connect()
    var res = yield client.query('SELECT $1::text as name', ['foo'])
    assert.equal(res.rows[0].name, 'foo')

    var threw = false
    try {
      yield client.query('SELECT LKDSJDSLKFJ')
    } catch (e) {
      threw = true
    }
    assert(threw)
    client.release()
    yield pool.end()
  })
)
